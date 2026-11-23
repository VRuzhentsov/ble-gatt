//! The [`Driver`] implementation of [`LinuxDriver`].
use super::*;

#[async_trait]
impl Driver for LinuxDriver {
    async fn capabilities(&self) -> CapabilityReport {
        let peripheral = self
            .adapter
            .supported_advertising_capabilities()
            .await
            .ok()
            .flatten()
            .is_some();
        log::info!("capabilities: central=true peripheral={peripheral}");
        CapabilityReport {
            central: true,
            peripheral,
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.sweep_leftover_links(service).await;
        let adapter = self.adapter.clone();
        log::info!("scan: starting discovery for service {}", service.0);
        // The guard is created before the calls below, so a failure in
        // them still stops the discovery just started.
        let scan_guard = self.discovery.begin_scan().await?;
        // The same stream `bluer::Adapter::discover_devices` built: already
        // known devices first, then adapter events until discovery stops.
        let changes = adapter
            .events()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let known = adapter
            .device_addresses()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let events = futures::stream::iter(known)
            .map(AdapterEvent::DeviceAdded)
            .chain(changes.take_while(|event| {
                std::future::ready(!matches!(
                    event,
                    AdapterEvent::PropertyChanged(bluer::AdapterProperty::Discovering(false))
                ))
            }));
        let target = service.0;
        let fast_company = self.fast_scan_company.load(Ordering::Relaxed);

        // Events are handled in a task that owns the discovery guard and ends
        // when the caller drops the returned stream (the channel closes),
        // which is what stops discovery.
        //
        // BlueZ announces a device once (`DeviceAdded`, or the known list at
        // start), often before its services, RSSI or manufacturer data are
        // filled in, and never again for an object it already has. So every
        // device not yet fully reported stays pending for the whole scan and
        // is re-read on each tick, rather than only at its announcement or
        // for a fixed window after it. A window of 5s missed a phone BlueZ
        // heard 6s into a 30s scan, three scans in a row on hardware.
        //
        // A matched device stays watched after it is reported, too: its name
        // arrives in the scan response, possibly after the first report, and
        // BlueZ's name can change from a cached one to the advertised one. A
        // device is reported again when its manufacturer data, service data
        // or name changes, so a peer's changed radio records reach the
        // caller.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<DiscoveredPeer>>(32);
        tokio::spawn(async move {
            let _scan_guard = scan_guard;
            let mut reported = ScanReports::default();
            let mut pending: HashSet<bluer::Address> = HashSet::new();
            let mut events = Box::pin(events);
            let mut tick = tokio::time::interval(SCAN_RECHECK_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let due: Vec<bluer::Address> = tokio::select! {
                    event = events.next() => match event {
                        Some(AdapterEvent::DeviceAdded(address)) => {
                            pending.insert(address);
                            vec![address]
                        }
                        Some(AdapterEvent::DeviceRemoved(address)) => {
                            pending.remove(&address);
                            continue;
                        }
                        Some(_) => continue,
                        None => break,
                    },
                    _ = tick.tick() => pending.iter().copied().collect(),
                    () = tx.closed() => break,
                };
                for address in due {
                    // Reported without manufacturer data too, so an advertiser
                    // that never sends any is still found, then again once the
                    // data arrives.
                    let (peer, complete) =
                        match ScannedPeer::read(&adapter, address, target, fast_company).await {
                            ScannedPeer::Complete(peer) => (peer, true),
                            ScannedPeer::NoManufacturerData(peer) => (peer, false),
                            _ => continue,
                        };
                    if !reported.record(address, &peer) {
                        continue;
                    }
                    log::info!(
                        "scan: discovered {address} name={:?} rssi={:?} manufacturer_data={}",
                        peer.name,
                        peer.rssi,
                        if complete { "yes" } else { "not yet" }
                    );
                    if tx.send(Ok(peer)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let address: bluer::Address = peer.0.parse().map_err(|_| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: "invalid Bluetooth address".to_string(),
        })?;
        let device = self
            .adapter
            .device(address)
            .map_err(|err| BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: err.to_string(),
            })?;
        // A previous cancelled attempt's cleanup Disconnect to this exact
        // address may still be genuinely in flight on BlueZ's side — see
        // `LinuxConnectGuard::drop`'s doc comment. Dialling in now would
        // risk that stale Disconnect landing on the replacement link this
        // call is about to establish. Checked before `dial_lock` so a
        // quarantined address fails fast rather than queuing behind it.
        if self.pending_cleanup.lock().unwrap().contains(peer) {
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a previous cancelled connect attempt is still being cleaned up"
                    .to_string(),
            });
        }
        // Refuse outright rather than queue behind `dial_lock`: a caller
        // that starts a new dial to this peer before an earlier one has
        // resolved produces a genuinely concurrent second `Connect()` to
        // the same `Device` — BlueZ rejects the loser immediately, so
        // letting it through just burns a real D-Bus round trip to fail the
        // same way this check already knows it will. Checked before
        // `dial_lock` for the same reason the quarantine check above is.
        if !self.in_flight.lock().unwrap().insert(peer.clone()) {
            log::warn!(
                "connect: {} refused — a dial to this peer is already in flight",
                peer.0
            );
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a connect attempt to this peer is already in flight".to_string(),
            });
        }
        // Constructed immediately — no `.await` between the insert above
        // and here — so a cancellation from this point on, including one
        // that arrives while merely queued for `dial_lock` below, always
        // has something alive to release the entry it just claimed.
        let _in_flight_guard = InFlightGuard {
            in_flight: self.in_flight.clone(),
            peer: peer.clone(),
        };
        // Before `dial_lock`: this can wait up to `DISCOVERY_STOP_TIMEOUT`,
        // which must not hold up GATT operations on other links.
        self.settle_discovery().await;
        // Recorded *before* dialling: BlueZ can publish the Connected
        // property before `connect()` returns, and the inbound watcher would
        // otherwise race us and announce our own outbound link as a central
        // arriving at our server.
        //
        // Held across the platform call, so a concurrent `disconnect` on an
        // older handle cannot land midway and drop this link.
        let _dial = self.dial_lock.lock(peer).await;
        // Re-checked now that `dial_lock` is actually held: the check above
        // only sees state as of *before* this call queued for the lock.
        // Real-hardware evidence (Codex review): a cancelled attempt's
        // cleanup can still be inside its own bounded `Disconnect` — itself
        // holding `dial_lock` — when this call's pre-check ran and found
        // `pending_cleanup` empty. If that cleanup times out and publishes
        // the quarantine *while this call is already queued* for the lock
        // it just released, the earlier check never sees it — this call
        // would dial straight into the address the fresh, now-unbounded
        // cleanup disconnect is still targeting. `in_flight` needs no
        // matching re-check: unlike `pending_cleanup`, it can only become
        // *un*-set while queued (this call's own entry persists for as
        // long as it holds it), never newly set against this same peer out
        // from under it.
        if self.pending_cleanup.lock().unwrap().contains(peer) {
            // `_in_flight_guard` releases `in_flight` on this return; no
            // manual cleanup needed here.
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a previous cancelled connect attempt is still being cleaned up"
                    .to_string(),
            });
        }
        // The generation distinguishes successive dials to the same address,
        // so a watcher from a previous connection cannot report a disconnect
        // for the one that replaced it.
        let dial_generation = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.dialed
            .lock()
            .unwrap()
            .insert(peer.clone(), dial_generation);
        // Reserved *before* `device.connect()` below is ever issued, not
        // lazily once cleanup begins — see `MAX_CONCURRENT_CLEANUP_DISCONNECTS`'s
        // doc comment (Codex P1, "reserve cleanup capacity before issuing
        // Connect"): if this attempt is later abandoned or times out, its
        // cleanup will need this exact capacity, and reserving it only
        // after the fact bounds nothing — a connect this driver cannot
        // guarantee it can ever clean up must not be issued at all.
        let Ok(cleanup_permit) = self.cleanup_permits.clone().try_acquire_owned() else {
            let mut dialed = self.dialed.lock().unwrap();
            if dialed.get(peer) == Some(&dial_generation) {
                dialed.remove(peer);
            }
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: format!(
                    "no cleanup capacity available: {MAX_CONCURRENT_CLEANUP_DISCONNECTS} peers already \
                     awaiting cleanup"
                ),
            });
        };
        log::info!("connect: dialling {} generation={dial_generation}", peer.0);
        // Cancellation guard, mirroring the Android driver's `ConnectGuard`
        // -- `connect()` is an async fn, so a caller can drop this future
        // mid-await (a timeout, a `select!` losing a race) while suspended
        // inside `device.connect().await` below. Without this, neither of
        // that call's own two return paths ever runs: the pending `dialed`
        // entry stays behind (every retry then refused as "already
        // dialling"), and more damagingly, BlueZ's own connect request keeps
        // running independently of the cancelled Rust future and can
        // complete anyway -- leaving a real ACL/GATT link with nothing on
        // the Rust side ever owning it, so even an explicit retry can fail
        // with "already connected" until that orphaned link drops
        // externally. Disarmed right after `device.connect()` actually
        // returns, on both its Ok and Err paths (each already does its own
        // correct cleanup/handoff).
        let mut guard = LinuxConnectGuard {
            adapter: self.adapter.clone(),
            address,
            dialed: self.dialed.clone(),
            dial_lock: self.dial_lock.clone(),
            pending_cleanup: self.pending_cleanup.clone(),
            cleanup_permit: Some(cleanup_permit),
            peer: peer.clone(),
            generation: dial_generation,
            armed: true,
        };
        // Bounded, not just cancellation-safe: without this, an unresponsive
        // peer's Connect reply never arriving holds `dial_lock` (and, real-
        // hardware evidence suggests, whatever capacity the shared D-Bus
        // connection has for outstanding calls) forever, so every later dial
        // queues up behind one that will never finish. See
        // `CONNECT_TIMEOUT`'s doc comment.
        let connect_result = match tokio::time::timeout(CONNECT_TIMEOUT, device.connect()).await {
            Ok(result) => {
                // The D-Bus call genuinely completed (Ok or Err) — nothing
                // left for the guard to cancel.
                guard.armed = false;
                result
            }
            Err(_elapsed) => {
                // Guard stays armed: unlike a completed call, BlueZ's own
                // Connect may still resolve later on its own schedule — the
                // exact orphaned-link scenario this guard exists to clean up
                // after, just triggered by our own timeout instead of a
                // caller dropping the future.
                log::warn!(
                    "connect: {} timed out after {CONNECT_TIMEOUT:?} waiting for BlueZ's Connect reply",
                    peer.0
                );
                let mut dialed = self.dialed.lock().unwrap();
                if dialed.get(peer) == Some(&dial_generation) {
                    dialed.remove(peer);
                }
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: format!("timed out after {CONNECT_TIMEOUT:?} waiting for BlueZ to respond to Connect"),
                });
            }
        };
        if let Err(err) = connect_result {
            log::warn!("connect: {} refused the link: {err}", peer.0);
            let mut dialed = self.dialed.lock().unwrap();
            if dialed.get(peer) == Some(&dial_generation) {
                dialed.remove(peer);
            }
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: err.to_string(),
            });
        }
        log::info!(
            "connect: link established to {} session={dial_generation}",
            peer.0
        );
        let _ = self.events_tx.send(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Central,
            session: Some(dial_generation),
        });

        // Watch BlueZ's own Connected property so an *unsolicited* drop (peer
        // out of range, powered off) reaches `events()`. Without this a
        // caller mid-transfer has no way to distinguish "slow" from "gone" —
        // see `Driver::events`. The task ends when the device disconnects or
        // the property stream closes, so it does not leak per connection.
        let watch_device = device.clone();
        let watch_peer = peer.clone();
        let events_tx = self.events_tx.clone();
        let dialed = self.dialed.clone();
        let generation = dial_generation;
        tokio::spawn(async move {
            let Ok(mut changes) = watch_device.events().await else {
                return;
            };
            // Poll once after subscribing. A peer that dropped between
            // `connect()` returning and this subscription taking effect has
            // already emitted its `Connected(false)`, and waiting for a
            // change that has been and gone means no central-role
            // `Disconnected` is ever emitted — leaving `dialed` stale and a
            // datagram receiver blocked forever.
            if !watch_device.is_connected().await.unwrap_or(false) {
                LinuxGattConnection::report_loss(&events_tx, &dialed, &watch_peer, generation);
                return;
            }
            while let Some(event) = changes.next().await {
                let bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false)) =
                    event
                else {
                    continue;
                };
                LinuxGattConnection::report_loss(&events_tx, &dialed, &watch_peer, generation);
                return;
            }
        });

        Ok(Box::new(LinuxGattConnection {
            session: dial_generation,
            dialed: self.dialed.clone(),
            dial_lock: self.dial_lock.clone(),
            served_peers: self.served_peers.clone(),
            notify_writers: self.notify_writers.clone(),
            events_tx: self.events_tx.clone(),
            peer: peer.clone(),
            device,
            att_mtu: AtomicU16::new(crate::hal::DEFAULT_ATT_MTU),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        log::info!(
            "advertise: registering service {} with {} characteristic(s)",
            service.uuid.0,
            service.characteristics.len()
        );
        service.validate()?;
        self.note_fast_company(&service);
        // Before anything is registered, so no central can have connected to
        // this process yet.
        self.sweep_leftover_links(service.uuid).await;
        // Held for the whole setup, including both BlueZ registrations, so a
        // concurrent stop cannot interleave with installing the handles.
        let _serialise = self.advertise_lock.lock().await;
        // Set again only once both registrations succeed.
        *self.served_service.lock().unwrap() = None;
        // Abort the previous generation's watchers *before* spawning any of
        // this one's, so no task registered below can be mistaken for a
        // leftover and cancelled at the end of this call.
        for previous in self.server_watch.lock().unwrap().drain(..) {
            previous.abort();
        }
        let values = self.values.clone();
        let next_session = self.next_session.clone();
        // Reserved (not yet published) before anything is registered, so
        // every handler built below is stamped with the generation it
        // belongs to. Deliberately *not* written into `advertise_generation`
        // here: doing so immediately would mark the *outgoing* generation's
        // still-live handlers stale before its BlueZ objects are actually
        // replaced below, silently dropping any write or subscribe that
        // lands on them in that window — hardware-confirmed to happen on a
        // re-advertise. Published the moment BlueZ serves the new
        // application (below), together with replacing the old one.
        let advertise_generation = self.advertise_generation.clone();
        let this_generation = self.next_generation();
        let server_watch = self.server_watch.clone();
        let notify_writers = self.notify_writers.clone();
        let mut notify_sessions = Vec::new();
        let served_peers = self.served_peers.clone();
        let adapter = self.adapter.clone();
        let events_tx = self.events_tx.clone();

        let mut local_characteristics = Vec::with_capacity(service.characteristics.len());
        for spec in &service.characteristics {
            let uuid = spec.uuid;
            values
                .lock()
                .unwrap()
                .insert(uuid, spec.initial_value.clone());

            let read = spec.readable.then(|| {
                let values = values.clone();
                CharacteristicRead {
                    read: true,
                    fun: Box::new(move |_req| {
                        let values = values.clone();
                        Box::pin(async move {
                            let value = values
                                .lock()
                                .unwrap()
                                .get(&uuid)
                                .cloned()
                                .unwrap_or_default();
                            ReqResult::Ok(value)
                        })
                    }),
                    ..Default::default()
                }
            });

            let write = spec.writable.then(|| {
                let values = values.clone();
                let events_tx = events_tx.clone();
                let served_peers = served_peers.clone();
                let adapter = adapter.clone();
                let notify_writers = notify_writers.clone();
                let next_session = next_session.clone();
                let server_watch = server_watch.clone();
                let advertise_generation = advertise_generation.clone();
                CharacteristicWrite {
                    write: true,
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, req| {
                        let values = values.clone();
                        let events_tx = events_tx.clone();
                        let served_peers = served_peers.clone();
                        let adapter = adapter.clone();
                        let notify_writers = notify_writers.clone();
                        let next_session = next_session.clone();
                        let server_watch = server_watch.clone();
                        let advertise_generation = advertise_generation.clone();
                        let address = req.device_address;
                        let peer = PeerAddress(address.to_string());
                        Box::pin(async move {
                            // A handler descheduled around the awaits below
                            // can resume after advertising has been stopped
                            // and restarted. Publishing then would inject
                            // this payload into the *replacement* `serve`
                            // session, where a datagram fragment is accepted
                            // as current data. Dropping the application
                            // handle does not prevent it — an already-running
                            // handler is not cancelled — so it checks here.
                            // Every effect below is generation-scoped, not
                            // just the final publish. A handler resuming
                            // after a stop/restart would otherwise overwrite
                            // the replacement server's characteristic value,
                            // claim its single-central slot with a phantom
                            // peer, announce that peer and spawn a watcher
                            // for it — after which the new `serve` refuses
                            // the real central. Guarding only the last send
                            // left all of that reachable.
                            //
                            // `server_watch` is a sync mutex precisely so
                            // there is no await anywhere in here: from the
                            // check onward this is indivisible.
                            // Not yet published (see `watch_notify_sessions`):
                            // wait briefly rather than drop a write that is
                            // current. The generation only moves forward, so
                            // after this the check below sees current or stale.
                            for _ in 0..GENERATION_PUBLISH_MAX_POLLS {
                                if *advertise_generation.lock().unwrap() >= this_generation {
                                    break;
                                }
                                tokio::time::sleep(GENERATION_PUBLISH_POLL).await;
                            }
                            // The peripheral admission lock, as
                            // `watch_notify_sessions` takes it: a write can
                            // admit a peer to `served_peers` too, and a
                            // central-role `disconnect()` holds this lock
                            // across its served-session check and the
                            // platform call. Without it a write could admit
                            // the peer in between and lose the session to
                            // that disconnect. Taken before the generation
                            // check, so everything below stays await-free.
                            let _admission = notify_writers.lock().await;
                            let current = advertise_generation.lock().unwrap();
                            if *current != this_generation {
                                log::warn!(
                                    "write: dropping a write from {} for stale generation \
                                     {this_generation} (current {current})",
                                    peer.0
                                );
                                return ReqResult::Ok(());
                            }

                            values.lock().unwrap().insert(uuid, value.clone());

                            // BlueZ gives a GATT *server* no connection
                            // callback at all — the write itself is the only
                            // signal that a central is present. So the first
                            // write from a peer arms the same Connected-
                            // property watcher the central role uses.
                            //
                            // The session is allocated before `Connected` is
                            // published: publishing first meant the very
                            // first write for a peer carried `session: None`,
                            // so `serve` recorded the channel with no
                            // identity and could not reject a queued
                            // `Disconnected` from the previous connection.
                            let (session, newly_served) = {
                                let mut served = served_peers.lock().unwrap();
                                match served.get(&peer) {
                                    Some(existing) => (*existing, false),
                                    None => {
                                        let session = next_session.fetch_add(1, Ordering::Relaxed);
                                        served.insert(peer.clone(), session);
                                        (session, true)
                                    }
                                }
                            };

                            let _ = events_tx.send(GattEvent::Connected {
                                peer: peer.clone(),
                                local_role: Role::Peripheral,
                                session: Some(session),
                            });

                            if newly_served {
                                let handle = LinuxDriver::watch_peripheral_disconnect(
                                    adapter,
                                    address,
                                    peer.clone(),
                                    events_tx.clone(),
                                    served_peers,
                                    notify_writers.clone(),
                                    session,
                                );
                                server_watch.lock().unwrap().push(handle.abort_handle());
                            }

                            let _ = events_tx.send(GattEvent::CharacteristicWritten {
                                peer,
                                characteristic: uuid,
                                value,
                                session: Some(session),
                            });
                            drop(current);

                            ReqResult::Ok(())
                        })
                    })),
                    ..Default::default()
                }
            });

            // `Io` rather than `Fun`: BlueZ passes the subscribing device's
            // address to `AcquireNotify` but not to `StartNotify`, so this is
            // the only notify mode on Linux that says *who* subscribed.
            let (control, control_handle) = spec
                .notifiable
                .then(bluer::gatt::local::characteristic_control)
                .map_or_else(|| (None, Default::default()), |(c, h)| (Some(c), h));

            let notify = spec.notifiable.then(|| CharacteristicNotify {
                notify: true,
                method: CharacteristicNotifyMethod::Io,
                ..Default::default()
            });

            if let Some(control) = control {
                notify_sessions.push(tokio::spawn(LinuxDriver::watch_notifications(
                    control,
                    uuid,
                    notify_writers.clone(),
                    events_tx.clone(),
                    served_peers.clone(),
                    adapter.clone(),
                    next_session.clone(),
                    server_watch.clone(),
                    advertise_generation.clone(),
                    this_generation,
                )));
            }

            local_characteristics.push(LocalCharacteristic {
                uuid: uuid.0,
                read,
                write,
                notify,
                control_handle,
                ..Default::default()
            });
        }

        let app = Application {
            services: vec![LocalService {
                uuid: service.uuid.0,
                primary: true,
                characteristics: local_characteristics,
                ..Default::default()
            }],
            ..Default::default()
        };

        let app_handle = self
            .adapter
            .serve_gatt_application(app)
            .await
            .map_err(|err| {
                log::warn!("advertise: BlueZ rejected the GATT application: {err}");
                BleError::Gatt(err.to_string())
            })?;
        // Switch over the moment BlueZ serves the new application: from now
        // on it dispatches writes and subscriptions to the new handlers, so
        // they must be current even while the advertisement below is still
        // being registered. Replacing the handle also unregisters the old
        // application, so no request reaches handlers that now refuse it.
        //
        // The old generation's centrals are released in the same step: their
        // disconnect watchers were aborted above, so a `served_peers` entry
        // left behind would make the next subscription from that address be
        // skipped for good. The admission lock (`notify_writers`) and the
        // generation lock are held across publish and drain, so a new
        // session is admitted either before (and released with the old) or
        // after (and kept), never lost in between.
        {
            let mut writers = self.notify_writers.lock().await;
            *self.app_handle.lock().await = Some(app_handle);
            let released: Vec<PeerAddress> = {
                let mut current = advertise_generation.lock().unwrap();
                *current = this_generation;
                self.served_peers
                    .lock()
                    .unwrap()
                    .drain()
                    .map(|(peer, _)| peer)
                    .collect()
            };
            self.announce_released(released, "re-advertise");
            writers.clear();
        }

        let adv_handle = self.register_advertisement(&service).await.map_err(|err| {
            log::warn!("advertise: BlueZ rejected the advertisement: {err}");
            BleError::Gatt(err.to_string())
        })?;

        *self.adv_handle.lock().await = Some(adv_handle);
        *self.served_service.lock().unwrap() = Some(service);
        log::info!("advertise: registered, generation={this_generation}");

        // The previous generation's watchers were already aborted before any
        // of this generation's tasks were spawned — see the drain near the
        // top of this function. Draining *here* would have been wrong: a
        // central can acquire a notify session between the application being
        // served and this point, and `watch_notify_sessions` pushes that
        // peer's disconnect watcher into the same vector — so a drain here
        // would abort a watcher belonging to the new advertisement, leaving
        // that peer stuck in `served_peers` with nothing to report its loss.
        self.server_watch.lock().unwrap().extend(
            notify_sessions
                .iter()
                .map(tokio::task::JoinHandle::abort_handle),
        );
        // The JoinHandles themselves are dropped here, which detaches rather
        // than cancels — the AbortHandles above are what stop them.
        drop(notify_sessions);
        Ok(())
    }

    fn served_service(&self) -> Option<GattServiceSpec> {
        self.served_service.lock().unwrap().clone()
    }

    /// Replace the stored value that the characteristic's read handler returns.
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let served = self.served_service.lock().unwrap().clone();
        let served = served
            .ok_or_else(|| BleError::Gatt("set_characteristic_value: nothing is served".into()))?;
        if !served
            .characteristics
            .iter()
            .any(|served_characteristic| served_characteristic.uuid == characteristic)
        {
            return Err(BleError::Gatt(format!(
                "characteristic {} is not served",
                characteristic.0
            )));
        }
        self.values.lock().unwrap().insert(characteristic, value);
        Ok(())
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        self.note_fast_company(&service);
        let _serialise = self.advertise_lock.lock().await;
        let serving = self
            .served_service
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|served| served.same_service(&service));
        if !serving {
            return Err(BleError::Gatt(format!(
                "update_advertisement: not serving service {} with these characteristics",
                service.uuid.0
            )));
        }
        // Only the LEAdvertisement1 object is replaced. The GATT application,
        // its generation and every served central stay as they are.
        // Registered before the old one is dropped, as `advertise` does, so
        // advertising never pauses. Except on a controller with no free
        // instance (some have one): BlueZ would refuse the second, so the
        // old one is withdrawn first.
        let mut adv_handle = self.adv_handle.lock().await;
        if adv_handle.is_none() {
            self.wait_advertisement_release().await?;
            *self.served_service.lock().unwrap() = Some(service);
            return Ok(());
        }
        if self
            .adapter
            .supported_advertising_instances()
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?
            == 0
        {
            self.release_advertisement(&mut adv_handle).await?;
        }
        let registered = self.register_advertisement(&service).await;
        match registered {
            Ok(handle) => {
                *adv_handle = Some(handle);
                *self.served_service.lock().unwrap() = Some(service);
                log::info!("advertise: advertisement updated, GATT application kept");
                Ok(())
            }
            Err(err) => {
                // The old advertisement is withdrawn too, so the caller's
                // refusal means "not advertising", as the trait says.
                *adv_handle = None;
                log::warn!("advertise: BlueZ rejected the updated advertisement: {err}");
                Err(BleError::Gatt(err.to_string()))
            }
        }
    }

    async fn set_advertising_enabled(&self, enabled: bool) -> Result<()> {
        let _serialise = self.advertise_lock.lock().await;
        let service =
            self.served_service.lock().unwrap().clone().ok_or_else(|| {
                BleError::Gatt("advertising visibility: nothing is served".into())
            })?;
        let mut handle = self.adv_handle.lock().await;
        if enabled {
            if handle.is_none() {
                self.wait_advertisement_release().await?;
                *handle = Some(
                    self.register_advertisement(&service)
                        .await
                        .map_err(|err| BleError::Gatt(err.to_string()))?,
                );
            }
        } else {
            self.release_advertisement(&mut handle).await?;
        }
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        log::info!("advertise: stopping");
        let _serialise = self.advertise_lock.lock().await;
        *self.served_service.lock().unwrap() = None;
        // Invalidate first, so a handler already running publishes nothing.
        // Taking the same lock the handlers publish under means an
        // in-progress publication completes before this, or is rejected —
        // never interleaved.
        *self.advertise_generation.lock().unwrap() = self.next_generation();
        for watch in self.server_watch.lock().unwrap().drain(..) {
            watch.abort();
        }
        self.release_served_peers("stop").await;
        *self.app_handle.lock().await = None;
        *self.adv_handle.lock().await = None;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        log::info!("close: releasing this process's links");
        if let Err(err) = self.stop_advertising().await {
            log::warn!("close: stopping the server failed: {err}");
        }
        let dialed: Vec<PeerAddress> = self.dialed.lock().unwrap().keys().cloned().collect();
        for peer in dialed {
            let Ok(address) = peer.0.parse::<bluer::Address>() else {
                continue;
            };
            let Ok(device) = self.adapter.device(address) else {
                continue;
            };
            let _dial = self.dial_lock.lock(&peer).await;
            match tokio::time::timeout(CLEANUP_DISCONNECT_TIMEOUT, device.disconnect()).await {
                Ok(Ok(())) => log::info!("close: {} disconnected", peer.0),
                Ok(Err(err)) if BleError::bluez_not_connected(&err) => {}
                Ok(Err(err)) => log::warn!("close: disconnecting {} failed: {err}", peer.0),
                Err(_) => log::warn!(
                    "close: disconnecting {} timed out after {CLEANUP_DISCONNECT_TIMEOUT:?}",
                    peer.0
                ),
            }
        }
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.notify_matching(characteristic, value, None, |_| true, "no subscriber")
            .await
    }

    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let wanted = peer.0.clone();
        self.notify_matching(
            characteristic,
            value,
            session.map(|session| (peer, session)),
            move |writer: &CharacteristicWriter| writer.device_address().to_string() == wanted,
            &format!("{} has no live notify session", peer.0),
        )
        .await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        // Serialised against dial (`dial_lock`) and, separately, against
        // peripheral-role session admission (`notify_writers`) — the lock
        // `watch_notify_sessions` takes before installing a replacement
        // `served_peers` entry. `dial_lock` alone does not serialise against
        // that: `watch_notify_sessions` never takes it, so a new session
        // could be installed between this call's check and its
        // `device.disconnect()`, and this call would then tear down the
        // replacement it just installed instead of the stale session it was
        // asked to end. Both are held across the check *and* the disconnect,
        // for the reason `dial_lock` already was: `Device` resolves by
        // address, so validating and then awaiting would let either kind of
        // reconnect land in the gap and have this call drop the replacement.
        let _dial = self.dial_lock.lock(peer).await;
        let _writers = self.notify_writers.lock().await;
        if let Some(session) = session {
            if self.served_peers.lock().unwrap().get(peer) != Some(&session) {
                return Ok(());
            }
        }
        let Ok(address) = peer.0.parse() else {
            return Err(BleError::Gatt(format!("malformed peer address {}", peer.0)));
        };
        // Already gone is success: this is called to guarantee absence, not
        // to assert presence.
        let Ok(device) = self.adapter.device(address) else {
            return Ok(());
        };
        device
            .disconnect()
            .await
            .map_err(|err| BleError::Gatt(format!("disconnecting {} failed: {err}", peer.0)))
    }

    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.events_tx.lifecycle_events()
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.events_tx.subscribe();
        Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx).map(|item| match item {
                Ok(event) => event,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    GattEvent::Lagged { dropped: n }
                }
            }),
        )
    }

    async fn radio_status(&self) -> crate::entities::models::RadioStatus {
        if self.adapter.is_powered().await.unwrap_or(false) {
            crate::entities::models::RadioStatus::On
        } else {
            crate::entities::models::RadioStatus::Off
        }
    }

    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        // Same lock order as `notify_matching`: writers first, then the
        // session check, so the writer found belongs to the session asked for.
        let writers = self.notify_writers.lock().await;
        if let Some(session) = session {
            if self.served_peers.lock().unwrap().get(peer) != Some(&session) {
                return None;
            }
        }
        // `CharacteristicWriter::mtu()` is already the usable value size
        // (bluer subtracts its BlueZ truncation workaround), not the raw MTU.
        writers
            .values()
            .flatten()
            .find(|w| !w.is_closed().unwrap_or(true) && w.device_address().to_string() == peer.0)
            .map(|w| w.mtu().min(crate::hal::MAX_ATT_ATTRIBUTE_LEN))
    }
}
