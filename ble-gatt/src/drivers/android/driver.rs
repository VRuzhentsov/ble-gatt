//! The [`Driver`] implementation of [`AndroidDriver`].
use super::*;

#[async_trait]
impl Driver for AndroidDriver {
    /// Every failure path here logs before falling back. `false`/`false` is
    /// indistinguishable from "this device genuinely has no BLE support", so
    /// a silent fallback turns a real bug into a plausible-looking hardware
    /// limitation — which is exactly what happened once already in this
    /// codebase and cost a whole debugging session. See docs/adr/0002.
    async fn capabilities(&self) -> CapabilityReport {
        let mut env = match self.inner.env() {
            Ok(env) => env,
            Err(err) => {
                log::error!("capabilities: JNI attach failed: {err}");
                return CapabilityReport::default();
            }
        };
        let central = match self.inner.call_bool(&mut env, "hasCentralSupport", "()Z") {
            Ok(value) => value,
            Err(err) => {
                let detail = BleError::describe_pending_exception(&mut env);
                log::error!(
                    "capabilities: hasCentralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
        let peripheral = match self
            .inner
            .call_bool(&mut env, "hasPeripheralSupport", "()Z")
        {
            Ok(value) => value,
            Err(err) => {
                let detail = BleError::describe_pending_exception(&mut env);
                log::error!(
                    "capabilities: hasPeripheralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
        log::info!("capabilities: central={central} peripheral={peripheral}");
        CapabilityReport {
            central,
            peripheral,
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let (tx, rx) = mpsc::channel(DISCOVERY_QUEUE_DEPTH);
        // Install the new sender only *after* the platform confirms the scan
        // started, and hold that lock across the JNI call.
        //
        // Holding it is what makes this atomic. `onScanFailed` for the
        // *existing* scan can fire while we are inside `startScan`; if we
        // had swapped our sender in first it would close ours instead of the
        // active scan's, and the active scan's stream would then hang
        // forever with its error never surfaced. Blocking that callback for
        // the duration of one JNI call is cheap, and Android posts
        // `ScanCallback` through a handler rather than calling it inline on
        // this thread, so it cannot deadlock us.
        let mut scan = self.inner.scan.lock().unwrap();
        // Chosen before `startScan` so the platform callbacks can echo it
        // back: a callback that fires during the call must be attributable
        // to this scan, not to whatever was installed before.
        let generation = scan.generation + 1;
        {
            let mut env = self.inner.env()?;
            let uuid = env
                .new_string(service.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bridge = self.inner.bridge()?;
            let started = env
                .call_method(
                    bridge.as_obj(),
                    "startScan",
                    "(Ljava/lang/String;J)I",
                    &[JValue::Object(&uuid), JValue::Long(generation as i64)],
                )
                .and_then(jni::objects::JValueGen::i);
            match started {
                // Same exception-clearing contract as `call_void`/`call_bool`:
                // a `startScan` that throws (a missing runtime Bluetooth
                // permission is the common case) would otherwise leave the
                // exception pending on the daemon-attached Tokio worker and
                // poison every later JNI call scheduled there.
                Err(err) => {
                    log::error!("scan: startScan threw for service {}", service.0);
                    return Err(BleError::jni_error(&mut env, "startScan", err));
                }
                Ok(status) => {
                    // On refusal the active scan keeps ownership untouched.
                    BleError::android_scan_start_result(status)?;
                    log::info!(
                        "scan: started for service {} generation={generation}",
                        service.0
                    );
                }
            }
        }
        // Ours now. Installed before the lock is released, so an
        // `onScanFailed` that fired during `startScan` — and is blocked on
        // this lock — finds our sender and records against our scan.
        scan.tx = Some(tx);
        scan.error = None;
        scan.generation = generation;
        scan.service = Some(service);
        scan.reported.clear();
        drop(scan);

        Ok(Box::pin(ScanStream {
            inner: self.inner.clone(),
            rx: ReceiverStream::new(rx),
            generation,
        }))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let since = self
            .inner
            .client_disconnected_at
            .lock()
            .unwrap()
            .get(&peer.0)
            .map(std::time::Instant::elapsed);
        if let Some(since) = since {
            if let Some(wait) = REDIAL_GAP.checked_sub(since) {
                log::info!(
                    "connect: {} disconnected {since:?} ago, waiting {wait:?} before dialling",
                    peer.0
                );
                tokio::time::sleep(wait).await;
            }
        }
        let (connected_tx, connected_rx) = oneshot::channel();
        let session;
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(peer.0.clone()).or_default();
            // Reject rather than overwrite, for *both* an in-flight connect
            // and one that already succeeded. The Kotlin bridge keeps one
            // `BluetoothGatt` per address: a second connect would replace it
            // without closing the first, leave two Rust handles sharing the
            // replacement's callback state, and let a stale callback from the
            // old GATT delete the new map entry — breaking both handles.
            if state.connected_tx.is_some() {
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: "a connection to this peer is already in progress".to_string(),
                });
            }
            if state.live {
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: "a connection to this peer is already open".to_string(),
                });
            }
            state.session = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
            state.connected_tx = Some(connected_tx);
            session = state.session;
        }

        // Every failure below must clear `connected_tx`. Leaving it set once
        // its receiver is gone makes the guard above reject every retry for
        // the lifetime of the driver — so a transient cause (a missing
        // runtime permission, a powered-down adapter) would become permanent
        // even after it was fixed.
        let dial = (|| -> Result<()> {
            let mut env = self.inner.env()?;
            let session = session as i64;
            let address = env
                .new_string(&peer.0)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "connect",
                "(Ljava/lang/String;J)V",
                &[JValue::Object(&address), JValue::Long(session)],
            )
        })();
        if let Err(err) = dial {
            log::warn!("connect: JNI dial to {} failed: {err}", peer.0);
            self.inner.clear_pending_connect(&peer.0);
            return Err(err);
        }
        log::info!("connect: dialling {} session={session}", peer.0);

        // Cancellation guard. `connect()` is an async fn, so its caller can
        // drop the future mid-await — a timeout, a `select!` losing a race —
        // and nothing below would then run. Without this, the pending slot
        // stays populated and every retry is refused as "already in
        // progress"; and if the callback later lands anyway, it marks a GATT
        // live that no Rust handle owns, after which retries are refused as
        // "already open" instead. Disarmed on success.
        let guard = ConnectGuard {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            armed: true,
        };

        if connected_rx.await.is_err() {
            // The guard handles cleanup on the way out.
            log::warn!(
                "connect: {} disconnected before the connection completed",
                peer.0
            );
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "disconnected before connection completed".to_string(),
            });
        }
        log::info!("connect: {} connected session={session}", peer.0);
        let mut guard = guard;
        guard.armed = false;

        let session = self
            .inner
            .connections
            .lock()
            .unwrap()
            .get(&peer.0)
            .map(|state| state.session);
        Ok(Box::new(AndroidGattConnection {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            session,
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        // Held across claiming the generation and issuing the platform call,
        // so an abandoned attempt's cleanup cannot interleave with a retry.
        // Released before awaiting the result, so a cancelled attempt's
        // cleanup is able to acquire it.
        // Held across claiming the generation *and* issuing the platform
        // call. Releasing after the increment left two windows: a
        // `stop_advertising` could complete while no attempt was installed
        // and this future would then start a server after the stop returned,
        // and two attempts could claim generations in one order and call
        // Kotlin in the other, so the older one replaced the newer. Released
        // before awaiting the result, so a cancelled attempt's cleanup can
        // still acquire it.
        let serialise = self.inner.advertise_lock.lock().await;
        let advertise_generation = self
            .inner
            .advertise_generation
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        // `startAdvertising` replaces the server; set again on success.
        *self.inner.served_service.lock().unwrap() = None;
        // Scoped: `JNIEnv` is `!Send`, so it must be entirely out of scope
        // before the `.await` below, or the whole future stops being `Send`
        // and no longer satisfies the trait.
        let rx = {
            let mut env = self.inner.env()?;
            let n = service.characteristics.len() as i32;

            let service_uuid = env
                .new_string(service.uuid.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let char_uuids = env
                .new_object_array(n, "java/lang/String", JObject::null())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let readable = env
                .new_boolean_array(n)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let writable = env
                .new_boolean_array(n)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let notifiable = env
                .new_boolean_array(n)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let values = env
                .new_object_array(n, "[B", JObject::null())
                .map_err(|err| BleError::Gatt(err.to_string()))?;

            for (i, characteristic) in service.characteristics.iter().enumerate() {
                let i = i as i32;
                let uuid_str = env
                    .new_string(characteristic.uuid.0.to_string())
                    .map_err(|err| BleError::Gatt(err.to_string()))?;
                env.set_object_array_element(&char_uuids, i, &uuid_str)
                    .map_err(|err| BleError::Gatt(err.to_string()))?;
                env.set_boolean_array_region(
                    &readable,
                    i,
                    &[jboolean::from(characteristic.readable)],
                )
                .map_err(|err| BleError::Gatt(err.to_string()))?;
                env.set_boolean_array_region(
                    &writable,
                    i,
                    &[jboolean::from(characteristic.writable)],
                )
                .map_err(|err| BleError::Gatt(err.to_string()))?;
                env.set_boolean_array_region(
                    &notifiable,
                    i,
                    &[jboolean::from(characteristic.notifiable)],
                )
                .map_err(|err| BleError::Gatt(err.to_string()))?;
                let value = env
                    .byte_array_from_slice(&characteristic.initial_value)
                    .map_err(|err| BleError::Gatt(err.to_string()))?;
                env.set_object_array_element(&values, i, &value)
                    .map_err(|err| BleError::Gatt(err.to_string()))?;
            }

            let ad = AdvertisementArgs::new(&mut env, &service)?;

            let bridge = self.inner.bridge()?;
            let _monitor = env
                .lock_obj(bridge.as_obj())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let (tx, rx) = oneshot::channel();
            *self.inner.advertise_tx.lock().unwrap() = Some(PendingAdvertisement {
                sender: tx,
                service: None,
            });
            self.inner.call_void(
                &mut env,
                "startAdvertising",
                "(Ljava/lang/String;[Ljava/lang/String;[Z[Z[Z[[B[I[[B[Ljava/lang/String;[[B[BI[BI)V",
                &[
                    JValue::Object(&service_uuid),
                    JValue::Object(&char_uuids),
                    JValue::Object(&readable),
                    JValue::Object(&writable),
                    JValue::Object(&notifiable),
                    JValue::Object(&values),
                    JValue::Object(&ad.manufacturer_ids),
                    JValue::Object(&ad.manufacturer_values),
                    JValue::Object(&ad.service_data_uuids),
                    JValue::Object(&ad.service_data_values),
                    JValue::Object(&ad.scan_response_service_data),
                    JValue::Int(ad.fast_company_id),
                    JValue::Object(&ad.fast_data),
                    JValue::Int(ad.advertise_mode),
                ],
            )?;
            rx
        };

        drop(serialise);

        // Armed once the platform call has been issued. A caller that times
        // out or drops this future runs none of the code below, and dropping
        // `rx` performs no platform cleanup — so Kotlin would go on to
        // finish service registration and start advertising for an attempt
        // nobody is waiting on. That leaves a live server the caller never
        // received, and a retry then either overwrites the bridge's sole
        // server reference or fails as already advertising.
        let guard = AdvertiseGuard {
            inner: self.inner.clone(),
            generation: advertise_generation,
            armed: true,
        };

        let outcome = match rx.await {
            Ok(Ok(())) => {
                log::info!("advertise: started, generation={advertise_generation}");
                // Only while this attempt is still current: a stop or a newer
                // attempt that ran after this one's lock was released owns
                // the server now.
                if self.inner.advertise_generation.load(Ordering::SeqCst) == advertise_generation {
                    *self.inner.served_service.lock().unwrap() = Some(service);
                }
                Ok(())
            }
            Ok(Err(code)) => {
                log::warn!("advertise: Android refused to start (AdvertiseCallback error {code})");
                Err(BleError::Gatt(format!(
                    "Android refused to start advertising (AdvertiseCallback error {code})"
                )))
            }
            Err(_) => {
                log::warn!("advertise: outcome was never reported");
                Err(BleError::Gatt(
                    "advertise outcome was never reported".to_string(),
                ))
            }
        };
        // Disarmed whenever an outcome was actually observed, success or
        // failure: success hands the caller a server it now owns, and a
        // failure has already been torn down by `failAdvertise` on the
        // Kotlin side. The guard exists solely for the path where this
        // future is dropped before `rx` resolves, where neither is true.
        let mut guard = guard;
        guard.armed = false;
        outcome
    }

    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service.lock().unwrap().clone()
    }

    /// Replace `characteristic.value` through `BleGattBridge.setCharacteristicValue`.
    /// The Kotlin read handler returns this stored value.
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let mut env = self.inner.env()?;
        let uuid = env
            .new_string(characteristic.0.to_string())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let bytes = env
            .byte_array_from_slice(&value)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let bridge = self.inner.bridge()?;
        let update_result = env
            .call_method(
                bridge.as_obj(),
                "setCharacteristicValue",
                "(Ljava/lang/String;[B)Z",
                &[JValue::Object(&uuid), JValue::Object(&bytes)],
            )
            .and_then(jni::objects::JValueGen::z);
        match update_result {
            Ok(true) => Ok(()),
            Ok(false) => Err(BleError::Gatt(format!(
                "set_characteristic_value: {} is not served",
                characteristic.0
            ))),
            Err(err) => Err(BleError::jni_error(&mut env, "setCharacteristicValue", err)),
        }
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        // Held until the outcome arrives, so no advertise or stop can install
        // its own waiter in between and receive this update's result.
        let _serialise = self.inner.advertise_lock.lock().await;
        let serving = self
            .inner
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
        // Scoped: `JNIEnv` is `!Send` (see `advertise`).
        let rx = {
            let mut env = self.inner.env()?;
            let service_uuid = env
                .new_string(service.uuid.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let ad = AdvertisementArgs::new(&mut env, &service)?;
            let bridge = self.inner.bridge()?;
            let _monitor = env
                .lock_obj(bridge.as_obj())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let (tx, rx) = oneshot::channel();
            *self.inner.advertise_tx.lock().unwrap() = Some(PendingAdvertisement {
                sender: tx,
                service: Some(service),
            });
            let result = self.inner.call_void(
                &mut env,
                "updateAdvertising",
                "(Ljava/lang/String;[I[[B[Ljava/lang/String;[[B[BI[BI)V",
                &[
                    JValue::Object(&service_uuid),
                    JValue::Object(&ad.manufacturer_ids),
                    JValue::Object(&ad.manufacturer_values),
                    JValue::Object(&ad.service_data_uuids),
                    JValue::Object(&ad.service_data_values),
                    JValue::Object(&ad.scan_response_service_data),
                    JValue::Int(ad.fast_company_id),
                    JValue::Object(&ad.fast_data),
                    JValue::Int(ad.advertise_mode),
                ],
            );
            if let Err(error) = result {
                self.inner.advertise_tx.lock().unwrap().take();
                return Err(error);
            }
            rx
        };
        // The native callback owns snapshot commit, including after this waiter is cancelled.
        match rx.await {
            Ok(Ok(())) => {
                log::info!("advertise: advertisement updated, GATT server kept");
                Ok(())
            }
            Ok(Err(code)) => {
                log::warn!("advertise: Android refused the updated advertisement (error {code})");
                Err(BleError::Gatt(format!(
                    "Android refused the updated advertisement (AdvertiseCallback error {code})"
                )))
            }
            Err(_) => Err(BleError::Gatt(
                "advertisement update outcome was never reported".to_string(),
            )),
        }
    }

    async fn set_advertising_enabled(&self, enabled: bool) -> Result<()> {
        let _serialise = self.inner.advertise_lock.lock().await;
        if self.inner.served_service.lock().unwrap().is_none() {
            return Err(BleError::Gatt(
                "advertising visibility: nothing is served".into(),
            ));
        }
        let rx = {
            let mut env = self.inner.env()?;
            if self.inner.call_int(&mut env, "bridgeAbiVersion", "()I")? != BRIDGE_ABI_VERSION {
                return Err(BleError::Unsupported(format!(
                    "advertising visibility requires Android bridge ABI {BRIDGE_ABI_VERSION}"
                )));
            }
            let bridge = self.inner.bridge()?;
            let _monitor = env
                .lock_obj(bridge.as_obj())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let (tx, rx) = oneshot::channel();
            *self.inner.advertise_tx.lock().unwrap() = Some(PendingAdvertisement {
                sender: tx,
                service: None,
            });
            if let Err(err) = self.inner.call_void(
                &mut env,
                "setAdvertisingEnabled",
                "(Z)V",
                &[JValue::Bool(jboolean::from(enabled))],
            ) {
                self.inner.advertise_tx.lock().unwrap().take();
                return Err(err);
            }
            rx
        };
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(code)) => Err(BleError::Gatt(format!(
                "Android refused advertising visibility (error {code})"
            ))),
            Err(_) => Err(BleError::Gatt(
                "advertising visibility outcome was not reported".into(),
            )),
        }
    }

    async fn stop_advertising(&self) -> Result<()> {
        // Same lock `advertise` holds across its platform call. Without it a
        // stop could land between an attempt installing its sender and
        // issuing `startAdvertising`: the stop resolves that sender and
        // stops whatever is visible, then the advertise resumes and starts a
        // server *after* stop has already returned — and because the attempt
        // observed an outcome, its guard is disarmed and nothing cleans it
        // up.
        let _serialise = self.inner.advertise_lock.lock().await;
        let mut env = self.inner.env()?;
        let bridge = self.inner.bridge()?;
        let _monitor = env
            .lock_obj(bridge.as_obj())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        *self.inner.served_service.lock().unwrap() = None;
        // Resolve any advertise still waiting on Android's asynchronous
        // start result. Kotlin invalidates the server generation, so that
        // callback is deliberately ignored when it arrives — which means
        // nothing else would ever complete this waiter and `advertise()`
        // would hang for the life of the process.
        if let Some(pending) = self.inner.advertise_tx.lock().unwrap().take() {
            let _ = pending.send(Err(ADVERTISE_ERROR_STOPPED));
        }
        log::info!("advertise: stopping");
        self.inner
            .call_void(&mut env, "stopAdvertising", "()V", &[])
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        // Broadcast is expressed as one addressed send per subscriber, so
        // every payload goes through the same queued, completion-confirmed
        // path — Android has no atomic multi-device notify anyway.
        let subscribers = self.subscribed_peers(characteristic)?;
        if subscribers.is_empty() {
            return Err(BleError::Gatt(
                "notify reached no subscriber — not advertising, unknown characteristic, \
                 or nobody subscribed"
                    .to_string(),
            ));
        }
        let mut delivered = false;
        let mut last_error = None;
        for peer in subscribers {
            match self
                .notify_peer(&peer, None, characteristic, value.clone())
                .await
            {
                Ok(()) => delivered = true,
                Err(err) => last_error = Some(err),
            }
        }
        if !delivered {
            return Err(last_error
                .unwrap_or_else(|| BleError::Gatt("notify reached no subscriber".to_string())));
        }
        Ok(())
    }

    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        // The session is validated inside Kotlin, atomically with selecting
        // the subscriber and enqueuing. Checking it here would be a separate
        // transition, and Rust cannot hold `server_sessions` across the JNI
        // call without completing a deadlock cycle through the bridge
        // monitor. 0 means "whichever session holds this address".
        let request_id = self.inner.next_notify_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner
            .notify_waiters
            .lock()
            .unwrap()
            .insert(request_id, tx);

        let queued = (|| -> Result<bool> {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&peer.0)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bytes = env
                .byte_array_from_slice(&value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bridge = self.inner.bridge()?;
            let result = env
                .call_method(
                    bridge.as_obj(),
                    "notifyCharacteristicTo",
                    "(Ljava/lang/String;Ljava/lang/String;[BJJ)Z",
                    &[
                        JValue::Object(&address),
                        JValue::Object(&uuid),
                        JValue::Object(&bytes),
                        JValue::Long(request_id as i64),
                        JValue::Long(session.unwrap_or(0) as i64),
                    ],
                )
                .and_then(jni::objects::JValueGen::z);
            match result {
                Ok(value) => Ok(value),
                Err(err) => Err(BleError::jni_error(&mut env, "notifyCharacteristicTo", err)),
            }
        })();

        match queued {
            // Nothing was queued, so no callback is coming — drop the waiter
            // rather than leaving this future to hang forever.
            Ok(false) | Err(_) => {
                log::warn!(
                    "notify: {} was not queued for {} — no live notify session",
                    value.len(),
                    peer.0
                );
                self.inner
                    .notify_waiters
                    .lock()
                    .unwrap()
                    .remove(&request_id);
                queued?;
                return Err(BleError::Gatt(format!(
                    "{} has no live notify session for this characteristic",
                    peer.0
                )));
            }
            Ok(true) => {}
        }

        // Android reports the real status here, not from the call above.
        // Returning Ok on initiation alone would tell a reliable caller a
        // fragment was sent when transmission had actually failed.
        match rx.await {
            Ok(true) => {
                log::trace!("notify: {} bytes delivered to {}", value.len(), peer.0);
                Ok(())
            }
            Ok(false) => {
                log::warn!(
                    "notify: {} bytes to {} failed in transmission",
                    value.len(),
                    peer.0
                );
                Err(BleError::Gatt(format!("notify to {} failed", peer.0)))
            }
            Err(_) => {
                log::warn!("notify: send to {} was abandoned before completion", peer.0);
                Err(BleError::Gatt(format!(
                    "notify to {} was abandoned before completion",
                    peer.0
                )))
            }
        }
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        // The session is validated inside Kotlin, atomically with selecting
        // and cancelling the device. Checking it here and then calling would
        // be two transitions, and a Binder callback can replace the
        // subscription in between. 0 means "whichever session holds this
        // address", matching `None` at the port.
        let mut env = self.inner.env()?;
        let address = env
            .new_string(&peer.0)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        self.inner.call_void(
            &mut env,
            "disconnectServerPeer",
            "(Ljava/lang/String;J)V",
            &[
                JValue::Object(&address),
                JValue::Long(session.unwrap_or(0) as i64),
            ],
        )
    }

    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.inner.server_events_tx.lifecycle_events()
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.inner.server_events_tx.subscribe();
        Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx).map(|item| match item {
                Ok(event) => event,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    GattEvent::Lagged { dropped: n }
                }
            }),
        )
    }

    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        if let Some(session) = session {
            if self.inner.server_sessions.lock().unwrap().get(&peer.0) != Some(&session) {
                return None;
            }
        }
        // Filled by `onMtuChanged` from either role's callback: one ATT
        // bearer per link, so a central's MTU exchange on our server lands
        // in the same per-address entry.
        let mtu = *self.inner.att_mtus.lock().unwrap().get(&peer.0)?;
        Some(
            (mtu as usize)
                .saturating_sub(crate::hal::ATT_HEADER_LEN)
                .min(crate::hal::MAX_ATT_ATTRIBUTE_LEN),
        )
    }

    async fn radio_status(&self) -> RadioStatus {
        let enabled = (|| -> Result<bool> {
            let mut env = self.inner.env()?;
            self.inner.call_bool(&mut env, "isRadioEnabled", "()Z")
        })();
        match enabled {
            Ok(true) => RadioStatus::On,
            Ok(false) => RadioStatus::Off,
            // A JNI failure here is not itself a radio state — assume the
            // common case and let the receiver correct it on the next
            // toggle.
            Err(_) => RadioStatus::On,
        }
    }
}
