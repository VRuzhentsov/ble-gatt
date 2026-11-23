use super::*;
use dbus::channel::MatchingReceiver;

#[test]
fn the_bluez_advertisement_keeps_the_legacy_fit_and_asks_for_the_interval() {
    use std::time::Duration;
    let mut spec = GattServiceSpec::new(ServiceUuid(uuid::Uuid::from_u128(1)), vec![]);
    spec.manufacturer_data.insert(0xFFFF, vec![1, 2, 3, 4, 5]);
    spec.service_data.insert(spec.uuid, vec![0xF8; 9]);
    spec.fast_advertisement = Some(crate::FastAdvertisement {
        company_id: 0xFFFF,
        data: vec![0xFC; 26],
    });
    let advertisement = spec.bluez_advertisement(true);
    assert!(
        advertisement.service_data.is_empty(),
        "the header does not fit next to Fini's data"
    );
    assert_eq!(
        advertisement.manufacturer_data[&0xFFFF],
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(advertisement.min_interval, Some(Duration::from_millis(250)));
    assert_eq!(advertisement.max_interval, Some(Duration::from_millis(250)));
    assert_eq!(spec.bluez_advertisement(false).min_interval, None);
    // Even alone: flags 3 + UUID list 18 + header 27 = 48 bytes. BlueZ has
    // no scan response to move it to, so a Linux peer is always read.
    spec.manufacturer_data.clear();
    spec.advertising_mode = crate::AdvertisingMode::LowLatency;
    let advertisement = spec.bluez_advertisement(true);
    assert!(advertisement.service_data.is_empty());
    assert_eq!(advertisement.min_interval, Some(Duration::from_millis(100)));
    // Data that fits is kept.
    spec.service_data.clear();
    spec.service_data
        .insert(ServiceUuid(uuid::Uuid::from_u128(2)), vec![7]);
    let advertisement = spec.bluez_advertisement(true);
    assert_eq!(advertisement.service_data.len(), 1);
}

#[test]
fn a_scanned_device_is_the_target_by_service_or_by_fast_advertisement() {
    let target = uuid::Uuid::from_u128(0xB1E6);
    let other = uuid::Uuid::from_u128(7);
    let company = FastAdvertisement::DEFAULT_SCAN_COMPANY;
    let is_target = |uuids: &HashSet<uuid::Uuid>,
                     manufacturer: &std::collections::BTreeMap<u16, Vec<u8>>| {
        ScannedPeer::is_target(uuids, target, company, manufacturer)
    };
    let no_company = std::collections::BTreeMap::new();
    assert!(is_target(&[target].into(), &no_company));
    assert!(!is_target(&[other].into(), &no_company));
    assert!(!is_target(&HashSet::new(), &no_company));
    // A Windows peer's own publisher: no services, only the fast
    // advertisement.
    let fast: std::collections::BTreeMap<u16, Vec<u8>> = [(0xFFFF, vec![0xFC; 26])].into();
    assert!(is_target(&HashSet::new(), &fast));
    assert!(is_target(&[other].into(), &fast));
    let fini: std::collections::BTreeMap<u16, Vec<u8>> = [(0xFFFF, vec![1, 2, 3, 4, 5])].into();
    assert!(!is_target(&[other].into(), &fini));
    // The same bytes under another company are not admitted.
    let other_company: std::collections::BTreeMap<u16, Vec<u8>> = [(0x004C, vec![0xFC; 26])].into();
    assert!(!is_target(&HashSet::new(), &other_company));
}

#[test]
fn a_scan_reports_a_device_again_when_its_radio_records_change() {
    let address = bluer::Address::new([1, 2, 3, 4, 5, 6]);
    let service = ServiceUuid(uuid::Uuid::from_u128(0xB1E6));
    let mut peer = DiscoveredPeer {
        address: PeerAddress(address.to_string()),
        name: None,
        services: vec![service],
        manufacturer_data: Default::default(),
        service_data: Default::default(),
        rssi: Some(-60),
    };
    let mut reported = ScanReports::default();
    assert!(reported.record(address, &peer), "first sighting");
    assert!(!reported.record(address, &peer), "nothing changed");
    peer.rssi = Some(-40);
    assert!(!reported.record(address, &peer), "RSSI alone is no change");

    peer.manufacturer_data.insert(0xFFFF, vec![0xFC; 26]);
    assert!(reported.record(address, &peer), "manufacturer data arrived");
    // After the first complete report: new bytes under the same company.
    peer.manufacturer_data.insert(0xFFFF, vec![0xFD; 26]);
    assert!(reported.record(address, &peer), "manufacturer data changed");
    assert!(!reported.record(address, &peer));
    peer.service_data.insert(service, vec![0xF8; 9]);
    assert!(reported.record(address, &peer), "service data arrived");
    peer.service_data.insert(service, vec![0xF9; 9]);
    assert!(reported.record(address, &peer), "service data changed");
    peer.name = Some("peer".to_string());
    assert!(reported.record(address, &peer), "name changed");
    assert!(!reported.record(address, &peer));

    let other = bluer::Address::new([6, 5, 4, 3, 2, 1]);
    assert!(
        reported.record(other, &peer),
        "each device is tracked apart"
    );
}

#[test]
fn in_progress_maps_to_adapter_busy_not_unavailable() {
    let busy = bluer::Error {
        kind: bluer::ErrorKind::InProgress,
        message: "Operation already in progress".into(),
    };
    assert!(matches!(
        BleError::bluez_discovery(busy),
        BleError::AdapterBusy(_)
    ));
    let off = bluer::Error {
        kind: bluer::ErrorKind::NotReady,
        message: "Resource Not Ready".into(),
    };
    assert!(matches!(
        BleError::bluez_discovery(off),
        BleError::AdapterUnavailable(_)
    ));
}

#[test]
fn stop_is_owed_only_when_running_with_no_live_scan() {
    let mut state = ScanState::default();
    assert!(!state.idle_running());
    state.running = true;
    state.live = 1;
    assert!(!state.idle_running());
    state.live = 0;
    assert!(state.idle_running());
}

/// A fake BlueZ adapter with BlueZ's per-client discovery rules
/// (`src/adapter.c`): `StartDiscovery` from a client already in the
/// discovery list is refused with `InProgress`, and `Discovering` is
/// true while any client is in it. `fail_next_stop` makes the next
/// `StopDiscovery` fail *and leave the client listed* — the wedge seen
/// on real hardware.
#[derive(Default)]
struct FakeAdapter {
    clients: HashSet<String>,
    fail_next_stop: bool,
    starts: usize,
    /// Held inside the `StartDiscovery` handler before it replies, so a
    /// test can cancel the caller after BlueZ has accepted the call.
    start_delay: std::time::Duration,
    powered: bool,
}

type Fake = Arc<StdMutex<FakeAdapter>>;

/// A private `dbus-daemon`, killed on drop.
///
/// libdbus reads the system bus address once per process, so only one
/// test in this binary can use a private bus: every scenario that needs
/// one lives in `discovery_recovers_from_a_failed_stop`.
struct PrivateBus(std::process::Child);

impl Drop for PrivateBus {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts a private bus, points this process's *system* bus at it, and
/// serves `FakeAdapter` as `org.bluez` `/org/bluez/hci0` on it. `None`
/// when no `dbus-daemon` is installed.
async fn fake_bluez() -> Option<(PrivateBus, Fake, Arc<dbus::nonblock::SyncConnection>)> {
    use std::io::BufRead;
    let mut child = std::process::Command::new("dbus-daemon")
        .args(["--session", "--nofork", "--print-address"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .ok()?;
    let mut address = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut address)
        .unwrap();
    let bus = PrivateBus(child);
    // Only this module's tests touch the system bus.
    std::env::set_var("DBUS_SYSTEM_BUS_ADDRESS", address.trim());

    let (resource, conn) = dbus_tokio::connection::new_system_sync().unwrap();
    tokio::spawn(resource);
    conn.request_name("org.bluez", false, true, false)
        .await
        .unwrap();
    let fake = Fake::default();
    let mut cr = dbus_crossroads::Crossroads::new();
    cr.set_async_support(Some((
        conn.clone(),
        Box::new(|task| {
            tokio::spawn(task);
        }),
    )));
    let iface = cr.register(
        "org.bluez.Adapter1",
        |b: &mut dbus_crossroads::IfaceBuilder<Fake>| {
            b.property::<bool, _>("Discovering")
                .get(|_, fake| Ok(!fake.lock().unwrap().clients.is_empty()));
            b.property::<bool, _>("Powered")
                .get(|_, fake| Ok(fake.lock().unwrap().powered));
            b.method(
                "SetDiscoveryFilter",
                ("filter",),
                (),
                |_, _, (_filter,): (dbus::arg::PropMap,)| Ok(()),
            );
            // Async so `start_delay` holds only this reply, not a runtime
            // thread.
            b.method_with_cr_async("StartDiscovery", (), (), |mut ctx, cr, ()| {
                let fake: Fake = cr.data_mut::<Fake>(ctx.path()).unwrap().clone();
                let sender = ctx.message().sender().unwrap().to_string();
                async move {
                    let delay = fake.lock().unwrap().start_delay;
                    tokio::time::sleep(delay).await;
                    let result = {
                        let mut fake = fake.lock().unwrap();
                        if fake.clients.insert(sender) {
                            fake.starts += 1;
                            Ok(())
                        } else {
                            Err((
                                "org.bluez.Error.InProgress",
                                "Operation already in progress",
                            )
                                .into())
                        }
                    };
                    ctx.reply(result)
                }
            });
            b.method("StopDiscovery", (), (), |ctx, fake, ()| {
                let sender = ctx.message().sender().unwrap().to_string();
                let mut fake = fake.lock().unwrap();
                if std::mem::take(&mut fake.fail_next_stop) {
                    return Err(("org.bluez.Error.Failed", "injected StopDiscovery failure").into());
                }
                if !fake.clients.remove(&sender) {
                    return Err(("org.bluez.Error.Failed", "No discovery started").into());
                }
                Ok(())
            });
        },
    );
    cr.insert("/org/bluez/hci0", &[iface], fake.clone());
    let bluez = conn.clone();
    conn.start_receive(
        dbus::message::MatchRule::new_method_call(),
        Box::new(move |msg, conn| {
            let _ = cr.handle_message(msg, conn);
            true
        }),
    );
    Some((bus, fake, bluez))
}

/// The real-hardware wedge, end to end against the fake. A failed
/// `StopDiscovery` that leaves our client listed must keep the stop
/// owed, so the next `connect()` retries it rather than dialling into
/// the discovery (Codex P1 on PR #21). And if our state has lost track
/// of a listed client (e.g. a start cancelled after BlueZ accepted it),
/// the next scan's `InProgress` must be cleared and the scan started
/// anyway, not fail every scan from then on. And a scan cancelled
/// mid-start must leave the stop owed. Also covers refcounted sharing.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_recovers_from_a_failed_stop() {
    let Some((_bus, fake, bluez)) = fake_bluez().await else {
        eprintln!("dbus-daemon not installed; skipping");
        return;
    };
    let session = Session::new().await.unwrap();
    let adapter = session.adapter("hci0").unwrap();
    let discovery = Arc::new(DiscoveryBus::open(&adapter).await.unwrap());

    // Two concurrent scans share one discovery.
    let first = discovery.begin_scan().await.unwrap();
    let second = discovery.begin_scan().await.unwrap();
    assert_eq!(fake.lock().unwrap().starts, 1);
    drop(first);
    assert!(
        !discovery.state.lock().unwrap().idle_running(),
        "a live scan still needs discovery"
    );

    // The last scan ends and its StopDiscovery fails: wedged, and the
    // stop must stay owed.
    fake.lock().unwrap().fail_next_stop = true;
    drop(second);
    discovery.stop_if_idle("test").await;
    assert_eq!(
        fake.lock().unwrap().clients.len(),
        1,
        "fake must now be wedged"
    );
    assert!(
        discovery.state.lock().unwrap().idle_running(),
        "a failed stop must stay owed"
    );

    // What `connect()` awaits: the owed stop is retried, and discovery
    // has really stopped once it returns.
    discovery.stop_if_idle("test").await;
    assert!(
        fake.lock().unwrap().clients.is_empty(),
        "discovery must be stopped before a dial"
    );
    assert!(!discovery.state.lock().unwrap().running);

    // Our state loses track of a client BlueZ still lists: the next scan
    // gets `InProgress`, and must recover instead of failing.
    fake.lock()
        .unwrap()
        .clients
        .insert(session_unique_name(&discovery));
    let third = discovery
        .begin_scan()
        .await
        .expect("scan must recover from the wedge");
    assert_eq!(fake.lock().unwrap().clients.len(), 1);
    assert_eq!(fake.lock().unwrap().starts, 2);
    drop(third);
    discovery.stop_if_idle("test").await;
    assert!(fake.lock().unwrap().clients.is_empty());

    // A scan cancelled after BlueZ accepted its `StartDiscovery` must
    // leave the stop owed, so the next `connect()` stops discovery
    // instead of dialling into it (Codex P1 on PR #21).
    fake.lock().unwrap().start_delay = std::time::Duration::from_millis(300);
    let cancelled =
        tokio::time::timeout(std::time::Duration::from_millis(50), discovery.begin_scan()).await;
    assert!(cancelled.is_err(), "the scan must be cancelled mid-start");
    // Let the fake finish accepting the start.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    fake.lock().unwrap().start_delay = std::time::Duration::ZERO;
    assert_eq!(
        fake.lock().unwrap().clients.len(),
        1,
        "BlueZ accepted the start"
    );
    assert!(
        discovery.state.lock().unwrap().idle_running(),
        "the stop must be owed"
    );
    discovery.stop_if_idle("test").await;
    assert!(
        fake.lock().unwrap().clients.is_empty(),
        "discovery must be stopped before a dial"
    );

    // bluetoothd restarting: `org.bluez` leaves the bus, then comes back.
    // `Off` must be reported, and `On` only once the adapter is powered.
    let events_tx = EventBus::new(8);
    let mut events = events_tx.subscribe();
    LinuxDriver::watch_bluez_restart(discovery.conn.clone(), adapter.clone(), events_tx).await;
    let next = |events: &mut tokio::sync::broadcast::Receiver<GattEvent>, wait_ms| {
        let wait = std::time::Duration::from_millis(wait_ms);
        let mut events = events.resubscribe();
        async move {
            tokio::time::timeout(wait, events.recv())
                .await
                .ok()
                .and_then(|e| e.ok())
        }
    };
    let off = next(&mut events, 2000);
    bluez.release_name("org.bluez").await.unwrap();
    assert!(
        matches!(
            off.await,
            Some(GattEvent::RadioChanged {
                status: crate::entities::models::RadioStatus::Off
            })
        ),
        "bluetoothd leaving the bus must report the radio Off"
    );

    let early = next(&mut events, 800);
    bluez
        .request_name("org.bluez", false, true, false)
        .await
        .unwrap();
    assert!(
        early.await.is_none(),
        "On must wait until the restarted adapter is powered"
    );

    let on = next(&mut events, 3000);
    fake.lock().unwrap().powered = true;
    assert!(
        matches!(
            on.await,
            Some(GattEvent::RadioChanged {
                status: crate::entities::models::RadioStatus::On
            })
        ),
        "bluetoothd back with a powered adapter must report the radio On"
    );
}

#[test]
fn only_a_definite_start_failure_clears_the_owed_stop() {
    let err = |kind| bluer::Error {
        kind,
        message: String::new(),
    };
    assert!(BleError::bluez_start_not_running(&err(
        bluer::ErrorKind::NotReady
    )));
    assert!(!BleError::bluez_start_not_running(&err(
        bluer::ErrorKind::InProgress
    )));
    assert!(!BleError::bluez_start_not_running(&err(
        bluer::ErrorKind::Internal(bluer::InternalErrorKind::DBus(
            "org.freedesktop.DBus.Error.NoReply".into()
        ))
    )));
}

fn session_unique_name(discovery: &DiscoveryBus) -> String {
    discovery.conn.unique_name().to_string()
}
