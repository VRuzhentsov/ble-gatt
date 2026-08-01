//! Protocol-level tests against the mock backend — CI-safe, no radio, no
//! BlueZ daemon required. Exercises the full `Backend`/`GattConnection`
//! contract exactly as a real backend consumer would: scan, connect,
//! read/write, and server-initiated notify.

use std::collections::BTreeMap;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::{
    Backend, CapabilityReport, CharacteristicUuid, GattCharacteristicSpec, GattEvent,
    GattServiceSpec, PeerAddress, ServiceUuid, WriteType,
};
use tokio_stream::StreamExt;
use uuid::Uuid;

fn full_capabilities() -> CapabilityReport {
    CapabilityReport {
        central: true,
        peripheral: true,
    }
}

#[tokio::test]
async fn central_discovers_and_reads_the_peripherals_advertised_service() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());

    let peripheral = MockBackend::new(
        PeerAddress("peripheral-1".to_string()),
        network.clone(),
        full_capabilities(),
    );
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"hello".to_vec(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-1".to_string()), network.clone(), full_capabilities());

    let mut discovered = central.scan(service_uuid).await.expect("scan should succeed");
    let peer = discovered
        .next()
        .await
        .expect("peripheral should be discovered")
        .expect("discovery should not error");
    assert_eq!(peer.address, PeerAddress("peripheral-1".to_string()));

    let mut connection = central.connect(&peer.address).await.expect("connect should succeed");
    let value = connection.read(characteristic_uuid).await.expect("read should succeed");
    assert_eq!(value, b"hello");
}

#[tokio::test]
async fn write_from_central_is_readable_back_and_fires_a_lifecycle_event() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-2".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: false,
                initial_value: Vec::new(),
            }],
        })
        .await
        .expect("advertise should succeed");
    let mut events = peripheral.events();

    let central = MockBackend::new(PeerAddress("central-2".to_string()), network.clone(), full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");

    connection
        .write(characteristic_uuid, b"updated".to_vec())
        .await
        .expect("write should succeed");

    // `Connected` is not once-per-connection: it fires on `connect` *and*
    // again ahead of every write, because neither real backend has a
    // server-side connection signal to key off. Consumers must tolerate
    // repeats, so this skips them rather than asserting a fixed sequence.
    let written_event = loop {
        match events.next().await.expect("write event should arrive") {
            ble_gatt::GattEvent::Connected { .. } => continue,
            other => break other,
        }
    };
    match written_event {
        ble_gatt::GattEvent::CharacteristicWritten { characteristic, value, .. } => {
            assert_eq!(characteristic, characteristic_uuid);
            assert_eq!(value, b"updated");
        }
        other => panic!("expected CharacteristicWritten, got {other:?}"),
    }

    let read_back = connection.read(characteristic_uuid).await.expect("read should succeed");
    assert_eq!(read_back, b"updated");
}

#[tokio::test]
async fn server_initiated_notify_is_delivered_to_a_subscribed_central() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-3".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: false,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-3".to_string()), network.clone(), full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");
    let mut notifications = connection.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    peripheral
        .notify(characteristic_uuid, b"push".to_vec())
        .await
        .expect("notify should succeed");

    let received = notifications
        .next()
        .await
        .expect("notification should arrive")
        .expect("no gap");
    assert_eq!(received, b"push");
}

#[tokio::test]
async fn connect_to_a_peer_that_is_not_advertising_fails() {
    let network = MockNetwork::new();
    let central = MockBackend::new(PeerAddress("central-4".to_string()), network.clone(), full_capabilities());

    let result = central.connect(&PeerAddress("nobody-home".to_string())).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn advertise_on_a_peripheral_incapable_backend_is_rejected_honestly() {
    let network = MockNetwork::new();
    let central_only = CapabilityReport {
        central: true,
        peripheral: false,
    };
    let backend = MockBackend::new(PeerAddress("central-only".to_string()), network, central_only);

    let result = backend
        .advertise(GattServiceSpec {
            uuid: ServiceUuid(Uuid::new_v4()),
            characteristics: vec![],
        })
        .await;

    assert!(matches!(result, Err(ble_gatt::BleError::PeripheralUnsupported)));
}

// ---------------------------------------------------------------------
// Capabilities added for consumers that drive third-party device
// protocols (identity-in-advertisement, bulk transfer, link loss).
// ---------------------------------------------------------------------

#[tokio::test]
async fn advertisement_payload_identifies_a_specific_peer_before_connecting() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-adv".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![],
        })
        .await
        .expect("advertise should succeed");

    // A vendor device publishing its real identity (e.g. a device EUI) in
    // manufacturer-specific data, which is frequently the only way to tell
    // two units of the same product apart.
    let device_eui = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
    let mut manufacturer_data = BTreeMap::new();
    manufacturer_data.insert(0x1234u16, device_eui.clone());
    let mut service_data = BTreeMap::new();
    service_data.insert(service_uuid, b"peer-id-payload".to_vec());
    network.set_advertisement_data(
        &peripheral_addr,
        manufacturer_data,
        service_data,
        Some(-55),
    );

    let central = MockBackend::new(PeerAddress("central-adv".to_string()), network.clone(), full_capabilities());
    let mut discovered = central.scan(service_uuid).await.expect("scan should succeed");
    let peer = discovered
        .next()
        .await
        .expect("peripheral should be discovered")
        .expect("discovery should not error");

    assert_eq!(peer.manufacturer_data.get(&0x1234), Some(&device_eui));
    assert_eq!(
        peer.service_data.get(&service_uuid).map(|v| v.as_slice()),
        Some(b"peer-id-payload".as_slice())
    );
    assert_eq!(peer.rssi, Some(-55));
}

#[tokio::test]
async fn connection_reports_an_mtu_that_bounds_a_single_write() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-mtu".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-mtu".to_string()), network.clone(), full_capabilities());
    let connection = central.connect(&peripheral_addr).await.expect("connect should succeed");

    // The point of exposing this at all: a caller chunking a bulk transfer
    // must derive the chunk size from the connection rather than hardcoding
    // one, since it is only known after negotiation and varies per peer.
    assert!(connection.att_mtu() >= ble_gatt::backend::DEFAULT_ATT_MTU);
    assert_eq!(
        connection.max_write_len(),
        connection.att_mtu() as usize - ble_gatt::backend::ATT_HEADER_LEN
    );
}

#[tokio::test]
async fn write_without_response_is_accepted_and_still_delivers() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-wnr".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: false,
                initial_value: Vec::new(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-wnr".to_string()), network.clone(), full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");

    connection
        .write_with_type(characteristic_uuid, b"bulk-chunk".to_vec(), WriteType::WithoutResponse)
        .await
        .expect("unacknowledged write should succeed");

    let read_back = connection.read(characteristic_uuid).await.expect("read should succeed");
    assert_eq!(read_back, b"bulk-chunk");
}

#[tokio::test]
async fn central_learns_about_unsolicited_peer_loss_through_the_event_stream() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-drop".to_string());
    let central_addr = PeerAddress("central-drop".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(central_addr.clone(), network.clone(), full_capabilities());
    // Subscribe before connecting: a central-only consumer must be able to
    // observe events without ever advertising.
    let mut events = central.events();
    let _connection = central.connect(&peripheral_addr).await.expect("connect should succeed");

    let connected = events.next().await.expect("connected event");
    assert!(matches!(connected, GattEvent::Connected { .. }));

    // The peer vanishes — no API call on our side. This is the case that
    // would otherwise leave a caller hanging mid-transfer.
    peripheral.simulate_peer_loss(&central_addr);

    let lost = events.next().await.expect("disconnect event should reach the central");
    match lost {
        GattEvent::Disconnected { peer, .. } => assert_eq!(peer, peripheral_addr),
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

#[tokio::test]
async fn an_asynchronous_scan_failure_is_reported_instead_of_looking_like_no_peers() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());

    // The failure mode this guards: Android's `onScanFailed` arrives *after*
    // `scan()` has already returned Ok, so a caller that only sees the stream
    // end would report "no devices found" when Bluetooth is actually off or
    // the scan permission was denied.
    network.arm_scan_failure("scan failed (ScanCallback error code 2)");

    let central = MockBackend::new(
        PeerAddress("central-scanfail".to_string()),
        network.clone(),
        full_capabilities(),
    );
    let mut discovered = central.scan(service_uuid).await.expect("scan starts successfully");

    let item = discovered.next().await.expect("the failure must surface as an item");
    assert!(
        item.is_err(),
        "a failed scan must be distinguishable from an empty one, got {item:?}"
    );
    assert!(discovered.next().await.is_none(), "an error item ends the scan");
}

#[tokio::test]
async fn a_central_refused_by_a_single_peer_server_stops_receiving_notifications() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-exclusive".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: true,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        })
        .await
        .expect("advertise should succeed");

    // Two centrals subscribe. Subscribing needs no server consent, so at
    // this point the server has no say in who receives its broadcasts.
    let first = MockBackend::new(PeerAddress("central-first".to_string()), network.clone(), full_capabilities());
    let mut first_conn = first.connect(&peripheral_addr).await.expect("connect should succeed");
    let mut first_rx = first_conn.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    let refused_addr = PeerAddress("central-refused".to_string());
    let refused = MockBackend::new(refused_addr.clone(), network.clone(), full_capabilities());
    let mut refused_conn = refused.connect(&peripheral_addr).await.expect("connect should succeed");
    let mut refused_rx = refused_conn.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    // Proof the hazard is real rather than hypothetical: before exclusion, a
    // broadcast reaches the peer the server intends to refuse.
    peripheral
        .notify(characteristic_uuid, b"for-the-served-peer".to_vec())
        .await
        .expect("notify should succeed");
    assert_eq!(
        refused_rx.next().await.and_then(|item| item.ok()).as_deref(),
        Some(b"for-the-served-peer".as_slice()),
        "precondition: notify is a broadcast, so an un-excluded peer does receive it"
    );
    // The served peer received it too — drain so the assertion below reads
    // the notification sent *after* exclusion, not this one.
    assert_eq!(
        first_rx.next().await.and_then(|item| item.ok()).as_deref(),
        Some(b"for-the-served-peer".as_slice())
    );

    // What a single-peer server must actually do about it.
    peripheral
        .disconnect_peer(&refused_addr, None)
        .await
        .expect("disconnect_peer should succeed");

    peripheral
        .notify(characteristic_uuid, b"private-payload".to_vec())
        .await
        .expect("notify should succeed");

    assert_eq!(
        first_rx.next().await.and_then(|item| item.ok()).as_deref(),
        Some(b"private-payload".as_slice()),
        "the served central must still receive its own traffic"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), refused_rx.next())
            .await
            .map(|item| item.is_none())
            .unwrap_or(true),
        "a disconnected central must not receive the served peer's fragments"
    );
}

#[tokio::test]
async fn an_addressed_notify_reaches_only_its_peer() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-addressed".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: false,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let served_addr = PeerAddress("central-served".to_string());
    let served = MockBackend::new(served_addr.clone(), network.clone(), full_capabilities());
    let mut served_conn = served.connect(&peripheral_addr).await.expect("connect");
    let mut served_rx = served_conn.subscribe(characteristic_uuid).await.expect("subscribe");

    // A second central subscribes. It needs no server consent to do so, and
    // disconnecting it is asynchronous — so during that window a *broadcast*
    // would hand it the served peer's traffic. Addressing is what closes the
    // window rather than narrowing it.
    let other_addr = PeerAddress("central-other".to_string());
    let other = MockBackend::new(other_addr.clone(), network.clone(), full_capabilities());
    let mut other_conn = other.connect(&peripheral_addr).await.expect("connect");
    let mut other_rx = other_conn.subscribe(characteristic_uuid).await.expect("subscribe");

    peripheral
        .notify_peer(&served_addr, characteristic_uuid, b"for-the-served-peer".to_vec())
        .await
        .expect("addressed notify should succeed");

    assert_eq!(
        served_rx.next().await.and_then(|item| item.ok()).as_deref(),
        Some(b"for-the-served-peer".as_slice())
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), other_rx.next())
            .await
            .is_err(),
        "an addressed notify must not reach a peer it was not addressed to"
    );

    // And addressing a peer with no session is an error, not a silent no-op.
    assert!(
        peripheral
            .notify_peer(
                &PeerAddress("nobody".to_string()),
                characteristic_uuid,
                b"x".to_vec()
            )
            .await
            .is_err(),
        "notifying an unsubscribed peer must report failure"
    );
}

#[tokio::test]
async fn a_stale_connection_handle_cannot_disturb_the_session_that_replaced_it() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-stale-handle".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"v".to_vec(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(
        PeerAddress("central-stale-handle".to_string()),
        network.clone(),
        full_capabilities(),
    );

    // A handle held across an unsolicited drop and reconnect. Both are keyed
    // by address, so without a session check the old handle operates on the
    // new session — and its `disconnect` tears down a live connection.
    let mut stale = central.connect(&peripheral_addr).await.expect("connect");
    let mut current = central.connect(&peripheral_addr).await.expect("reconnect");

    assert!(
        stale.disconnect().await.is_err(),
        "a superseded handle must refuse to disconnect the session that replaced it"
    );
    assert!(
        stale.read(characteristic_uuid).await.is_err(),
        "a superseded handle must refuse to read through the replacement"
    );

    // The replacement is untouched.
    let value = current
        .read(characteristic_uuid)
        .await
        .expect("the current connection must still be usable");
    assert_eq!(value, b"v");
}

#[tokio::test]
async fn a_disconnected_handle_is_actually_closed() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-closed".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"v".to_vec(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(
        PeerAddress("central-closed".to_string()),
        network.clone(),
        full_capabilities(),
    );
    let mut connection = central.connect(&peripheral_addr).await.expect("connect");
    connection.read(characteristic_uuid).await.expect("read while open");
    connection.disconnect().await.expect("disconnect");

    // Everything must now be refused. A mock that kept accepting these would
    // let post-disconnect use pass tests that the real backends reject.
    assert!(
        connection.read(characteristic_uuid).await.is_err(),
        "a closed connection must refuse reads"
    );
    assert!(
        connection.write(characteristic_uuid, b"x".to_vec()).await.is_err(),
        "a closed connection must refuse writes"
    );
    assert!(
        connection.subscribe(characteristic_uuid).await.is_err(),
        "a closed connection must refuse subscribes"
    );
    assert!(
        connection.disconnect().await.is_err(),
        "a closed connection must refuse a second disconnect"
    );
}

#[tokio::test]
async fn simulated_peer_loss_closes_the_link_as_well_as_announcing_it() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-loss-state".to_string());
    let central_addr = PeerAddress("central-loss-state".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"v".to_vec(),
            }],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(central_addr.clone(), network.clone(), full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect");
    connection.read(characteristic_uuid).await.expect("read while connected");

    // The documented link-loss simulation must leave the same state a real
    // drop would: not merely an event, but a connection that no longer works.
    peripheral.simulate_peer_loss(&central_addr);

    assert!(
        connection.read(characteristic_uuid).await.is_err(),
        "a lost link must refuse reads"
    );
    assert!(
        connection.write(characteristic_uuid, b"x".to_vec()).await.is_err(),
        "a lost link must refuse writes"
    );
    assert!(
        peripheral
            .notify_peer(&central_addr, characteristic_uuid, b"n".to_vec())
            .await
            .is_err(),
        "a lost peer must no longer be reachable by notify"
    );
}

#[tokio::test]
async fn writes_to_unknown_or_read_only_characteristics_are_refused() {
    let network = MockNetwork::new();
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let writable_uuid = CharacteristicUuid(Uuid::new_v4());
    let read_only_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-perms".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    peripheral
        .advertise(GattServiceSpec {
            uuid: service_uuid,
            characteristics: vec![
                GattCharacteristicSpec {
                    uuid: writable_uuid,
                    readable: true,
                    writable: true,
                    notifiable: false,
                    initial_value: Vec::new(),
                },
                GattCharacteristicSpec {
                    uuid: read_only_uuid,
                    readable: true,
                    writable: false,
                    notifiable: false,
                    initial_value: b"ro".to_vec(),
                },
            ],
        })
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(
        PeerAddress("central-perms".to_string()),
        network.clone(),
        full_capabilities(),
    );
    let mut connection = central.connect(&peripheral_addr).await.expect("connect");

    connection
        .write(writable_uuid, b"ok".to_vec())
        .await
        .expect("a declared writable characteristic must accept writes");

    // Neither real backend exposes a writable characteristic for these, so a
    // mock that accepted them would let a mistyped UUID or a permissions
    // mistake pass here and fail only on a device.
    assert!(
        connection
            .write(CharacteristicUuid(Uuid::new_v4()), b"x".to_vec())
            .await
            .is_err(),
        "an unknown characteristic must be refused"
    );
    assert!(
        connection.write(read_only_uuid, b"x".to_vec()).await.is_err(),
        "a characteristic advertised as not writable must be refused"
    );
    assert_eq!(
        connection.read(read_only_uuid).await.expect("read"),
        b"ro",
        "the refused write must not have mutated the value"
    );
}
