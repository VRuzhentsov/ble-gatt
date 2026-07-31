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
    let peer = discovered.next().await.expect("peripheral should be discovered");
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

    let received = notifications.next().await.expect("notification should arrive");
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
    let peer = discovered.next().await.expect("peripheral should be discovered");

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
        GattEvent::Disconnected { peer } => assert_eq!(peer, peripheral_addr),
        other => panic!("expected Disconnected, got {other:?}"),
    }
}
