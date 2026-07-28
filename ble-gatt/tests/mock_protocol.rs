//! Protocol-level tests against the mock backend — CI-safe, no radio, no
//! BlueZ daemon required. Exercises the full `Backend`/`GattConnection`
//! contract exactly as a real backend consumer would: scan, connect,
//! read/write, and server-initiated notify.

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::{
    Backend, CapabilityReport, CharacteristicUuid, GattCharacteristicSpec, GattServiceSpec,
    PeerAddress, ServiceUuid,
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

    // `connect` itself fires a `Connected` event — drain it before the write.
    let _connected = events.next().await.expect("connected event");

    connection
        .write(characteristic_uuid, b"updated".to_vec())
        .await
        .expect("write should succeed");

    let written_event = events.next().await.expect("write event");
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
