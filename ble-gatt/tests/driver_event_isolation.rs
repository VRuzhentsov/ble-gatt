//! Driver-boundary tests: no consumer task is polled during the flood.
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{
    CapabilityReport, CharacteristicUuid, Driver, GattCharacteristicSpec, GattEvent,
    GattServiceSpec, PeerAddress, ServiceUuid,
};
use tokio_stream::StreamExt;
use uuid::Uuid;

async fn flood(network: std::sync::Arc<MockNetwork>, central_network: std::sync::Arc<MockNetwork>) {
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let peripheral = MockDriver::new(PeerAddress("server".into()), network, caps);
    let central = MockDriver::new(PeerAddress("client".into()), central_network, caps);
    let characteristic = CharacteristicUuid(Uuid::new_v4());
    peripheral
        .advertise(GattServiceSpec::new(
            ServiceUuid(Uuid::new_v4()),
            vec![GattCharacteristicSpec {
                uuid: characteristic,
                readable: true,
                writable: true,
                notifiable: false,
                initial_value: vec![],
            }],
        ))
        .await
        .unwrap();
    let mut critical = peripheral.lifecycle_events();
    let mut legacy = peripheral.events();
    let mut connection = central
        .connect(&PeerAddress("server".into()))
        .await
        .unwrap();
    for _ in 0..512 {
        connection.write(characteristic, vec![1]).await.unwrap();
    }
    connection.disconnect().await.unwrap();
    // A broker read barrier: observing its latest response guarantees the source
    // handled all writes; wait for the two critical transitions, never the data.
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), critical.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(first, GattEvent::Connected { .. }),
        "critical source lost Connected: {first:?}"
    );
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), critical.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(second, GattEvent::Disconnected { .. }),
        "critical source polluted by payload/repeated Connected: {second:?}"
    );
    assert!(
        matches!(legacy.next().await, Some(GattEvent::Lagged { .. })),
        "legacy flood must really overflow"
    );
}

#[tokio::test]
async fn known_mock_notification_loss_is_typed_receive_gap() {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let address = PeerAddress("server".into());
    let central_address = PeerAddress("client".into());
    let peripheral = MockDriver::new(address.clone(), network.clone(), caps);
    let central = MockDriver::new(central_address.clone(), network.clone(), caps);
    let characteristic = CharacteristicUuid(Uuid::new_v4());
    peripheral
        .advertise(GattServiceSpec::new(
            ServiceUuid(Uuid::new_v4()),
            vec![GattCharacteristicSpec {
                uuid: characteristic,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: vec![],
            }],
        ))
        .await
        .unwrap();
    let mut connection = central.connect(&address).await.unwrap();
    let mut notifications = connection.subscribe(characteristic).await.unwrap();
    network.simulate_notification_gap(&address, &central_address);
    assert!(matches!(
        notifications.next().await,
        Some(Err(ble_gatt::BleError::ReceiveGap(_)))
    ));
    peripheral.notify(characteristic, vec![2]).await.unwrap();
    assert_eq!(notifications.next().await.unwrap().unwrap(), vec![2]);
}

async fn critical_flood(
    network: std::sync::Arc<MockNetwork>,
    central_network: std::sync::Arc<MockNetwork>,
) {
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let peripheral = MockDriver::new(PeerAddress("server".into()), network, caps);
    let central = MockDriver::new(PeerAddress("client".into()), central_network, caps);
    peripheral
        .advertise(GattServiceSpec::new(ServiceUuid(Uuid::new_v4()), vec![]))
        .await
        .unwrap();
    let mut events = peripheral.lifecycle_events();
    for _ in 0..128 {
        let mut connection = central
            .connect(&PeerAddress("server".into()))
            .await
            .unwrap();
        connection.disconnect().await.unwrap();
    }
    assert!(
        matches!(events.next().await, Some(GattEvent::Lagged { dropped }) if dropped > 0),
        "real critical overflow must stay terminal, not ReceiveGap"
    );
}

#[tokio::test]
async fn local_critical_flood_reports_lag() {
    let network = MockNetwork::new();
    critical_flood(network.clone(), network).await;
}

#[tokio::test]
async fn local_payload_flood_preserves_lifecycle() {
    let network = MockNetwork::new();
    flood(network.clone(), network).await;
}

#[tokio::test]
async fn role_radio_watcher_keeps_transition_before_payload_flood() {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let peripheral = std::sync::Arc::new(MockDriver::new(
        PeerAddress("server".into()),
        network.clone(),
        caps,
    ));
    let adapter = ble_gatt::Adapter::new(peripheral.clone()).await;
    let characteristic = CharacteristicUuid(Uuid::new_v4());
    peripheral
        .advertise(GattServiceSpec::new(
            ServiceUuid(Uuid::new_v4()),
            vec![GattCharacteristicSpec {
                uuid: characteristic,
                readable: true,
                writable: true,
                notifiable: false,
                initial_value: vec![],
            }],
        ))
        .await
        .unwrap();
    let central = MockDriver::new(PeerAddress("client".into()), network, caps);
    let mut connection = central
        .connect(&PeerAddress("server".into()))
        .await
        .unwrap();
    peripheral.simulate_radio(ble_gatt::RadioStatus::Off);
    for _ in 0..512 {
        connection.write(characteristic, vec![1]).await.unwrap();
    }
    let mut status = adapter.status();
    tokio::time::timeout(
        std::time::Duration::from_millis(300),
        status.wait_for(|state| *state == ble_gatt::RadioStatus::Off),
    )
    .await
    .expect("radio transition was evicted")
    .unwrap();
}

#[cfg(feature = "mock-broker")]
#[tokio::test]
async fn broker_transport_loss_is_critical_not_payload_gap() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close, wait_close) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = wait_close.await;
        drop(socket);
    });
    let network = MockNetwork::remote(address).await.unwrap();
    let driver = MockDriver::new(
        PeerAddress("client".into()),
        network,
        CapabilityReport::default(),
    );
    let mut lifecycle = driver.lifecycle_events();
    close.send(()).unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_millis(300), lifecycle.next())
        .await
        .expect("broker closure must wake critical subscribers")
        .unwrap();
    assert!(matches!(event, GattEvent::Lagged { .. }), "{event:?}");
    server.await.unwrap();
}

#[cfg(feature = "mock-broker")]
#[tokio::test]
async fn broker_payload_flood_preserves_lifecycle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let broker = tokio::spawn(MockNetwork::serve(listener));
    flood(
        MockNetwork::remote(address).await.unwrap(),
        MockNetwork::remote(address).await.unwrap(),
    )
    .await;
    broker.abort();
}

#[cfg(feature = "mock-broker")]
#[tokio::test]
async fn broker_critical_flood_reports_lag() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let broker = tokio::spawn(MockNetwork::serve(listener));
    critical_flood(
        MockNetwork::remote(address).await.unwrap(),
        MockNetwork::remote(address).await.unwrap(),
    )
    .await;
    broker.abort();
}
