use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{
    CapabilityReport, CharacteristicUuid, Driver, GattCharacteristicSpec, GattServiceSpec,
    PeerAddress, ServiceUuid,
};
use std::sync::Arc;
use tokio_stream::StreamExt;
use uuid::Uuid;

async fn preserves_sessions(network: Arc<MockNetwork>) {
    let capabilities = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let peripheral = MockDriver::new(
        PeerAddress("visibility-server".into()),
        network.clone(),
        capabilities,
    );
    let central = MockDriver::new(
        PeerAddress("visibility-client".into()),
        network,
        capabilities,
    );
    let service = ServiceUuid(Uuid::new_v4());
    let characteristic = CharacteristicUuid(Uuid::new_v4());
    let spec = GattServiceSpec::new(
        service,
        vec![GattCharacteristicSpec {
            uuid: characteristic,
            readable: true,
            writable: true,
            notifiable: true,
            initial_value: vec![1],
        }],
    );
    assert!(peripheral.set_advertising_enabled(true).await.is_err());
    peripheral.advertise(spec).await.unwrap();
    let mut connection = central
        .connect(&PeerAddress("visibility-server".into()))
        .await
        .unwrap();
    let session = connection.session();
    let mut notifications = connection.subscribe(characteristic).await.unwrap();
    peripheral.set_advertising_enabled(false).await.unwrap();
    peripheral.set_advertising_enabled(false).await.unwrap();
    assert!(central.scan(service).await.unwrap().next().await.is_none());
    assert!(peripheral.served_service().is_some());
    let mut updated = peripheral.served_service().unwrap();
    updated.local_name = Some("updated while hidden".into());
    peripheral.update_advertisement(updated).await.unwrap();
    assert!(central.scan(service).await.unwrap().next().await.is_none());
    connection.write(characteristic, vec![2]).await.unwrap();
    assert_eq!(connection.read(characteristic).await.unwrap(), vec![2]);
    peripheral
        .notify_peer(
            &PeerAddress("visibility-client".into()),
            session,
            characteristic,
            vec![3],
        )
        .await
        .unwrap();
    assert_eq!(notifications.next().await.unwrap().unwrap(), vec![3]);
    peripheral.set_advertising_enabled(true).await.unwrap();
    assert!(central.scan(service).await.unwrap().next().await.is_some());
    assert_eq!(connection.session(), session);
    peripheral.notify(characteristic, vec![4]).await.unwrap();
    assert_eq!(notifications.next().await.unwrap().unwrap(), vec![4]);
    peripheral.stop_advertising().await.unwrap();
    assert!(peripheral.served_service().is_none());
    assert!(connection.read(characteristic).await.is_err());
}

#[tokio::test]
async fn local_visibility_preserves_sessions() {
    preserves_sessions(MockNetwork::new()).await;
}

#[cfg(feature = "mock-broker")]
#[tokio::test]
async fn broker_visibility_preserves_sessions() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let broker = tokio::spawn(MockNetwork::serve(listener));
    preserves_sessions(MockNetwork::remote(address).await.unwrap()).await;
    broker.abort();
}
