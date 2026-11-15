use ble_gatt::{
    drivers::mock::{MockDriver, MockNetwork},
    Adapter, CapabilityReport, Driver, PeerAddress,
};
use ble_gatt_cli_example::{central, peripheral, SERVICE};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

async fn device(network: &Arc<MockNetwork>, name: &str) -> (Adapter, Arc<MockDriver>) {
    let driver = Arc::new(MockDriver::new(
        PeerAddress(name.into()),
        network.clone(),
        CapabilityReport {
            central: true,
            peripheral: true,
        },
    ));
    (Adapter::new(driver.clone()).await, driver)
}

#[tokio::test]
async fn raw_roles_exchange_bytes_and_release_the_radio() {
    let network = MockNetwork::new();
    let (server, server_driver) = device(&network, "server").await;
    let (client, _) = device(&network, "client").await;
    let message = b"hello".to_vec();
    // Poll the server first. The mock scan reads a snapshot of advertisements.
    let (received, exchanged) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            peripheral(&server, WAIT),
            central(&client, message.clone(), WAIT)
        )
    })
    .await
    .expect("the example must finish");
    assert_eq!(received.expect("peripheral must serve"), message);
    let (initial, reply) = exchanged.expect("central must exchange bytes");
    assert_eq!(initial, b"ready");
    assert_eq!(reply, message);
    assert_eq!(
        network.disconnected_peers(),
        vec![PeerAddress("server".into())]
    );
    assert!(
        server_driver.served_service().is_none(),
        "stop must remove the server"
    );
}

#[tokio::test]
async fn idle_peripheral_times_out_and_stops_serving() {
    let network = MockNetwork::new();
    let (server, driver) = device(&network, "server").await;
    let error = peripheral(&server, Duration::from_millis(30))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(driver.served_service().is_none());
}

#[tokio::test]
async fn failed_exchange_disconnects_before_returning() {
    let network = MockNetwork::new();
    let (server, _) = device(&network, "server").await;
    let (client, _) = device(&network, "client").await;
    // Use a real mock GATT server without notifications to stall the exchange.
    let handle = server
        .peripheral()
        .serve(ble_gatt::GattServiceSpec::new(
            SERVICE,
            vec![ble_gatt::GattCharacteristicSpec {
                uuid: ble_gatt_cli_example::VALUE,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"ready".to_vec(),
            }],
        ))
        .await
        .unwrap();
    let error = central(&client, b"hello".to_vec(), Duration::from_millis(30))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("GATT exchange timed out"),
        "{error}"
    );
    assert_eq!(
        network.disconnected_peers(),
        vec![PeerAddress("server".into())]
    );
    handle.stop().await.unwrap();
}
