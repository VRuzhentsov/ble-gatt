//! The role objects (ADR-0007 D4–D7) on the mock radio: published adapter
//! and connection state, and handles that stop what they started.

use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::{
    Adapter, Backend, CapabilityReport, CharacteristicUuid, ConnectionState, GattCharacteristicSpec,
    GattServiceSpec, PeerAddress, RadioStatus, ServiceUuid,
};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(2);

fn capabilities() -> CapabilityReport {
    CapabilityReport {
        central: true,
        peripheral: true,
    }
}

fn service() -> GattServiceSpec {
    GattServiceSpec::new(
        ServiceUuid(Uuid::new_v4()),
        vec![GattCharacteristicSpec {
            uuid: CharacteristicUuid(Uuid::new_v4()),
            readable: true,
            writable: true,
            notifiable: true,
            initial_value: b"hi".to_vec(),
        }],
    )
}

fn device(network: &Arc<MockNetwork>, name: &str) -> Arc<MockBackend> {
    Arc::new(MockBackend::new(PeerAddress(name.into()), network.clone(), capabilities()))
}

#[tokio::test]
async fn adapter_publishes_the_radio_status() {
    let network = MockNetwork::new();
    let backend = device(&network, "a");
    let adapter = Adapter::new(backend.clone() as Arc<dyn Backend>).await;
    let mut status = adapter.status();
    assert_eq!(*status.borrow(), RadioStatus::On);

    backend.simulate_radio(RadioStatus::Off);
    timeout(WAIT, status.wait_for(|s| *s == RadioStatus::Off)).await.unwrap().unwrap();

    backend.simulate_radio(RadioStatus::On);
    timeout(WAIT, adapter.wait_available()).await.unwrap();
}

#[tokio::test]
async fn a_connection_publishes_disconnected_when_the_peer_goes_away() {
    let network = MockNetwork::new();
    let peripheral_backend = device(&network, "peripheral");
    let central_backend = device(&network, "central");
    let peripheral = Adapter::new(peripheral_backend.clone() as Arc<dyn Backend>).await.peripheral();
    let spec = service();
    let characteristic = spec.characteristics[0].uuid;
    let _server = peripheral.serve(spec).await.unwrap();

    let central = Adapter::new(central_backend.clone() as Arc<dyn Backend>).await.central();
    let mut connection = central.connect(&PeerAddress("peripheral".into())).await.unwrap();
    assert_eq!(connection.read(characteristic).await.unwrap(), b"hi");

    let mut state = connection.state();
    let token = connection.cancelled();
    assert_eq!(*state.borrow(), ConnectionState::Connected);

    // The mock reports a lost link from the server's side.
    peripheral_backend.simulate_peer_loss(&PeerAddress("central".into()));
    timeout(WAIT, state.wait_for(|s| s.is_disconnected())).await.unwrap().unwrap();
    timeout(WAIT, token.cancelled()).await.unwrap();
}

#[tokio::test]
async fn dropping_a_connection_cancels_its_token() {
    let network = MockNetwork::new();
    let peripheral_backend = device(&network, "peripheral");
    let central_backend = device(&network, "central");
    let _server = Adapter::new(peripheral_backend as Arc<dyn Backend>)
        .await
        .peripheral()
        .serve(service())
        .await
        .unwrap();
    let connection = Adapter::new(central_backend as Arc<dyn Backend>)
        .await
        .central()
        .connect(&PeerAddress("peripheral".into()))
        .await
        .unwrap();

    let token = connection.cancelled();
    let task = tokio::spawn(token.clone().run_until_cancelled_owned(std::future::pending::<()>()));
    drop(connection);
    assert_eq!(timeout(WAIT, task).await.unwrap().unwrap(), None);
}

#[tokio::test]
async fn dropping_the_server_handle_stops_advertising() {
    let network = MockNetwork::new();
    let peripheral_backend = device(&network, "peripheral");
    let central_backend = device(&network, "central");
    let spec = service();
    let service_uuid = spec.uuid;
    let server = Adapter::new(peripheral_backend as Arc<dyn Backend>)
        .await
        .peripheral()
        .serve(spec)
        .await
        .unwrap();
    let central = Adapter::new(central_backend as Arc<dyn Backend>).await.central();

    let mut scan = central.scan(service_uuid).await.unwrap();
    let found = timeout(WAIT, scan.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(found.address, PeerAddress("peripheral".into()));
    drop(scan);

    drop(server);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut scan = central.scan(service_uuid).await.unwrap();
    assert!(
        !matches!(timeout(Duration::from_millis(300), scan.next()).await, Ok(Some(_))),
        "the peripheral is still advertising after its handle was dropped"
    );
}
