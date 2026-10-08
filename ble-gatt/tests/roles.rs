//! The role objects (ADR-0007 D4–D7) on the mock radio: published adapter
//! and connection state, and handles that stop what they started.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::{
    Adapter, Backend, BoxStream, CapabilityReport, CharacteristicUuid, ConnectionState, DiscoveredPeer,
    GattCharacteristicSpec, GattConnection, GattEvent, GattServiceSpec, PeerAddress, RadioStatus,
    ServiceUuid,
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
async fn dropping_a_connection_disconnects_it() {
    let network = MockNetwork::new();
    let _server = Adapter::new(device(&network, "peripheral") as Arc<dyn Backend>)
        .await
        .peripheral()
        .serve(service())
        .await
        .unwrap();
    let connection = Adapter::new(device(&network, "central") as Arc<dyn Backend>)
        .await
        .central()
        .connect(&PeerAddress("peripheral".into()))
        .await
        .unwrap();

    drop(connection);
    timeout(WAIT, async {
        while network.disconnected_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a dropped connection disconnects its driver");
    assert_eq!(network.disconnected_peers(), vec![PeerAddress("peripheral".into())]);
}

#[tokio::test]
async fn an_explicit_disconnect_is_not_repeated_on_drop() {
    let network = MockNetwork::new();
    let _server = Adapter::new(device(&network, "peripheral") as Arc<dyn Backend>)
        .await
        .peripheral()
        .serve(service())
        .await
        .unwrap();
    let mut connection = Adapter::new(device(&network, "central") as Arc<dyn Backend>)
        .await
        .central()
        .connect(&PeerAddress("peripheral".into()))
        .await
        .unwrap();

    connection.disconnect().await.unwrap();
    drop(connection);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(network.disconnected_peers().len(), 1);
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

/// Counts `advertise` calls while delegating everything else, so a test can
/// observe `Peripheral::serve`'s radio-recovery behavior (re-registering on
/// `RadioChanged::On`) without the mock backend needing to model BlueZ
/// forgetting its GATT app on a radio loss, which it doesn't.
struct CountingBackend {
    inner: Arc<dyn Backend>,
    advertise_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Backend for CountingBackend {
    async fn capabilities(&self) -> CapabilityReport {
        self.inner.capabilities().await
    }

    async fn scan(&self, service: ServiceUuid) -> ble_gatt::Result<BoxStream<ble_gatt::Result<DiscoveredPeer>>> {
        self.inner.scan(service).await
    }

    async fn connect(&self, peer: &PeerAddress) -> ble_gatt::Result<Box<dyn GattConnection>> {
        self.inner.connect(peer).await
    }

    async fn advertise(&self, service: GattServiceSpec) -> ble_gatt::Result<()> {
        self.advertise_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.advertise(service).await
    }

    async fn stop_advertising(&self) -> ble_gatt::Result<()> {
        self.inner.stop_advertising().await
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> ble_gatt::Result<()> {
        self.inner.notify(characteristic, value).await
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> ble_gatt::Result<()> {
        self.inner.notify_peer(peer, session, characteristic, value).await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> ble_gatt::Result<()> {
        self.inner.disconnect_peer(peer, session).await
    }

    fn events(&self) -> BoxStream<GattEvent> {
        self.inner.events()
    }

    async fn radio_status(&self) -> RadioStatus {
        self.inner.radio_status().await
    }
}

#[tokio::test]
async fn a_radio_recovery_re_registers_the_service() {
    let network = MockNetwork::new();
    let peripheral_backend = device(&network, "peripheral");
    let advertise_calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingBackend {
        inner: peripheral_backend.clone() as Arc<dyn Backend>,
        advertise_calls: advertise_calls.clone(),
    });

    let _server = Adapter::new(counting as Arc<dyn Backend>).await.peripheral().serve(service()).await.unwrap();
    assert_eq!(advertise_calls.load(Ordering::SeqCst), 1, "serve's own initial advertise");

    peripheral_backend.simulate_radio(RadioStatus::Off);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        advertise_calls.load(Ordering::SeqCst),
        1,
        "a radio loss alone must not trigger a re-advertise"
    );

    peripheral_backend.simulate_radio(RadioStatus::On);
    timeout(WAIT, async {
        while advertise_calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a radio recovery re-registers the service");
}
