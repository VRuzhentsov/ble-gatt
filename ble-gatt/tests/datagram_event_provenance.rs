//! Raw consumption preserves lifecycle-versus-payload stream provenance.
use async_trait::async_trait;
use ble_gatt::{
    datagram::{self, DatagramConfig},
    drivers::mock::{MockDriver, MockNetwork},
    Adapter, BoxStream, CapabilityReport, CharacteristicUuid, DiscoveredPeer, Driver,
    GattConnection, GattEvent, GattServiceSpec, PeerAddress, Result, ServiceUuid,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

struct PayloadEvents {
    inner: Arc<MockDriver>,
    injected: Mutex<Option<mpsc::Receiver<GattEvent>>>,
}
#[async_trait]
impl Driver for PayloadEvents {
    async fn capabilities(&self) -> CapabilityReport {
        self.inner.capabilities().await
    }
    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.inner.scan(service).await
    }
    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        self.inner.connect(peer).await
    }
    async fn advertise(&self, spec: GattServiceSpec) -> Result<()> {
        self.inner.advertise(spec).await
    }
    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service()
    }
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .set_characteristic_value(characteristic, value)
            .await
    }
    async fn stop_advertising(&self) -> Result<()> {
        self.inner.stop_advertising().await
    }
    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.inner.notify(characteristic, value).await
    }
    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .notify_peer(peer, session, characteristic, value)
            .await
    }
    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        self.inner.disconnect_peer(peer, session).await
    }
    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.inner.lifecycle_events()
    }
    fn events(&self) -> BoxStream<GattEvent> {
        let payload = self
            .inner
            .events()
            .filter(|event| matches!(event, GattEvent::CharacteristicWritten { .. }));
        let injected = self
            .injected
            .lock()
            .unwrap()
            .take()
            .expect("one payload subscriber");
        Box::pin(payload.merge(ReceiverStream::new(injected)))
    }
}

#[tokio::test]
async fn payload_lag_is_recoverable_but_critical_lag_ends_the_raw_generation() {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let address = PeerAddress("provenance-peripheral".into());
    let inner = Arc::new(MockDriver::new(address.clone(), network.clone(), caps));
    let (inject, receiver) = mpsc::channel(1);
    let driver = Arc::new(PayloadEvents {
        inner,
        injected: Mutex::new(Some(receiver)),
    });
    let peripheral = Adapter::new(driver).await;
    let central = Adapter::new(Arc::new(MockDriver::new(
        PeerAddress("provenance-central".into()),
        network.clone(),
        caps,
    )))
    .await;
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::from_u128(31)),
        CharacteristicUuid(uuid::Uuid::from_u128(32)),
    );
    let mut incoming = datagram::serve(&peripheral, &config).await.unwrap();
    let mut raw = datagram::connect(&central, &address, &config)
        .await
        .unwrap();
    let mut served = tokio::time::timeout(Duration::from_millis(300), incoming.next())
        .await
        .expect("lifecycle-only Connected must admit central")
        .unwrap();
    inject.send(GattEvent::Lagged { dropped: 2 }).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(300), served.recv())
            .await
            .unwrap(),
        Some(Err(ble_gatt::BleError::ReceiveGap(_)))
    ));
    raw.send(vec![7]).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), served.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![7]
    );
    network.simulate_event_lag(&address, 3);
    tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            if served.send(vec![8]).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("critical lag must invalidate served generation");
}
