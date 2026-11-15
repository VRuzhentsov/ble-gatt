//! The GATT advertisement (ADR-0012) over the mock radio: a peer whose
//! radio advertisement carries no optional data
//! is completed from its GATT advertisement; a peer that advertises its
//! data is not read at all.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ble_gatt::hal::{BoxStream, GattConnection};
use ble_gatt::{BleError, DiscoveredPeer, GattEvent, GattServiceSpec, WriteType};
use tokio::sync::Notify;

use ble_gatt::advertisement::{self, GattAdvertisementPolicy, GATT_ADVERTISEMENT_CHARACTERISTIC};
use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{Adapter, CapabilityReport, CharacteristicUuid, Driver, PeerAddress, ServiceUuid};
use tokio_stream::StreamExt;
use uuid::Uuid;

const COMPANY: u16 = 0xFFFF;

fn both_roles() -> CapabilityReport {
    CapabilityReport { central: true, peripheral: true }
}

fn config(flag: u8) -> DatagramConfig {
    let mut config = DatagramConfig::new(
        ServiceUuid(Uuid::from_u128(0xb1e6a000_f101_4000_8000_00805f9b34fb)),
        CharacteristicUuid(Uuid::from_u128(0xb1e6a001_f101_4000_8000_00805f9b34fb)),
    );
    config.advertised_manufacturer_data.insert(COMPANY, vec![flag, 0xAA, 0xBB, 0xCC, 0xDD]);
    config.advertised_local_name = Some("Desktop".into());
    config
}

fn driver(network: &Arc<MockNetwork>, name: &str) -> Arc<MockDriver> {
    Arc::new(MockDriver::new(PeerAddress(name.into()), network.clone(), both_roles()))
}

#[tokio::test]
async fn a_peer_without_advertised_data_is_completed_from_its_gatt_advertisement() {
    let network = MockNetwork::new();
    let windows = driver(&network, "windows");
    let phone = driver(&network, "phone");
    let config = config(0x01);

    // Advertise only the service UUID. Serve the full data through GATT.
    let mut spec = config.service_spec();
    spec.manufacturer_data.clear();
    spec.local_name = None;
    windows.advertise(spec).await.unwrap();

    let scan = phone.scan(config.service).await.unwrap();
    let mut peers =
        advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    let peer = peers.next().await.expect("a peer").expect("no scan error");
    assert_eq!(peer.address, PeerAddress("windows".into()));
    assert_eq!(peer.manufacturer_data.get(&COMPANY), Some(&vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]));
    assert_eq!(peer.name.as_deref(), Some("Desktop"));
    assert!(peers.next().await.is_none(), "the scan ended and the peer was read once");
}

#[tokio::test]
async fn a_peer_that_advertises_its_data_is_not_read() {
    let network = MockNetwork::new();
    let linux = driver(&network, "linux");
    let phone = driver(&network, "phone");
    let config = config(0x00);
    // Served without the GATT advertisement characteristic: a read would fail.
    let mut spec = config.service_spec();
    spec.characteristics.retain(|c| c.uuid != GATT_ADVERTISEMENT_CHARACTERISTIC);
    linux.advertise(spec).await.unwrap();

    let scan = phone.scan(config.service).await.unwrap();
    let mut peers =
        advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    let peer = peers.next().await.expect("a peer").expect("no scan error");
    assert_eq!(peer.manufacturer_data.get(&COMPANY).map(|v| v[0]), Some(0x00));
}

#[tokio::test]
async fn an_advertisement_update_changes_the_gatt_advertisement() {
    let network = MockNetwork::new();
    let windows = driver(&network, "windows");
    let phone = driver(&network, "phone");
    let adapter = Adapter::new(windows.clone()).await;

    let _channels = datagram::serve(&adapter, &config(0x00)).await.unwrap();
    let before = advertisement::read(phone.as_ref(), &PeerAddress("windows".into())).await.unwrap();
    assert_eq!(before.manufacturer_data.get(&COMPANY).map(|v| v[0]), Some(0x00));

    datagram::update_advertisement(&adapter, &config(0x01)).await.unwrap();
    let after = advertisement::read(phone.as_ref(), &PeerAddress("windows".into())).await.unwrap();
    assert_eq!(
        after.manufacturer_data.get(&COMPANY).map(|v| v[0]),
        Some(0x01),
        "the Add Device flag changed"
    );
    assert_eq!(after.local_name.as_deref(), Some("Desktop"));
}

/// Dropping the completed stream must end the scan it wraps at once. The
/// Linux driver stops BlueZ discovery only when its scan stream is dropped,
/// so a forwarding task that waits for the next advertisement first keeps
/// discovery on until some peer happens to advertise.
#[tokio::test]
async fn dropping_the_completed_stream_ends_the_scan_at_once() {
    let network = MockNetwork::new();
    let phone = driver(&network, "phone");
    let config = config(0x01);

    // A scan that never reports a peer, and that says when it is dropped.
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let scan = Box::pin(futures::stream::poll_fn(move |_| {
        let _held_until_drop = &dropped_tx;
        std::task::Poll::Pending
    }));

    let peers =
        advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    // Let the forwarding task reach its wait for the next advertisement.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(peers);

    let ended = tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx).await;
    assert!(ended.is_ok(), "the scan was still held after the completed stream was dropped");
}

#[derive(Default)]
struct ReadState {
    reads: AtomicUsize,
    started: Notify,
    disconnected: Notify,
}

struct ReadDriver {
    state: Arc<ReadState>,
    blocked: bool,
}

struct ReadConnection {
    state: Arc<ReadState>,
    blocked: bool,
    peer: PeerAddress,
}

#[async_trait]
impl GattConnection for ReadConnection {
    fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }
    fn att_mtu(&self) -> u16 {
        23
    }
    async fn read(&mut self, _: CharacteristicUuid) -> ble_gatt::Result<Vec<u8>> {
        self.state.reads.fetch_add(1, Ordering::SeqCst);
        self.state.started.notify_one();
        if self.blocked {
            std::future::pending::<()>().await;
        }
        Err(BleError::Gatt("read failed".into()))
    }
    async fn disconnect(&mut self) -> ble_gatt::Result<()> {
        self.state.disconnected.notify_one();
        Ok(())
    }
    async fn write_with_type(
        &mut self,
        _: CharacteristicUuid,
        _: Vec<u8>,
        _: WriteType,
    ) -> ble_gatt::Result<()> {
        unreachable!()
    }
    async fn subscribe(
        &mut self,
        _: CharacteristicUuid,
    ) -> ble_gatt::Result<BoxStream<ble_gatt::Result<Vec<u8>>>> {
        unreachable!()
    }
}

#[async_trait]
impl Driver for ReadDriver {
    async fn capabilities(&self) -> CapabilityReport {
        both_roles()
    }
    async fn connect(&self, peer: &PeerAddress) -> ble_gatt::Result<Box<dyn GattConnection>> {
        Ok(Box::new(ReadConnection { state: self.state.clone(), blocked: self.blocked, peer: peer.clone() }))
    }
    fn events(&self) -> BoxStream<GattEvent> {
        Box::pin(tokio_stream::pending())
    }
    async fn scan(&self, _: ServiceUuid) -> ble_gatt::Result<BoxStream<ble_gatt::Result<DiscoveredPeer>>> {
        unreachable!()
    }
    async fn advertise(&self, _: GattServiceSpec) -> ble_gatt::Result<()> {
        unreachable!()
    }
    fn served_service(&self) -> Option<GattServiceSpec> {
        None
    }
    async fn set_characteristic_value(&self, _: CharacteristicUuid, _: Vec<u8>) -> ble_gatt::Result<()> {
        unreachable!()
    }
    async fn stop_advertising(&self) -> ble_gatt::Result<()> {
        unreachable!()
    }
    async fn notify(&self, _: CharacteristicUuid, _: Vec<u8>) -> ble_gatt::Result<()> {
        unreachable!()
    }
    async fn notify_peer(
        &self,
        _: &PeerAddress,
        _: Option<u64>,
        _: CharacteristicUuid,
        _: Vec<u8>,
    ) -> ble_gatt::Result<()> {
        unreachable!()
    }
    async fn disconnect_peer(&self, _: &PeerAddress, _: Option<u64>) -> ble_gatt::Result<()> {
        unreachable!()
    }
}

fn empty_peer() -> DiscoveredPeer {
    DiscoveredPeer {
        address: PeerAddress("peer".into()),
        name: None,
        services: vec![config(0).service],
        manufacturer_data: Default::default(),
        service_data: Default::default(),
        rssi: None,
    }
}

#[tokio::test]
async fn a_read_timeout_disconnects_the_acquired_connection() {
    let state = Arc::new(ReadState::default());
    let driver = Arc::new(ReadDriver { state: state.clone(), blocked: true });
    let scan = Box::pin(tokio_stream::iter([Ok(empty_peer())]));
    let policy = GattAdvertisementPolicy { timeout: Duration::from_millis(20), ..Default::default() };
    let mut peers = advertisement::complete(driver, config(0).service, scan, policy);
    assert!(peers.next().await.unwrap().is_ok());
    tokio::time::timeout(Duration::from_secs(1), state.disconnected.notified())
        .await
        .expect("the timed out read must disconnect");
    assert!(peers.next().await.is_none());
}

#[tokio::test]
async fn dropping_output_during_a_read_releases_the_scan_and_connection() {
    let state = Arc::new(ReadState::default());
    let driver = Arc::new(ReadDriver { state: state.clone(), blocked: true });
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let mut first = Some(Ok(empty_peer()));
    let scan = Box::pin(futures::stream::poll_fn(move |_| {
        let _held_until_drop = &dropped_tx;
        first.take().map_or(std::task::Poll::Pending, |peer| std::task::Poll::Ready(Some(peer)))
    }));
    let peers = advertisement::complete(driver, config(0).service, scan, GattAdvertisementPolicy::default());
    tokio::time::timeout(Duration::from_secs(1), state.started.notified()).await.unwrap();
    drop(peers);
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .expect("dropping output must stop the scan during a read")
        .unwrap_err();
    tokio::time::timeout(Duration::from_secs(1), state.disconnected.notified())
        .await
        .expect("the cancelled read must disconnect");
}

#[tokio::test]
async fn later_sightings_do_not_reset_the_read_failure_limit() {
    let state = Arc::new(ReadState::default());
    let driver = Arc::new(ReadDriver { state: state.clone(), blocked: false });
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let scan = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    let policy = GattAdvertisementPolicy {
        max_failures: 1,
        max_peers: 1,
        refresh: Duration::from_millis(1),
        ..Default::default()
    };
    let mut peers = advertisement::complete(driver, config(0).service, scan, policy);
    tx.send(Ok(empty_peer())).await.unwrap();
    assert!(peers.next().await.unwrap().is_ok());
    for _ in 0..3 {
        tx.send(Ok(empty_peer())).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(state.reads.load(Ordering::SeqCst), 1, "the failure limit applies to the whole scan");
    let mut extra = empty_peer();
    extra.address = PeerAddress("extra".into());
    tx.send(Ok(extra.clone())).await.unwrap();
    assert_eq!(
        peers.next().await.unwrap().unwrap().address,
        extra.address,
        "a full tracking table passes other peers through"
    );
    drop(tx);
    assert!(peers.next().await.is_none(), "stopped peers must not prevent drainage");
}

#[tokio::test]
async fn refresh_reports_changed_data_once_and_drains_after_the_scan() {
    let network = MockNetwork::new();
    let peripheral = driver(&network, "peer");
    let central = driver(&network, "phone");
    let mut spec = config(0).service_spec();
    spec.manufacturer_data.clear();
    peripheral.advertise(spec).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let scan = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    let policy = GattAdvertisementPolicy { refresh: Duration::from_millis(5), ..Default::default() };
    let mut peers = advertisement::complete(central, config(0).service, scan, policy);
    tx.send(Ok(empty_peer())).await.unwrap();
    let first = peers.next().await.unwrap().unwrap();
    assert_eq!(first.manufacturer_data[&COMPANY][0], 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peers.next()).await.is_err(),
        "unchanged refreshes must not be reported"
    );
    peripheral
        .set_characteristic_value(
            GATT_ADVERTISEMENT_CHARACTERISTIC,
            advertisement::GattAdvertisement::of(&config(1).service_spec()).encode(),
        )
        .await
        .unwrap();
    let changed = tokio::time::timeout(Duration::from_secs(1), peers.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(changed.manufacturer_data[&COMPANY][0], 1);
    drop(tx);
    assert!(peers.next().await.is_none());
}
