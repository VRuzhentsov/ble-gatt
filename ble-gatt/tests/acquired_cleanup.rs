use async_trait::async_trait;
use ble_gatt::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;
use tokio_stream::StreamExt;

#[derive(Default)]
struct Counts {
    started: AtomicUsize,
    active: AtomicUsize,
    completed: AtomicUsize,
    cancelled: AtomicUsize,
    acquired: AtomicUsize,
    reads: AtomicUsize,
    mode: AtomicUsize,
}
struct Cleanup {
    counts: Arc<Counts>,
    done: bool,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.counts.active.fetch_sub(1, SeqCst);
        if !self.done {
            self.counts.cancelled.fetch_add(1, SeqCst);
        }
    }
}
struct TestConnection {
    peer: PeerAddress,
    counts: Arc<Counts>,
    delay: Option<Duration>,
}
#[async_trait]
impl GattConnection for TestConnection {
    fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }
    fn att_mtu(&self) -> u16 {
        23
    }
    async fn read(&mut self, _: CharacteristicUuid) -> Result<Vec<u8>> {
        self.counts.reads.fetch_add(1, SeqCst);
        match self.counts.mode.load(SeqCst) {
            1 => return Ok(vec![255]),
            2 => return std::future::pending().await,
            3 | 4 => {
                let mut metadata = vec![b'B', b'G', 1, 1];
                metadata.extend_from_slice(&[7; 16]);
                let mut advertisement = advertisement::GattAdvertisement::default();
                advertisement
                    .service_data
                    .insert(ServiceUuid(uuid::Uuid::from_u128(11)), metadata);
                return Ok(advertisement.encode());
            }
            _ => {}
        }
        Err(BleError::Protocol(
            "controlled metadata-read failure".into(),
        ))
    }
    async fn write_with_type(
        &mut self,
        _: CharacteristicUuid,
        _: Vec<u8>,
        _: WriteType,
    ) -> Result<()> {
        unreachable!()
    }
    async fn subscribe(&mut self, _: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        if self.counts.mode.load(SeqCst) == 4 {
            std::future::pending().await
        } else {
            Err(BleError::Protocol("controlled subscribe failure".into()))
        }
    }
    async fn disconnect(&mut self) -> Result<()> {
        self.counts.started.fetch_add(1, SeqCst);
        self.counts.active.fetch_add(1, SeqCst);
        let mut cleanup = Cleanup {
            counts: self.counts.clone(),
            done: false,
        };
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        } else {
            std::future::pending::<()>().await;
        }
        self.counts.completed.fetch_add(1, SeqCst);
        cleanup.done = true;
        Ok(())
    }
}
struct TestDriver {
    counts: Arc<Counts>,
    delay: Option<Duration>,
}
#[async_trait]
impl Driver for TestDriver {
    async fn capabilities(&self) -> CapabilityReport {
        CapabilityReport {
            central: true,
            peripheral: true,
        }
    }
    async fn scan(&self, _: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        Ok(Box::pin(
            tokio_stream::iter([Ok(DiscoveredPeer {
                address: PeerAddress("probe".into()),
                name: None,
                services: vec![],
                manufacturer_data: Default::default(),
                service_data: Default::default(),
                rssi: None,
            })])
            .chain(tokio_stream::pending()),
        ))
    }
    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        self.counts.acquired.fetch_add(1, SeqCst);
        Ok(Box::new(TestConnection {
            peer: peer.clone(),
            counts: self.counts.clone(),
            delay: self.delay,
        }))
    }
    async fn advertise(&self, _: GattServiceSpec) -> Result<()> {
        unreachable!()
    }
    fn served_service(&self) -> Option<GattServiceSpec> {
        None
    }
    async fn set_characteristic_value(&self, _: CharacteristicUuid, _: Vec<u8>) -> Result<()> {
        unreachable!()
    }
    async fn stop_advertising(&self) -> Result<()> {
        Ok(())
    }
    async fn notify(&self, _: CharacteristicUuid, _: Vec<u8>) -> Result<()> {
        unreachable!()
    }
    async fn notify_peer(
        &self,
        _: &PeerAddress,
        _: Option<u64>,
        _: CharacteristicUuid,
        _: Vec<u8>,
    ) -> Result<()> {
        unreachable!()
    }
    async fn disconnect_peer(&self, _: &PeerAddress, _: Option<u64>) -> Result<()> {
        unreachable!()
    }
    fn events(&self) -> BoxStream<GattEvent> {
        Box::pin(tokio_stream::pending())
    }
}
fn link(counts: Arc<Counts>, delay: Option<Duration>) -> Arc<PeerLink> {
    link_limit(counts, delay, 1)
}
fn link_limit(counts: Arc<Counts>, delay: Option<Duration>, limit: usize) -> Arc<PeerLink> {
    let mut config = PeerLinkConfig::new(datagram::DatagramConfig::new(
        ServiceUuid(uuid::Uuid::from_u128(11)),
        CharacteristicUuid(uuid::Uuid::from_u128(12)),
    ));
    config.max_links = MaxLinks(limit);
    PeerLink::with_driver(Arc::new(TestDriver { counts, delay }), config)
}

#[tokio::test]
async fn distinct_failed_acquisitions_cannot_escape_cleanup_admission() {
    let counts = Arc::new(Counts::default());
    let owner = link(counts.clone(), None);
    for n in 0..80 {
        assert!(owner
            .request(&PeerAddress(format!("peer-{n}")))
            .await
            .is_err());
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        counts.started.load(SeqCst),
        1,
        "MaxLinks(1) must retain the acquired cleanup reservation"
    );
    assert_eq!(counts.active.load(SeqCst), 1);
    assert!(
        owner.close().await.is_err(),
        "hung cleanup cannot produce successful close"
    );
    drop(owner);
    assert_eq!(counts.active.load(SeqCst), 0);
}

async fn wait_count(counter: &AtomicUsize, value: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while counter.load(SeqCst) < value {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_cancelled_probe_reserves_its_address_until_cleanup_finishes() {
    let counts = Arc::new(Counts::default());
    counts.mode.store(2, SeqCst);
    let owner = link_limit(counts.clone(), Some(Duration::from_millis(150)), 2);
    let stream = owner.discover().await.unwrap();
    // The peer advertises no radio record: it is read after a 1 s grace for
    // its scan response.
    tokio::time::sleep(Duration::from_secs(1)).await;
    wait_count(&counts.reads, 1).await;
    drop(stream);
    wait_count(&counts.started, 1).await;
    let next = owner.discover().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        counts.acquired.load(SeqCst),
        1,
        "a second discovery must not replace a cleaning probe session"
    );
    drop(next);
    owner.close().await.unwrap();
    assert_eq!(counts.completed.load(SeqCst), 1);
}

#[tokio::test]
async fn finite_acquired_cleanup_finishes_before_successful_close_or_drop() {
    for explicit in [true, false] {
        let counts = Arc::new(Counts::default());
        let owner = link(counts.clone(), Some(Duration::from_millis(150)));
        assert!(owner.request(&PeerAddress("finite".into())).await.is_err());
        if explicit {
            owner.close().await.unwrap();
        }
        drop(owner);
        assert_eq!(counts.started.load(SeqCst), 1);
        assert_eq!(
            counts.completed.load(SeqCst),
            1,
            "finite cleanup must finish, not be aborted with the runtime"
        );
        assert_eq!(counts.cancelled.load(SeqCst), 0);
        assert_eq!(counts.active.load(SeqCst), 0);
    }
}

#[tokio::test]
async fn decode_and_raw_pending_setup_failures_drain_the_same_owned_cleanup() {
    for mode in [1, 3] {
        let counts = Arc::new(Counts::default());
        counts.mode.store(mode, SeqCst);
        let owner = link(counts.clone(), Some(Duration::from_millis(150)));
        assert!(owner.request(&PeerAddress("setup".into())).await.is_err());
        assert!(owner.request(&PeerAddress("setup".into())).await.is_err());
        assert_eq!(counts.acquired.load(SeqCst), 1);
        owner.close().await.unwrap();
        drop(owner);
        assert_eq!(counts.completed.load(SeqCst), 1);
        assert_eq!(counts.cancelled.load(SeqCst), 0);
    }
}

#[tokio::test]
async fn cancelled_metadata_and_raw_subscribe_acquisitions_are_drained_on_drop() {
    for mode in [2, 4] {
        let counts = Arc::new(Counts::default());
        counts.mode.store(mode, SeqCst);
        let owner = link(counts.clone(), Some(Duration::from_millis(150)));
        assert!(tokio::time::timeout(
            Duration::from_millis(30),
            owner.request(&PeerAddress("cancelled".into()))
        )
        .await
        .is_err());
        drop(owner);
        assert_eq!(counts.completed.load(SeqCst), 1);
        assert_eq!(counts.cancelled.load(SeqCst), 0);
    }
}

#[tokio::test]
async fn a_hung_cleanup_preserves_unrelated_peer_and_control_progress() {
    let counts = Arc::new(Counts::default());
    let owner = link_limit(counts.clone(), None, 2);
    assert!(owner.request(&PeerAddress("hung".into())).await.is_err());
    wait_count(&counts.started, 1).await;
    tokio::time::timeout(Duration::from_millis(300), async {
        // The same worker must still dispatch discovery and an unrelated dial.
        assert!(matches!(
            owner.request(&PeerAddress("other".into())).await,
            Err(BleError::Protocol(_))
        ));
        let stream = owner.discover().await.unwrap();
        drop(stream);
    })
    .await
    .unwrap();
    assert_eq!(counts.acquired.load(SeqCst), 2);
    let started = std::time::Instant::now();
    assert!(owner.close().await.is_err());
    assert!(started.elapsed() < Duration::from_secs(6));
    drop(owner);
    assert_eq!(counts.active.load(SeqCst), 0);
}

#[tokio::test]
async fn completed_old_cleanup_cannot_disconnect_a_replacement_acquisition() {
    let counts = Arc::new(Counts::default());
    let owner = link(counts.clone(), Some(Duration::from_millis(150)));
    let peer = PeerAddress("reused".into());
    assert!(owner.request(&peer).await.is_err());
    assert!(owner.request(&peer).await.is_err());
    assert_eq!(counts.acquired.load(SeqCst), 1);
    wait_count(&counts.completed, 1).await;
    // A completed cleanup is dispatched before releasing admission; retry only
    // the public busy result, not a failed acquired connection.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match owner.request(&peer).await {
                Err(BleError::AdapterBusy(_)) => tokio::time::sleep(Duration::from_millis(1)).await,
                Err(BleError::Protocol(_)) => break,
                _ => panic!("unexpected replacement outcome"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.acquired.load(SeqCst), 2);
    assert_eq!(counts.started.load(SeqCst), 2);
    assert_eq!(counts.completed.load(SeqCst), 1);
    owner.close().await.unwrap();
    assert_eq!(counts.completed.load(SeqCst), 2);
    assert_eq!(counts.cancelled.load(SeqCst), 0);
}
