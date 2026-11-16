//! Executable managed lifecycle regressions over the unchanged raw datagram API.
use super::managed_codec::{self, Record};
use crate::{
    datagram::{DatagramChannel, DatagramConfig},
    drivers::mock::{MockDriver, MockNetwork},
    CapabilityReport, CharacteristicUuid, ConnectionRequest, Driver, EndpointId, PeerAddress,
    PeerChannel, PeerChannelCloseReason, PeerLink, PeerLinkConfig, ServiceUuid,
};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt;

fn config() -> DatagramConfig {
    DatagramConfig::new(
        ServiceUuid(uuid::Uuid::from_u128(11)),
        CharacteristicUuid(uuid::Uuid::from_u128(12)),
    )
}
async fn raw_request(
    config: DatagramConfig,
) -> (Arc<PeerLink>, DatagramChannel, ConnectionRequest) {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let raw_driver = Arc::new(MockDriver::new(
        PeerAddress("raw".into()),
        network.clone(),
        caps,
    ));
    let managed = PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress("managed".into()),
            network,
            caps,
        )),
        PeerLinkConfig::new(config.clone()),
    );
    let mut incoming = managed.incoming().unwrap();
    managed.set_advertising(true).await.unwrap();
    let mut raw =
        DatagramChannel::connect_driver(raw_driver, &PeerAddress("managed".into()), &config)
            .await
            .unwrap();
    raw.send(
        Record::Request {
            sender: EndpointId::from_bytes([1; 16]),
            target: managed.endpoint_id(),
            attempt: 71,
            nonce: -8,
        }
        .encode(),
    )
    .await
    .unwrap();
    assert!(matches!(
        Record::decode(&raw.recv().await.unwrap().unwrap(), config.max_message_len).unwrap(),
        Record::Response { attempt: 71, .. }
    ));
    let request = incoming.next().await.unwrap();
    (managed, raw, request)
}
async fn raw_established(config: DatagramConfig) -> (Arc<PeerLink>, DatagramChannel, PeerChannel) {
    let (managed, mut raw, request) = raw_request(config).await;
    raw.send(
        Record::Decision {
            attempt: 71,
            action: 0,
        }
        .encode(),
    )
    .await
    .unwrap();
    let channel = request.accept().await.unwrap();
    assert_eq!(
        Record::decode(&raw.recv().await.unwrap().unwrap(), 128).unwrap(),
        Record::Decision {
            attempt: 71,
            action: 0
        }
    );
    (managed, raw, channel)
}
fn mock_link(network: &Arc<MockNetwork>, address: &str) -> Arc<PeerLink> {
    let mut cfg = config();
    cfg.inbound_queue_depth = 1;
    PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress(address.into()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        PeerLinkConfig::new(cfg),
    )
}
async fn connect_links(
    local: &PeerLink,
    remote: &PeerLink,
    address: &str,
) -> (PeerChannel, PeerChannel) {
    let mut incoming = remote.incoming().unwrap();
    remote.set_advertising(true).await.unwrap();
    let outgoing = local.request(&PeerAddress(address.into())).await.unwrap();
    let request = incoming.next().await.unwrap();
    let (left, right) = tokio::join!(outgoing.accept(), request.accept());
    (left.unwrap(), right.unwrap())
}

#[tokio::test]
async fn overloaded_peer_does_not_block_other_peer_and_gap_wakes_shared_receive() {
    let network = MockNetwork::new();
    let a = mock_link(&network, "gap-a");
    let b = mock_link(&network, "gap-b");
    let c = mock_link(&network, "gap-c");
    let (ab, ba) = connect_links(&a, &b, "gap-b").await;
    let (ac, ca) = connect_links(&a, &c, "gap-c").await;
    ba.send(vec![1]).await.unwrap();
    ba.send(vec![2]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    ca.send(vec![99]).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), ac.recv())
            .await
            .expect("unrelated peer must progress")
            .unwrap()
            .unwrap(),
        vec![99]
    );
    assert_eq!(ab.state.received.lock().await.len(), 1);
    let shared = ab.clone();
    assert_eq!(shared.recv().await.unwrap().unwrap(), vec![1]);
    assert_eq!(ab.recv().await.unwrap().unwrap(), vec![2]);
    let parked = tokio::spawn(async move { shared.recv().await });
    network.simulate_notification_gap(&PeerAddress("gap-b".into()), &PeerAddress("gap-a".into()));
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(300), parked)
            .await
            .expect("known wire gap must wake parked application receive")
            .unwrap(),
        Some(Err(crate::BleError::ReceiveGap(_)))
    ));
    assert_eq!(ab.close_reason(), None);
    ba.send(vec![9]).await.unwrap();
    assert_eq!(ab.recv().await.unwrap().unwrap(), vec![9]);
    ab.send(vec![10]).await.unwrap();
    assert_eq!(ba.recv().await.unwrap().unwrap(), vec![10]);
    network.simulate_event_lag(&PeerAddress("gap-b".into()), 2);
    tokio::time::timeout(Duration::from_secs(1), ba.state.ended.cancelled())
        .await
        .unwrap();
    assert!(ba.recv().await.is_none());
    assert_eq!(ba.close_reason(), Some(PeerChannelCloseReason::LinkLost));
    a.close().await.unwrap();
    b.close().await.unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn stalled_peer_keeps_other_peer_and_control_live_and_can_be_cancelled() {
    let network = MockNetwork::new();
    let a = mock_link(&network, "stalled-a");
    let b = mock_link(&network, "stalled-b");
    let c = mock_link(&network, "stalled-c");
    a.set_advertising(true).await.unwrap();
    let (ab, ba) = connect_links(&a, &b, "stalled-b").await;
    let (ac, ca) = connect_links(&a, &c, "stalled-c").await;
    for byte in 0..3 {
        ba.send(vec![byte]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ab.state.received.lock().await.len(), 1);
    tokio::time::timeout(Duration::from_millis(300), async {
        a.set_advertising(false).await.unwrap();
        ca.send(vec![99]).await.unwrap();
        assert_eq!(ac.recv().await.unwrap().unwrap(), vec![99]);
        ac.send(vec![100]).await.unwrap();
        assert_eq!(ca.recv().await.unwrap().unwrap(), vec![100]);
        ab.disconnect().await.unwrap();
    })
    .await
    .expect("stalled admission must not hold control, another peer, or cancellation");
    assert!(ab.recv().await.is_none());
    assert_eq!(
        ab.close_reason(),
        Some(PeerChannelCloseReason::LocalDisconnect)
    );
    ca.send(vec![101]).await.unwrap();
    assert_eq!(ac.recv().await.unwrap().unwrap(), vec![101]);
    a.close().await.unwrap();
    b.close().await.unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn stalled_application_admission_expires_at_the_existing_io_bound() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(6), channel.state.ended.cancelled())
        .await
        .expect("stalled admission must expire rather than pin a generation");
    assert!(channel.recv().await.is_none());
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::LinkLost)
    );
    managed.close().await.unwrap();
}

/// A real mock carrier with one HAL operation deliberately never completing.
struct BlockingVisibility {
    inner: Arc<MockDriver>,
    entered: tokio::sync::Notify,
    disconnect_blocked: std::sync::atomic::AtomicBool,
    release_disconnect: tokio::sync::Notify,
    stop_blocked: bool,
}
#[async_trait::async_trait]
impl crate::Driver for BlockingVisibility {
    async fn capabilities(&self) -> CapabilityReport {
        self.inner.capabilities().await
    }
    async fn scan(
        &self,
        service: ServiceUuid,
    ) -> crate::Result<crate::BoxStream<crate::Result<crate::DiscoveredPeer>>> {
        self.inner.scan(service).await
    }
    async fn connect(&self, peer: &PeerAddress) -> crate::Result<Box<dyn crate::GattConnection>> {
        self.inner.connect(peer).await
    }
    async fn advertise(&self, spec: crate::GattServiceSpec) -> crate::Result<()> {
        self.inner.advertise(spec).await
    }
    fn served_service(&self) -> Option<crate::GattServiceSpec> {
        self.inner.served_service()
    }
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> crate::Result<()> {
        self.inner
            .set_characteristic_value(characteristic, value)
            .await
    }
    async fn set_advertising_enabled(&self, _enabled: bool) -> crate::Result<()> {
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn stop_advertising(&self) -> crate::Result<()> {
        if self.stop_blocked {
            return std::future::pending().await;
        }
        self.inner.stop_advertising().await
    }
    async fn notify(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> crate::Result<()> {
        self.inner.notify(characteristic, value).await
    }
    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> crate::Result<()> {
        self.inner
            .notify_peer(peer, session, characteristic, value)
            .await
    }
    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> crate::Result<()> {
        if self
            .disconnect_blocked
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release_disconnect.notified().await;
        }
        self.inner.disconnect_peer(peer, session).await
    }
    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        self.inner.notify_max_len(peer, session).await
    }
    fn events(&self) -> crate::BoxStream<crate::GattEvent> {
        self.inner.events()
    }
}

#[tokio::test]
async fn never_completing_visibility_does_not_block_other_peer_or_shutdown() {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let driver = Arc::new(BlockingVisibility {
        inner: Arc::new(MockDriver::new(
            PeerAddress("blocked".into()),
            network.clone(),
            caps,
        )),
        entered: tokio::sync::Notify::new(),
        disconnect_blocked: std::sync::atomic::AtomicBool::new(false),
        release_disconnect: tokio::sync::Notify::new(),
        stop_blocked: false,
    });
    let managed = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
    let remote = PeerLink::with_driver(
        Arc::new(MockDriver::new(PeerAddress("other".into()), network, caps)),
        PeerLinkConfig::new(config()),
    );
    let mut incoming = remote.incoming().unwrap();
    remote.set_advertising(true).await.unwrap();
    let outgoing = managed.request(&PeerAddress("other".into())).await.unwrap();
    let request = incoming.next().await.unwrap();
    let (local, other) = tokio::join!(outgoing.accept(), request.accept());
    let local = local.unwrap();
    let other = other.unwrap();
    managed.set_advertising(true).await.unwrap();
    let operation_owner = managed.clone();
    let pending = tokio::spawn(async move { operation_owner.set_advertising(false).await });
    driver.entered.notified().await;
    other.send(vec![7]).await.unwrap();
    let observed = tokio::time::timeout(Duration::from_millis(300), local.recv()).await;
    // Release every public use before asserting, so a RED cannot hang Drop on the old worker.
    // The old implementation has an unbounded HAL wait; cancel the probe's OS worker by
    // keeping its Arc alive only on the failing path (the test process reclaims it).
    if observed.is_err() {
        pending.abort();
        std::mem::forget(managed);
        std::mem::forget(remote);
        panic!("blocked visibility HAL call parked unrelated peer receive");
    }
    assert_eq!(observed.unwrap().unwrap().unwrap(), vec![7]);
    tokio::time::timeout(Duration::from_millis(300), local.disconnect())
        .await
        .expect("retirement must not wait for unrelated visibility operation")
        .unwrap();
    pending.abort();
    let _ = pending.await;
    let start = std::time::Instant::now();
    drop(managed);
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "shutdown must cancel unrelated HAL startup/toggle"
    );
    assert!(driver.inner.served_service().is_none());
    remote.close().await.unwrap();
}

#[tokio::test]
async fn blocked_native_retirement_keeps_other_peer_responsive_and_awaits_its_own_reply() {
    let network = MockNetwork::new();
    let driver = Arc::new(BlockingVisibility {
        inner: Arc::new(MockDriver::new(
            PeerAddress("retiring".into()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        entered: tokio::sync::Notify::new(),
        disconnect_blocked: std::sync::atomic::AtomicBool::new(false),
        release_disconnect: tokio::sync::Notify::new(),
        stop_blocked: false,
    });
    let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
    let remote = mock_link(&network, "retirement-remote");
    let unrelated = mock_link(&network, "retirement-unrelated");
    let (remote_use, served_use) = connect_links(&remote, &owner, "retiring").await;
    let (other_use, other_remote) = connect_links(&owner, &unrelated, "retirement-unrelated").await;
    driver
        .disconnect_blocked
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let retained = served_use.clone();
    let retirement = tokio::spawn(async move { served_use.disconnect().await });
    tokio::time::timeout(Duration::from_secs(1), driver.entered.notified())
        .await
        .unwrap();
    assert!(
        !retirement.is_finished(),
        "disconnect reply cannot precede native completion"
    );
    assert_eq!(
        retained.close_reason(),
        Some(PeerChannelCloseReason::LocalDisconnect)
    );
    assert!(retained.recv().await.is_none());
    other_remote.send(vec![17]).await.unwrap();
    let observed = tokio::time::timeout(Duration::from_millis(300), other_use.recv()).await;
    driver
        .disconnect_blocked
        .store(false, std::sync::atomic::Ordering::SeqCst);
    driver.release_disconnect.notify_one();
    retirement.await.unwrap().unwrap();
    assert_eq!(
        observed
            .expect("unrelated receive cannot await another generation's native cleanup")
            .unwrap()
            .unwrap(),
        vec![17]
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), remote_use.recv())
            .await
            .unwrap()
            .is_none()
    );
    owner.close().await.unwrap();
    remote.close().await.unwrap();
    unrelated.close().await.unwrap();
}

#[tokio::test]
async fn never_completing_native_server_stop_returns_a_bounded_error() {
    let network = MockNetwork::new();
    let driver = Arc::new(BlockingVisibility {
        inner: Arc::new(MockDriver::new(
            PeerAddress("never-stop".into()),
            network,
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        entered: tokio::sync::Notify::new(),
        disconnect_blocked: std::sync::atomic::AtomicBool::new(false),
        release_disconnect: tokio::sync::Notify::new(),
        stop_blocked: true,
    });
    let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
    owner.set_advertising(true).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(6), owner.close())
        .await
        .expect("HAL stop cannot hang worker shutdown indefinitely");
    assert!(matches!(
        result,
        Err(crate::BleError::Attempt(crate::AttemptError::TimedOut))
    ));
    let start = std::time::Instant::now();
    drop(owner);
    assert!(start.elapsed() < Duration::from_secs(1));
    // A timeout bounds our wait; it cannot certify that an intentionally stuck
    // HAL actually released anything. Clean up the mock directly, and assert no
    // stronger native guarantee than the callback evidence supports.
    driver.inner.stop_advertising().await.unwrap();
}

#[tokio::test]
async fn pending_metadata_probe_reserves_its_address_against_owned_dials() {
    let network = MockNetwork::new();
    let remote = mock_link(&network, "probe-reserved");
    remote.set_advertising(true).await.unwrap();
    let address = PeerAddress("probe-reserved".into());
    network.set_advertisement_data(&address, Default::default(), Default::default(), None);
    let driver = Arc::new(MockDriver::new(
        PeerAddress("probe-reader".into()),
        network,
        CapabilityReport {
            central: true,
            peripheral: true,
        },
    ));
    driver.stall_connect(&address, Duration::from_secs(30));
    let owner = PeerLink::with_driver(driver, PeerLinkConfig::new(config()));
    let discovery = owner.discover().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = tokio::time::timeout(Duration::from_millis(300), owner.request(&address)).await;
    drop(discovery);
    owner.close().await.unwrap();
    remote.close().await.unwrap();
    assert!(
        matches!(result, Ok(Err(crate::BleError::AdapterBusy(_)))),
        "a pending metadata probe must not race an owned dial: {result:?}"
    );
}

#[tokio::test]
async fn dropping_during_acquired_subscribe_wait_finishes_session_cleanup() {
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let remote = PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress("subscribe-remote".into()),
            network.clone(),
            caps,
        )),
        PeerLinkConfig::new(config()),
    );
    remote.set_advertising(true).await.unwrap();
    network.stall_subscribe(Duration::from_secs(30));
    let local = PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress("subscribe-local".into()),
            network.clone(),
            caps,
        )),
        PeerLinkConfig::new(config()),
    );
    let owner = local.clone();
    let request =
        tokio::spawn(async move { owner.request(&PeerAddress("subscribe-remote".into())).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    request.abort();
    let _ = request.await;
    drop(local);
    assert!(network.disconnected_peers().contains(&PeerAddress("subscribe-remote".into())), "worker shutdown must await acquired-session cleanup, not merely spawn it just before destroying its runtime");
    remote.close().await.unwrap();
}

#[tokio::test]
async fn dropping_advertising_link_unpublishes_its_mock_service() {
    use crate::Driver;
    let network = MockNetwork::new();
    let driver = Arc::new(MockDriver::new(
        PeerAddress("dropped".into()),
        network,
        CapabilityReport {
            central: true,
            peripheral: true,
        },
    ));
    let managed = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
    managed.set_advertising(true).await.unwrap();
    assert!(driver.served_service().is_some());
    drop(managed);
    assert!(
        driver.served_service().is_none(),
        "Drop must finish bounded server cleanup before joining"
    );
}

#[test]
fn records_reject_truncation_extensions_unsupported_versions_and_oversize_data() {
    let records = [
        Record::Request {
            sender: EndpointId::from_bytes([1; 16]),
            target: EndpointId::from_bytes([2; 16]),
            attempt: u64::MAX,
            nonce: i32::MIN,
        },
        Record::Response {
            sender: EndpointId::from_bytes([2; 16]),
            target: EndpointId::from_bytes([1; 16]),
            attempt: u64::MAX,
        },
        Record::Decision {
            attempt: 0,
            action: 2,
        },
        Record::Disconnect { attempt: 1 },
        Record::Ping { attempt: 1 },
        Record::Pong { attempt: 1 },
    ];
    for record in records {
        let mut bytes = record.encode();
        assert_eq!(Record::decode(&bytes, 4).unwrap(), record);
        for length in 0..bytes.len() {
            assert!(Record::decode(&bytes[..length], 4).is_err());
        }
        bytes.push(0);
        assert!(Record::decode(&bytes, 4).is_err());
    }
    let mut request = Record::Request {
        sender: EndpointId::from_bytes([1; 16]),
        target: EndpointId::from_bytes([2; 16]),
        attempt: 1,
        nonce: i32::MAX,
    }
    .encode();
    request[1] = 2;
    assert!(Record::decode(&request, 4).is_err());
    assert!(Record::decode(
        &Record::Data {
            attempt: 1,
            payload: vec![0; 5]
        }
        .encode(),
        4
    )
    .is_err());
    assert!(Record::decode(&[255; 9], 4).is_err());
    for bytes in [vec![], vec![b'B', b'G', 0, 1], vec![0; 20]] {
        assert!(managed_codec::ManagedMetadata::decode(&bytes).is_err());
    }
}
#[tokio::test]
async fn data_before_acceptance_fails_closed() {
    let (managed, mut raw, request) = raw_request(config()).await;
    raw.send(
        Record::Data {
            attempt: 71,
            payload: vec![1],
        }
        .encode(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        request.accept().await,
        Err(crate::BleError::Attempt(crate::AttemptError::InvalidRecord))
    ));
    managed.close().await.unwrap();
}
#[tokio::test]
async fn a_stalled_bounded_receiver_resumes_without_losing_scheduled_data() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_millis(300), managed.set_advertising(false))
        .await
        .expect("stalled peer must not block control")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    for byte in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), channel.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![byte]
        );
    }
    assert_eq!(channel.close_reason(), None);
    managed.close().await.unwrap();
}
#[tokio::test]
async fn native_end_interrupts_a_stalled_application_admission() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    raw.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_millis(300), channel.state.ended.cancelled())
        .await
        .expect("native loss must interrupt stalled admission");
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::LinkLost)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn a_stalled_bounded_receiver_reports_gap_without_ending_generation() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    // Even with the sole application slot full, lifecycle commands must progress.
    tokio::time::timeout(Duration::from_millis(300), managed.set_advertising(false))
        .await
        .expect("full application queue must not park the state owner")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    // Exercise the actual out-of-band loss flag independently of ordinary
    // application backpressure, which must no longer manufacture a gap.
    channel.state.overflow.raise();
    let gap = tokio::time::timeout(Duration::from_millis(300), channel.recv())
        .await
        .unwrap();
    assert!(
        matches!(gap, Some(Err(_))),
        "gap must precede buffered payload: {gap:?}"
    );
    assert_eq!(channel.close_reason(), None);
    for byte in 0..3 {
        assert_eq!(channel.recv().await.unwrap().unwrap(), vec![byte]);
    }
    raw.send(
        Record::Data {
            attempt: 71,
            payload: vec![9],
        }
        .encode(),
    )
    .await
    .unwrap();
    assert_eq!(channel.recv().await.unwrap().unwrap(), vec![9]);
    channel.send(vec![10]).await.unwrap();
    assert!(
        matches!(Record::decode(&raw.recv().await.unwrap().unwrap(), 128).unwrap(), Record::Data { payload, .. } if payload == vec![10])
    );
    managed.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiving_progresses_while_a_large_native_send_is_in_flight() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut config = config();
    config.inbound_queue_depth = 2;
    let (managed, mut raw, channel) = raw_established(config).await;
    let raw_sender = raw.sender();
    tokio::time::timeout(Duration::from_secs(5), async {
        let transmit = channel.send(vec![42; 128 * 1024]);
        let peer_transmit = async {
            for byte in 0..24 {
                raw_sender
                    .send(
                        Record::Data {
                            attempt: 71,
                            payload: vec![byte],
                        }
                        .encode(),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let receiver = channel.clone();
        let receive = tokio::spawn(async move {
            for byte in 0..24 {
                let item = receiver.recv().await;
                assert!(
                    item.is_some(),
                    "receive ended at {byte}: {:?}",
                    receiver.close_reason()
                );
                assert_eq!(item.unwrap().unwrap(), vec![byte]);
            }
        });
        let peer_receive = async {
            assert_eq!(
                Record::decode(&raw.recv().await.unwrap().unwrap(), 128 * 1024).unwrap(),
                Record::Data {
                    attempt: 71,
                    payload: vec![42; 128 * 1024]
                }
            );
        };
        let (sent, (), received, ()) = tokio::join!(transmit, peer_transmit, receive, peer_receive);
        sent.unwrap();
        received.unwrap();
    })
    .await
    .unwrap();
    managed.close().await.unwrap();
}

#[tokio::test]
async fn malformed_record_does_not_keep_a_channel_alive() {
    let (managed, mut raw, channel) = raw_established(config()).await;
    raw.send(vec![255]).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), channel.recv())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::ProtocolError)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn silent_peer_expires_at_the_loss_bound() {
    let (managed, _raw, channel) = raw_established(config()).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(32), channel.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::LinkLost)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn shutdown_records_reason_before_closing_receive() {
    let (managed, _raw, channel) = raw_established(config()).await;
    managed.close().await.unwrap();
    assert!(channel.recv().await.is_none());
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::Shutdown)
    );
}
#[tokio::test]
async fn raw_legacy_and_wrong_version_peers_never_enter_managed_incoming() {
    for legacy in [true, false] {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let raw_driver = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            PeerLinkConfig::new(config()),
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw =
            DatagramChannel::connect_driver(raw_driver, &PeerAddress("managed".into()), &config())
                .await
                .unwrap();
        let mut bytes = if legacy {
            b"legacy payload".to_vec()
        } else {
            Record::Request {
                sender: EndpointId::from_bytes([1; 16]),
                target: managed.endpoint_id(),
                attempt: 1,
                nonce: 0,
            }
            .encode()
        };
        if !legacy {
            bytes[1] = 2;
        }
        raw.send(bytes).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), incoming.next())
                .await
                .is_err()
        );
        managed.close().await.unwrap();
    }
}
