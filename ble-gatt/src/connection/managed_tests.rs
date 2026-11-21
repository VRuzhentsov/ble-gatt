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
#[path = "../drivers/android/advertisement_outcome.rs"]
mod advertisement_outcome;

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
    metadata: Option<Arc<MetadataControl>>,
}

#[derive(Default)]
struct MetadataControl {
    scan_input:
        std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<crate::Result<crate::DiscoveredPeer>>>>,
    scan_drained: tokio::sync::Notify,
    critical_events: std::sync::Mutex<Option<crate::drivers::event_bus::EventBus>>,
    updates: std::sync::atomic::AtomicUsize,
    values: std::sync::atomic::AtomicUsize,
    starts: std::sync::atomic::AtomicUsize,
    connects: std::sync::atomic::AtomicUsize,
    stops: std::sync::atomic::AtomicUsize,
    startup_blocked: std::sync::atomic::AtomicBool,
    advertise_failure: std::sync::atomic::AtomicBool,
    blocked: std::sync::atomic::AtomicBool,
    native_failure: std::sync::atomic::AtomicBool,
    value_failure: std::sync::atomic::AtomicBool,
    hide_failure: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    started: tokio::sync::Notify,
    lifecycle_subscriptions: std::sync::atomic::AtomicUsize,
    loss_seen: tokio::sync::Notify,
    on_seen: tokio::sync::Notify,
    late_native: std::sync::atomic::AtomicBool,
    pending_native: std::sync::Mutex<
        Option<advertisement_outcome::PendingAdvertisement<crate::GattServiceSpec>>,
    >,
    committed_native: std::sync::Mutex<Option<crate::GattServiceSpec>>,
}

impl BlockingVisibility {
    async fn complete_native_update(&self) -> bool {
        let control = self.metadata.as_ref().unwrap();
        let pending = control.pending_native.lock().unwrap().take();
        let Some(pending) = pending else {
            return false;
        };
        self.inner
            .update_advertisement(pending.service.clone().unwrap())
            .await
            .unwrap();
        pending.complete(Ok(()), &control.committed_native);
        true
    }
    fn metadata(
        network: Arc<MockNetwork>,
        address: &str,
    ) -> (Arc<Self>, Arc<MetadataControl>, Arc<PeerLink>) {
        Self::metadata_with(network, address, config())
    }
    fn metadata_with(
        network: Arc<MockNetwork>,
        address: &str,
        config: DatagramConfig,
    ) -> (Arc<Self>, Arc<MetadataControl>, Arc<PeerLink>) {
        let metadata = Arc::new(MetadataControl::default());
        let driver = Arc::new(Self {
            inner: Arc::new(MockDriver::new(
                PeerAddress(address.into()),
                network,
                CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            )),
            entered: tokio::sync::Notify::new(),
            disconnect_blocked: std::sync::atomic::AtomicBool::new(false),
            release_disconnect: tokio::sync::Notify::new(),
            stop_blocked: false,
            metadata: Some(metadata.clone()),
        });
        let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config));
        (driver, metadata, owner)
    }
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
        let scan = self.inner.scan(service).await?;
        if let Some(control) = &self.metadata {
            let input = control.scan_input.lock().unwrap().take();
            if let Some(mut input) = input {
                let control = control.clone();
                return Ok(Box::pin(scan.chain(futures::stream::poll_fn(move |cx| {
                    control.scan_drained.notify_one();
                    input.poll_recv(cx)
                }))));
            }
        }
        Ok(scan)
    }
    async fn connect(&self, peer: &PeerAddress) -> crate::Result<Box<dyn crate::GattConnection>> {
        if let Some(control) = &self.metadata {
            control
                .connects
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.connect(peer).await
    }
    async fn advertise(&self, spec: crate::GattServiceSpec) -> crate::Result<()> {
        if let Some(control) = &self.metadata {
            control
                .starts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if let Some(control) = &self.metadata {
            if control
                .startup_blocked
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                control.entered.notify_one();
                control.release.notified().await;
            }
            if control
                .advertise_failure
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::BleError::Gatt("advertise refused".into()));
            }
        }
        let result = self.inner.advertise(spec).await;
        if let Some(control) = &self.metadata {
            control.started.notify_one();
        }
        result
    }
    fn served_service(&self) -> Option<crate::GattServiceSpec> {
        self.inner.served_service()
    }
    async fn update_advertisement(&self, spec: crate::GattServiceSpec) -> crate::Result<()> {
        if let Some(control) = &self.metadata {
            control
                .updates
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if control
                .late_native
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                *control.pending_native.lock().unwrap() =
                    Some(advertisement_outcome::PendingAdvertisement {
                        sender,
                        service: Some(spec),
                    });
                control.entered.notify_one();
                return receiver
                    .await
                    .map_err(|_| crate::BleError::Gatt("native callback invalidated".into()))?
                    .map_err(|code| crate::BleError::Gatt(format!("native error {code}")));
            }
            control.entered.notify_one();
            if control.blocked.load(std::sync::atomic::Ordering::SeqCst) {
                control.release.notified().await;
            }
            if control
                .native_failure
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::BleError::Gatt("native update refused".into()));
            }
        }
        self.inner.update_advertisement(spec).await
    }
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> crate::Result<()> {
        if let Some(control) = &self.metadata {
            control
                .values
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if control
                .value_failure
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::BleError::Gatt("readable update refused".into()));
            }
        }
        self.inner
            .set_characteristic_value(characteristic, value)
            .await
    }
    async fn set_advertising_enabled(&self, enabled: bool) -> crate::Result<()> {
        if let Some(control) = &self.metadata {
            if !enabled
                && control
                    .hide_failure
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::BleError::Unsupported("hide fence".into()));
            }
            let result = self.inner.set_advertising_enabled(enabled).await;
            if result.is_ok() && !enabled {
                if let Some(pending) = control.pending_native.lock().unwrap().take() {
                    let _ = pending.send(Err(-1));
                }
            }
            return result;
        }
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn stop_advertising(&self) -> crate::Result<()> {
        if let Some(control) = &self.metadata {
            control
                .stops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if self.stop_blocked {
            return std::future::pending().await;
        }
        if let Some(control) = &self.metadata {
            control.pending_native.lock().unwrap().take();
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
    fn lifecycle_events(&self) -> crate::BoxStream<crate::GattEvent> {
        let events = self.inner.lifecycle_events();
        if let Some(control) = &self.metadata {
            if control
                .lifecycle_subscriptions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                let injected = control
                    .critical_events
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|bus| bus.lifecycle_events());
                let events: crate::BoxStream<crate::GattEvent> = match injected {
                    Some(injected) => Box::pin(events.merge(injected)),
                    None => events,
                };
                let control = control.clone();
                return Box::pin(events.map(move |event| {
                    match &event {
                        crate::GattEvent::RadioChanged {
                            status: crate::RadioStatus::Off,
                        }
                        | crate::GattEvent::Lagged { .. } => control.loss_seen.notify_one(),
                        crate::GattEvent::RadioChanged {
                            status: crate::RadioStatus::On,
                        } => control.on_seen.notify_one(),
                        _ => {}
                    }
                    event
                }));
            }
        }
        events
    }
}

#[tokio::test]
async fn managed_discovery_zero_capacity_reports_queue_full_and_ends() {
    let network = MockNetwork::new();
    let advertiser = mock_link(&network, "zero-advertiser");
    advertiser.set_advertising(true).await.unwrap();
    let (driver, control, unused) = BlockingVisibility::metadata(network, "zero-reader");
    unused.close().await.unwrap();
    let mut cfg = PeerLinkConfig::new(config());
    cfg.max_links = crate::MaxLinks(0);
    let reader = PeerLink::with_driver(driver, cfg);
    let mut discovery = reader.discover().await.unwrap();
    let item = tokio::time::timeout(Duration::from_millis(300), discovery.next())
        .await
        .expect("finite discovery must report refused initial work");
    assert!(matches!(
        item,
        Some(Err(crate::BleError::Attempt(
            crate::AttemptError::QueueFull
        )))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), discovery.next())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        control.connects.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn managed_discovery_full_capacity_defers_then_owned_sighting_drains() {
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let blocker = mock_link(&network, "full-blocker");
    let target = mock_link(&network, "full-target");
    let (driver, control, unused) = BlockingVisibility::metadata(network, "full-reader");
    unused.close().await.unwrap();
    let mut cfg = PeerLinkConfig::new(config());
    cfg.max_links = crate::MaxLinks(1);
    let reader = PeerLink::with_driver(driver.clone(), cfg);
    let (old_left, old_right) = connect_links(&reader, &blocker, "full-blocker").await;
    blocker.set_advertising(false).await.unwrap();
    target.set_advertising(true).await.unwrap();
    let mut finite = reader.discover().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(300), finite.next())
            .await
            .unwrap(),
        Some(Err(crate::BleError::Attempt(
            crate::AttemptError::QueueFull
        )))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), finite.next())
            .await
            .unwrap()
            .is_none()
    );
    drop(finite);
    let sighting = driver
        .inner
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    control.scan_drained.notified().await;
    let refused = tokio::time::timeout(Duration::from_millis(300), discovery.next())
        .await
        .expect("occupied positive capacity must report initial refusal");
    assert!(matches!(
        refused,
        Some(Err(crate::BleError::Attempt(
            crate::AttemptError::QueueFull
        )))
    ));
    assert_eq!(
        control.connects.load(SeqCst),
        1,
        "discovery cannot exceed the link limit"
    );
    old_left.send(vec![31]).await.unwrap();
    assert_eq!(old_right.recv().await.unwrap().unwrap(), vec![31]);
    old_left.disconnect().await.unwrap();
    let (left, right) = connect_links(&reader, &target, "full-target").await;
    input.send(Ok(sighting)).await.unwrap();
    drop(input);
    let (endpoint, _) = tokio::time::timeout(Duration::from_millis(300), discovery.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, target.endpoint_id());
    assert!(
        tokio::time::timeout(Duration::from_millis(300), discovery.next())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        control.connects.load(SeqCst),
        2,
        "owned sightings cannot start a raw probe"
    );
    left.send(vec![32]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![32]);
    right.send(vec![33]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![33]);
    reader.close().await.unwrap();
    blocker.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn managed_discovery_tracked_address_becomes_owned_without_competing_probe() {
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let target = mock_link(&network, "transition-target");
    let seed = mock_link(&network, "transition-seed");
    let (driver, control, reader) = BlockingVisibility::metadata(network, "transition-reader");
    seed.set_advertising(true).await.unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    // A completed probe consumes the interval's initial immediate tick.
    let (endpoint, _) = tokio::time::timeout(Duration::from_millis(300), discovery.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, seed.endpoint_id());
    seed.set_advertising(false).await.unwrap();
    target.set_advertising(true).await.unwrap();
    let sighting = driver
        .inner
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    input.send(Ok(sighting.clone())).await.unwrap();
    // This ordered stream item confirms that Worker tracked the preceding sighting.
    input
        .send(Err(crate::BleError::Gatt("sighting barrier".into())))
        .await
        .unwrap();
    assert!(
        matches!(discovery.next().await, Some(Err(crate::BleError::Gatt(message))) if message == "sighting barrier")
    );
    assert_eq!(
        control.connects.load(SeqCst),
        1,
        "target metadata remains unprobed"
    );
    let (left, right) = connect_links(&reader, &target, "transition-target").await;
    input.send(Ok(sighting)).await.unwrap();
    drop(input);
    // The tick and the owned sighting can publish in either order.
    let results = tokio::time::timeout(Duration::from_millis(300), discovery.collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|item| matches!(item, Err(crate::BleError::AdapterBusy(_))))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter_map(|item| item.as_ref().ok())
            .map(|(endpoint, _)| *endpoint)
            .collect::<Vec<_>>(),
        vec![target.endpoint_id()]
    );
    assert_eq!(control.connects.load(SeqCst), 2);
    left.send(vec![34]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![34]);
    right.send(vec![35]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![35]);
    reader.close().await.unwrap();
    seed.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn managed_discovery_deferred_capacity_refreshes_without_another_sighting() {
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let blocker = mock_link(&network, "deferred-blocker");
    let target = mock_link(&network, "deferred-target");
    let (driver, control, unused) = BlockingVisibility::metadata(network, "deferred-reader");
    unused.close().await.unwrap();
    let mut cfg = PeerLinkConfig::new(config());
    cfg.max_links = crate::MaxLinks(1);
    let reader = PeerLink::with_driver(driver, cfg);
    let (left, right) = connect_links(&reader, &blocker, "deferred-blocker").await;
    blocker.set_advertising(false).await.unwrap();
    target.set_advertising(true).await.unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(300), discovery.next())
            .await
            .unwrap(),
        Some(Err(crate::BleError::Attempt(
            crate::AttemptError::QueueFull
        )))
    ));
    assert_eq!(control.connects.load(SeqCst), 1);
    left.send(vec![36]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![36]);
    left.disconnect().await.unwrap();
    let (endpoint, _) = tokio::time::timeout(Duration::from_secs(11), discovery.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, target.endpoint_id());
    assert_eq!(
        control.connects.load(SeqCst),
        2,
        "one refresh probe follows capacity release"
    );
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), discovery.next())
            .await
            .unwrap()
            .is_none()
    );
    reader.close().await.unwrap();
    blocker.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_critical_lag_guards_visible_server_until_hidden_retry() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (driver, control, owner) = BlockingVisibility::metadata(network.clone(), "lag-owner");
    let critical = crate::drivers::event_bus::EventBus::new(2);
    *control.critical_events.lock().unwrap() = Some(critical.clone());
    let peer = mock_link(&network, "lag-peer");
    let (left, right) = connect_links(&peer, &owner, "lag-owner").await;
    let observer = MockDriver::new(
        PeerAddress("lag-observer".into()),
        network,
        CapabilityReport {
            central: true,
            peripheral: true,
        },
    );
    let mut readable = observer
        .connect(&PeerAddress("lag-owner".into()))
        .await
        .unwrap();
    let before = readable
        .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
        .await
        .unwrap();
    let before_native = driver.served_service().unwrap().manufacturer_data;
    // Overflow the production critical stream. No Off or On event occurs.
    for session in 0..4 {
        critical.send_lifecycle(crate::GattEvent::Connected {
            peer: PeerAddress("unrelated".into()),
            local_role: crate::Role::Peripheral,
            session: Some(session),
        });
    }
    tokio::time::timeout(Duration::from_millis(300), control.loss_seen.notified())
        .await
        .unwrap();
    let desired = GattAdvertisement {
        local_name: Some("After critical loss".into()),
        manufacturer_data: [(0xFFFF, vec![7, 6, 5, 4, 3])].into(),
        ..Default::default()
    };
    let result = owner.update_advertisement(desired.clone()).await;
    assert!(
        matches!(result, Err(crate::BleError::GattBusy(_))),
        "unknown visible native state cannot return staging success: {result:?}"
    );
    assert_eq!(
        readable
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        driver.served_service().unwrap().manufacturer_data,
        before_native
    );
    assert!(observer
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .is_some());
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::GattBusy(_))
    ));
    owner.set_advertising(false).await.unwrap();
    assert!(observer
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::GattBusy(_))
    ));
    owner.update_advertisement(desired.clone()).await.unwrap();
    assert!(observer
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    let actual = GattAdvertisement::decode(
        &readable
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(actual.local_name, desired.local_name);
    assert_eq!(actual.manufacturer_data, desired.manufacturer_data);
    owner.set_advertising(true).await.unwrap();
    let visible = observer
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(visible.manufacturer_data, desired.manufacturer_data);
    assert_eq!(
        control.starts.load(SeqCst),
        1,
        "reconciliation must not register another server"
    );
    assert_eq!(control.stops.load(SeqCst), 0);
    left.send(vec![61]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![61]);
    right.send(vec![62]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![62]);
    assert_eq!(left.close_reason(), None);
    assert_eq!(right.close_reason(), None);
    readable.disconnect().await.unwrap();
    peer.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_critical_lag_invalidates_admitted_update_without_losing_desired_values() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (driver, control, owner) =
        BlockingVisibility::metadata(network.clone(), "pending-lag-owner");
    let critical = crate::drivers::event_bus::EventBus::new(2);
    *control.critical_events.lock().unwrap() = Some(critical.clone());
    let peer = mock_link(&network, "pending-lag-peer");
    let (left, right) = connect_links(&peer, &owner, "pending-lag-owner").await;
    let observer = MockDriver::new(
        PeerAddress("pending-lag-observer".into()),
        network,
        CapabilityReport {
            central: true,
            peripheral: true,
        },
    );
    let mut readable = observer
        .connect(&PeerAddress("pending-lag-owner".into()))
        .await
        .unwrap();
    let desired = GattAdvertisement {
        local_name: Some("Admitted before loss".into()),
        manufacturer_data: [(0xFFFF, vec![1, 4, 7, 2, 5])].into(),
        ..Default::default()
    };
    control.blocked.store(true, SeqCst);
    let update_owner = owner.clone();
    let update_desired = desired.clone();
    let update =
        tokio::spawn(async move { update_owner.update_advertisement(update_desired).await });
    tokio::time::timeout(Duration::from_millis(300), control.entered.notified())
        .await
        .unwrap();
    for session in 0..4 {
        critical.send_lifecycle(crate::GattEvent::Connected {
            peer: PeerAddress("unrelated".into()),
            local_role: crate::Role::Peripheral,
            session: Some(session),
        });
    }
    tokio::time::timeout(Duration::from_millis(300), control.loss_seen.notified())
        .await
        .unwrap();
    control.blocked.store(false, SeqCst);
    control.release.notify_one();
    assert!(matches!(
        update.await.unwrap(),
        Err(crate::BleError::Attempt(crate::AttemptError::PeerLost))
    ));
    let actual = GattAdvertisement::decode(
        &readable
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(actual.local_name, desired.local_name);
    assert_eq!(actual.manufacturer_data, desired.manufacturer_data);
    assert_eq!(
        driver.served_service().unwrap().manufacturer_data,
        desired.manufacturer_data
    );
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::GattBusy(_))
    ));
    owner.set_advertising(false).await.unwrap();
    owner.update_advertisement(desired).await.unwrap();
    owner.set_advertising(true).await.unwrap();
    assert_eq!(control.starts.load(SeqCst), 1);
    assert_eq!(control.stops.load(SeqCst), 0);
    left.send(vec![63]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![63]);
    right.send(vec![64]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![64]);
    readable.disconnect().await.unwrap();
    peer.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_continuous_read_refreshes_without_new_native_fields() {
    use crate::advertisement::GattAdvertisement;
    let network = MockNetwork::new();
    // No radio record (as a Linux peer with Fini's data): the refresh timer applies.
    network.set_radio_capacity(
        &PeerAddress("name-refresh".into()),
        crate::drivers::mock::RadioCapacity::None,
    );
    let (_, _, advertiser) = BlockingVisibility::metadata(network.clone(), "name-refresh");
    advertiser
        .update_advertisement(GattAdvertisement {
            local_name: Some("Before timer".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    advertiser.set_advertising(true).await.unwrap();
    let (_, control, reader) = BlockingVisibility::metadata(network, "name-refresh-reader");
    let (_input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, before) = discovery.next().await.unwrap().unwrap();
    assert_eq!(before.name.as_deref(), Some("Before timer"));
    advertiser
        .update_advertisement(GattAdvertisement::default())
        .await
        .unwrap();
    // Once a read found the endpoint, the refresh is `KNOWN_PEER_REFRESH` (30 s), not 10 s.
    let changed = tokio::time::timeout(
        crate::advertisement::KNOWN_PEER_REFRESH + Duration::from_secs(2),
        discovery.next(),
    )
    .await;
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
    let (current_endpoint, current) = changed
        .expect("the existing metadata refresh schedule must detect a readable-only removal")
        .unwrap()
        .unwrap();
    assert_eq!(current_endpoint, endpoint);
    assert_eq!(current.name, None);
    assert!(current.manufacturer_data.is_empty());
}

#[tokio::test]
async fn managed_metadata_close_cancels_gated_startup_before_native_registration() {
    use std::sync::atomic::Ordering::SeqCst;
    let (driver, control, owner) =
        BlockingVisibility::metadata(MockNetwork::new(), "close-startup");
    control.startup_blocked.store(true, SeqCst);
    let start_owner = owner.clone();
    let start = tokio::spawn(async move { start_owner.set_advertising(true).await });
    tokio::time::timeout(Duration::from_millis(300), control.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(300), owner.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        start.await.unwrap(),
        Err(crate::BleError::Attempt(crate::AttemptError::Cancelled))
    ));
    control.startup_blocked.store(false, SeqCst);
    control.release.notify_one();
    assert!(driver.served_service().is_none());
    assert_eq!(control.starts.load(SeqCst), 1);
    assert_eq!(control.stops.load(SeqCst), 1);
}

#[tokio::test]
async fn managed_failed_startup_is_stopped_and_a_later_show_starts_again() {
    use std::sync::atomic::Ordering::SeqCst;
    let (driver, control, owner) =
        BlockingVisibility::metadata(MockNetwork::new(), "failed-startup");
    control.advertise_failure.store(true, SeqCst);
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::Gatt(_))
    ));
    assert_eq!(
        control.stops.load(SeqCst),
        1,
        "the failed startup is stopped"
    );
    control.advertise_failure.store(false, SeqCst);
    owner.set_advertising(true).await.unwrap();
    assert!(driver.served_service().is_some());
    assert_eq!(control.starts.load(SeqCst), 2);
    owner.close().await.unwrap();
    assert!(driver.served_service().is_none());
    assert_eq!(control.stops.load(SeqCst), 2);
}

#[tokio::test]
async fn managed_close_keeps_a_server_the_link_did_not_start() {
    use std::sync::atomic::Ordering::SeqCst;
    let (driver, control, owner) =
        BlockingVisibility::metadata(MockNetwork::new(), "caller-server");
    // The caller serves on the same driver; the link only scans.
    let spec = managed_codec::ManagedMetadata {
        endpoint: owner.endpoint_id(),
    }
    .service_spec(&config())
    .unwrap();
    driver.inner.advertise(spec).await.unwrap();
    drop(owner.discover().await.unwrap());
    owner.close().await.unwrap();
    assert_eq!(control.stops.load(SeqCst), 0);
    assert!(driver.served_service().is_some());
    driver.inner.stop_advertising().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_invalid_startup_rejects_before_any_native_effect() {
    use std::sync::atomic::Ordering::SeqCst;
    for invalid in 0..3 {
        let (driver, control, unused) =
            BlockingVisibility::metadata(MockNetwork::new(), "invalid-startup");
        unused.close().await.unwrap();
        let mut config = config();
        match invalid {
            0 => {
                config
                    .advertised_service_data
                    .insert(config.service, vec![0; 20]);
            }
            1 => {
                config.advertised_local_name = Some("x".repeat(255));
            }
            _ => {
                config.advertised_manufacturer_data = [(1, vec![0; 240]), (2, vec![0; 240])].into();
            }
        }
        let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config));
        assert!(matches!(
            owner.set_advertising(true).await,
            Err(crate::BleError::Protocol(_))
        ));
        assert_eq!(control.starts.load(SeqCst), 0);
        assert!(driver.served_service().is_none());
        owner.close().await.unwrap();
    }
}

#[tokio::test]
async fn managed_metadata_caller_abort_keeps_admitted_operation_owned() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    let (driver, control, owner) =
        BlockingVisibility::metadata(MockNetwork::new(), "cancel-metadata");
    owner.set_advertising(true).await.unwrap();
    control.blocked.store(true, SeqCst);
    let desired = GattAdvertisement {
        local_name: Some("Accepted before caller abort".into()),
        ..Default::default()
    };
    let update_owner = owner.clone();
    let metadata = desired.clone();
    let update = tokio::spawn(async move { update_owner.update_advertisement(metadata).await });
    control.entered.notified().await;
    update.abort();
    assert!(update.await.unwrap_err().is_cancelled());
    control.blocked.store(false, SeqCst);
    control.release.notify_one();
    tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            let spec = driver.served_service().unwrap();
            let readable = GattAdvertisement::decode(
                &spec
                    .characteristics
                    .iter()
                    .find(|v| v.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
                    .unwrap()
                    .initial_value,
            )
            .unwrap();
            if readable.local_name == desired.local_name {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the admitted update completes after its caller aborted");
    assert_eq!(control.values.load(SeqCst), 1);
    assert_eq!(control.updates.load(SeqCst), 1);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_owned_discovery_preserves_session_without_parallel_probe() {
    use crate::advertisement::GattAdvertisement;
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (_, _, local) = BlockingVisibility::metadata(network.clone(), "owned-reader");
    let (_, control, remote) = BlockingVisibility::metadata(network.clone(), "owned-advertiser");
    local.set_advertising(true).await.unwrap();
    let (left, right) = connect_links(&local, &remote, "owned-advertiser").await;
    let connects = control.connects.load(SeqCst);
    let desired = GattAdvertisement {
        local_name: Some("Readable name on owned peer".into()),
        manufacturer_data: [(0xFFFF, vec![1, 2, 3, 4, 5])].into(),
        ..Default::default()
    };
    local.update_advertisement(desired.clone()).await.unwrap();
    let mut discovery = remote.discover().await.unwrap();
    let (endpoint, discovered) = discovery.next().await.unwrap().unwrap();
    assert_eq!(endpoint, local.endpoint_id());
    assert_eq!(discovered.manufacturer_data, desired.manufacturer_data);
    assert_eq!(control.connects.load(SeqCst), connects);
    println!(
        "owned discovery observed readable_name={:?}; desired_name={:?}; no parallel probe",
        discovered.name, desired.local_name
    );
    left.send(vec![44]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![44]);
    drop(discovery);
    local.close().await.unwrap();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_fresh_and_continuous_discovery_reads_updated_fields() {
    use crate::advertisement::GattAdvertisement;
    let network = MockNetwork::new();
    let (_, _, advertiser) = BlockingVisibility::metadata(network.clone(), "refresh-advertiser");
    let mut desired = GattAdvertisement {
        manufacturer_data: [(0xFFFF, vec![0, 1, 2, 3, 4])].into(),
        local_name: Some("Before".into()),
        service_data: [(ServiceUuid(uuid::Uuid::from_u128(77)), vec![9])].into(),
    };
    advertiser
        .update_advertisement(desired.clone())
        .await
        .unwrap();
    advertiser.set_advertising(true).await.unwrap();
    let (driver, control, reader) = BlockingVisibility::metadata(network.clone(), "refresh-reader");
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, first) = tokio::time::timeout(Duration::from_millis(300), discovery.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(first.name, desired.local_name);
    assert_eq!(first.manufacturer_data, desired.manufacturer_data);
    desired
        .manufacturer_data
        .insert(0xFFFF, vec![1, 5, 6, 7, 8]);
    desired.local_name = Some("After".into());
    desired
        .service_data
        .insert(ServiceUuid(uuid::Uuid::from_u128(77)), vec![8]);
    advertiser
        .update_advertisement(desired.clone())
        .await
        .unwrap();
    // Fresh discovery must read current GATT data, not only observe local setter success.
    let fresh = mock_link(&network, "refresh-fresh");
    let mut fresh_discovery = fresh.discover().await.unwrap();
    let (_, current) = fresh_discovery.next().await.unwrap().unwrap();
    assert_eq!(current.name, desired.local_name);
    assert_eq!(current.manufacturer_data, desired.manufacturer_data);
    assert_eq!(
        current
            .service_data
            .get(&ServiceUuid(uuid::Uuid::from_u128(77))),
        Some(&vec![8])
    );
    drop(fresh_discovery);
    fresh.close().await.unwrap();
    let sighting = driver
        .inner
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    input.send(Ok(sighting)).await.unwrap();
    let changed = tokio::time::timeout(Duration::from_millis(300), discovery.next()).await;
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
    let (updated_endpoint, updated) = changed
        .expect("one continuous discovery must complete changed native flags and readable name")
        .unwrap()
        .unwrap();
    assert_eq!(updated_endpoint, endpoint);
    assert_eq!(updated.manufacturer_data, desired.manufacturer_data);
    assert_eq!(updated.name, desired.local_name);
    assert_eq!(
        updated
            .service_data
            .get(&ServiceUuid(uuid::Uuid::from_u128(77))),
        Some(&vec![8])
    );
}

#[tokio::test]
async fn managed_metadata_loss_during_update_requires_fenced_hidden_reconciliation() {
    use crate::advertisement::GattAdvertisement;
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (driver, control, owner) = BlockingVisibility::metadata(network, "loss-during-update");
    owner.set_advertising(true).await.unwrap();
    control.started.notified().await;
    control.blocked.store(true, SeqCst);
    let metadata = GattAdvertisement {
        local_name: Some("Desired across loss".into()),
        ..Default::default()
    };
    let update_owner = owner.clone();
    let desired = metadata.clone();
    let update = tokio::spawn(async move { update_owner.update_advertisement(desired).await });
    control.entered.notified().await;
    driver.inner.simulate_radio(crate::RadioStatus::Off);
    tokio::time::timeout(Duration::from_millis(300), control.loss_seen.notified())
        .await
        .unwrap();
    driver.inner.simulate_radio(crate::RadioStatus::On);
    tokio::time::timeout(Duration::from_millis(300), control.on_seen.notified())
        .await
        .unwrap();
    control.blocked.store(false, SeqCst);
    control.release.notify_one();
    assert!(matches!(
        update.await.unwrap(),
        Err(crate::BleError::Attempt(crate::AttemptError::PeerLost))
    ));
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::GattBusy(_))
    ));
    assert_eq!(control.starts.load(SeqCst), 1);
    owner.set_advertising(false).await.unwrap();
    owner.update_advertisement(metadata).await.unwrap();
    let shown = owner.set_advertising(true).await;
    owner.close().await.unwrap();
    shown.expect("a successful native hide must permit full hidden reconciliation after loss");
    assert_eq!(control.updates.load(SeqCst), 2);
    assert_eq!(control.starts.load(SeqCst), 1);
}

#[tokio::test]
async fn managed_metadata_timeout_fences_real_pending_outcome_and_close_drains_update() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (driver, control, owner) = BlockingVisibility::metadata(network.clone(), "late-metadata");
    owner.set_advertising(true).await.unwrap();
    let remote = mock_link(&network, "late-remote");
    let (left, right) = connect_links(&remote, &owner, "late-metadata").await;
    control.late_native.store(true, SeqCst);
    let metadata = GattAdvertisement {
        local_name: Some("Late desired".into()),
        ..Default::default()
    };
    let update_owner = owner.clone();
    let update_metadata = metadata.clone();
    let update =
        tokio::spawn(async move { update_owner.update_advertisement(update_metadata).await });
    tokio::time::timeout(Duration::from_millis(300), control.entered.notified())
        .await
        .unwrap();
    let mut busy = tokio::task::JoinSet::new();
    for _ in 0..128 {
        let owner = owner.clone();
        busy.spawn(async move {
            owner
                .update_advertisement(GattAdvertisement::default())
                .await
        });
    }
    while let Some(result) = busy.join_next().await {
        assert!(matches!(result.unwrap(), Err(crate::BleError::GattBusy(_))));
    }
    assert!(matches!(
        owner.set_advertising(false).await,
        Err(crate::BleError::GattBusy(_))
    ));
    left.send(vec![91]).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), right.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![91]
    );
    right.send(vec![92]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![92]);
    let result = tokio::time::timeout(Duration::from_secs(6), update)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(crate::BleError::Attempt(crate::AttemptError::TimedOut))
    ));
    assert!(driver.complete_native_update().await);
    let committed = control
        .committed_native
        .lock()
        .unwrap()
        .clone()
        .expect("production pending-outcome completion commits after the receiver times out");
    let committed_metadata = GattAdvertisement::decode(
        &committed
            .characteristics
            .iter()
            .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value,
    )
    .unwrap();
    assert_eq!(committed_metadata.local_name, metadata.local_name);
    assert_eq!(
        control.values.load(SeqCst),
        1,
        "the readable value is installed once, before the native update it matches"
    );
    assert!(matches!(
        owner.set_advertising(true).await,
        Err(crate::BleError::GattBusy(_))
    ));
    assert!(matches!(
        owner.update_advertisement(metadata.clone()).await,
        Err(crate::BleError::GattBusy(_))
    ));
    owner.set_advertising(false).await.unwrap();
    assert!(!driver.complete_native_update().await);
    control.late_native.store(false, SeqCst);
    owner.update_advertisement(metadata.clone()).await.unwrap();
    control.entered.notified().await;
    let spec = driver.served_service().unwrap();
    let readable = GattAdvertisement::decode(
        &spec
            .characteristics
            .iter()
            .find(|v| v.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value,
    )
    .unwrap();
    assert_eq!(readable.local_name, metadata.local_name);
    owner.set_advertising(true).await.unwrap();
    // A second timeout leaves its native callback pending until a successful hide fences it.
    control.late_native.store(true, SeqCst);
    let update_owner = owner.clone();
    let pending_metadata = metadata.clone();
    let fenced_update =
        tokio::spawn(async move { update_owner.update_advertisement(pending_metadata).await });
    control.entered.notified().await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(6), fenced_update)
            .await
            .unwrap()
            .unwrap(),
        Err(crate::BleError::Attempt(crate::AttemptError::TimedOut))
    ));
    assert!(control.pending_native.lock().unwrap().is_some());
    owner.set_advertising(false).await.unwrap();
    assert!(
        !driver.complete_native_update().await,
        "hide must invalidate the still-pending native outcome"
    );
    control.late_native.store(false, SeqCst);
    owner.update_advertisement(metadata.clone()).await.unwrap();
    control.entered.notified().await;
    owner.set_advertising(true).await.unwrap();
    // Actual owner close cancels an admitted native waiter, not only the public task.
    control.late_native.store(true, SeqCst);
    let update_owner = owner.clone();
    let update = tokio::spawn(async move { update_owner.update_advertisement(metadata).await });
    control.entered.notified().await;
    tokio::time::timeout(Duration::from_millis(300), owner.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        update.await.unwrap(),
        Err(crate::BleError::Attempt(crate::AttemptError::Cancelled))
    ));
    assert!(!driver.complete_native_update().await);
    assert!(driver.served_service().is_none());
    assert!(matches!(
        owner
            .update_advertisement(GattAdvertisement::default())
            .await,
        Err(crate::BleError::NotConnected(_))
    ));
    assert_eq!(control.starts.load(SeqCst), 1);
    assert_eq!(control.stops.load(SeqCst), 1);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_recovery_uses_latest_desired_spec_and_keeps_hidden_owners_hidden() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    let network = MockNetwork::new();
    let (driver, control, owner) =
        BlockingVisibility::metadata(network.clone(), "recovery-metadata");
    owner.set_advertising(true).await.unwrap();
    control.started.notified().await;
    let metadata = GattAdvertisement {
        local_name: Some("Newest recovery".into()),
        manufacturer_data: [(0xFFFF, vec![1, 2, 3, 4, 5])].into(),
        ..Default::default()
    };
    owner.update_advertisement(metadata.clone()).await.unwrap();
    // Model native server loss, not a physical radio action.
    driver.inner.stop_advertising().await.unwrap();
    driver.inner.simulate_radio(crate::RadioStatus::Off);
    driver.inner.simulate_radio(crate::RadioStatus::On);
    let recovered =
        tokio::time::timeout(Duration::from_millis(300), control.started.notified()).await;
    if recovered.is_err() {
        owner.close().await.unwrap();
    }
    recovered.expect("Worker must restore its latest desired metadata after native service loss");
    let spec = driver.served_service().unwrap();
    let readable = GattAdvertisement::decode(
        &spec
            .characteristics
            .iter()
            .find(|v| v.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value,
    )
    .unwrap();
    assert_eq!(readable.local_name, metadata.local_name);
    assert_eq!(readable.manufacturer_data, metadata.manufacturer_data);
    owner.close().await.unwrap();
    let (driver, control, owner) =
        BlockingVisibility::metadata(network.clone(), "recovery-metadata");
    owner.set_advertising(true).await.unwrap();
    control.started.notified().await;
    owner.set_advertising(false).await.unwrap();
    driver.inner.stop_advertising().await.unwrap();
    driver.inner.simulate_radio(crate::RadioStatus::Off);
    driver.inner.simulate_radio(crate::RadioStatus::On);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), control.started.notified())
            .await
            .is_err()
    );
    assert_eq!(control.starts.load(SeqCst), 1);
    owner
        .update_advertisement(GattAdvertisement {
            local_name: Some("Hidden desired".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(driver.served_service().is_none());
    owner.set_advertising(true).await.unwrap();
    let remote = mock_link(&network, "recovery-remote");
    let (left, right) = connect_links(&remote, &owner, "recovery-metadata").await;
    left.send(vec![43]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![43]);
    assert_eq!(control.starts.load(SeqCst), 2);
    remote.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_partial_failures_require_hide_and_full_hidden_retry() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    use std::sync::atomic::Ordering::SeqCst;
    for native_failure in [true, false] {
        let network = MockNetwork::new();
        let (driver, control, owner) = BlockingVisibility::metadata(network.clone(), "partial");
        let reader = MockDriver::new(
            PeerAddress("partial-reader".into()),
            network,
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        );
        owner.set_advertising(true).await.unwrap();
        let mut connection = reader
            .connect(&PeerAddress("partial".into()))
            .await
            .unwrap();
        let before = connection
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap();
        control.native_failure.store(native_failure, SeqCst);
        control.value_failure.store(!native_failure, SeqCst);
        let metadata = GattAdvertisement {
            local_name: Some("Desired after failure".into()),
            ..Default::default()
        };
        assert!(matches!(
            owner.update_advertisement(metadata.clone()).await,
            Err(crate::BleError::Gatt(_))
        ));
        let snapshot = driver.served_service().unwrap();
        let snapshot_value = &snapshot
            .characteristics
            .iter()
            .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value;
        // The readable value is installed before the advertisement is
        // published: a refused value write publishes nothing, and a refused
        // publish leaves the new value readable under the old advertisement.
        assert_eq!(snapshot_value, &before);
        let readable = connection
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap();
        assert_eq!(readable == before, !native_failure);
        let blocked = owner.set_advertising(true).await;
        if !matches!(blocked, Err(crate::BleError::GattBusy(_))) {
            connection.disconnect().await.unwrap();
            owner.close().await.unwrap();
            panic!("partial application must guard show: {blocked:?}");
        }
        assert!(matches!(
            owner.update_advertisement(metadata.clone()).await,
            Err(crate::BleError::GattBusy(_))
        ));
        control.hide_failure.store(true, SeqCst);
        assert!(matches!(
            owner.set_advertising(false).await,
            Err(crate::BleError::Unsupported(_))
        ));
        assert!(matches!(
            owner.set_advertising(true).await,
            Err(crate::BleError::GattBusy(_))
        ));
        control.hide_failure.store(false, SeqCst);
        owner.set_advertising(false).await.unwrap();
        assert!(matches!(
            owner.set_advertising(true).await,
            Err(crate::BleError::GattBusy(_))
        ));
        control.native_failure.store(false, SeqCst);
        control.value_failure.store(false, SeqCst);
        owner.update_advertisement(metadata.clone()).await.unwrap();
        let readable = GattAdvertisement::decode(
            &connection
                .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(readable.local_name, metadata.local_name);
        assert!(reader
            .scan(config().service)
            .await
            .unwrap()
            .next()
            .await
            .is_none());
        // A refused value write stops the update before it reaches the radio.
        let expected_updates = if native_failure { 2 } else { 1 };
        assert_eq!(control.updates.load(SeqCst), expected_updates);
        assert_eq!(control.starts.load(SeqCst), 1);
        assert_eq!(control.stops.load(SeqCst), 0);
        connection.disconnect().await.unwrap();
        owner.set_advertising(true).await.unwrap();
        owner.close().await.unwrap();
    }
}

#[tokio::test]
async fn managed_metadata_stages_before_startup_and_updates_while_hidden() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let driver = Arc::new(MockDriver::new(
        PeerAddress("staged".into()),
        network.clone(),
        caps,
    ));
    let reader = MockDriver::new(PeerAddress("staged-reader".into()), network, caps);
    let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
    let mut metadata = GattAdvertisement {
        local_name: Some("Staged".into()),
        ..Default::default()
    };
    owner.update_advertisement(metadata.clone()).await.unwrap();
    assert!(driver.served_service().is_none());
    let hidden = owner.set_advertising(false).await;
    if hidden.is_err() {
        owner.close().await.unwrap();
    }
    hidden.expect("hiding before startup must stage visibility without a HAL call");
    assert!(driver.served_service().is_none());
    owner.set_advertising(true).await.unwrap();
    let mut connection = reader.connect(&PeerAddress("staged".into())).await.unwrap();
    owner.set_advertising(false).await.unwrap();
    metadata.local_name = Some("Hidden update".into());
    metadata
        .manufacturer_data
        .insert(0xFFFF, vec![1, 9, 8, 7, 6]);
    owner.update_advertisement(metadata.clone()).await.unwrap();
    let bytes = connection
        .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
        .await
        .unwrap();
    let readable = GattAdvertisement::decode(&bytes).unwrap();
    assert_eq!(readable.local_name, metadata.local_name);
    assert_eq!(readable.manufacturer_data, metadata.manufacturer_data);
    assert!(reader
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    for invalid in [
        GattAdvertisement {
            service_data: [(config().service, vec![0; 20])].into(),
            ..Default::default()
        },
        GattAdvertisement {
            manufacturer_data: [(1, vec![0; 253])].into(),
            ..Default::default()
        },
        GattAdvertisement {
            manufacturer_data: [(1, vec![0; 240]), (2, vec![0; 240])].into(),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            owner.update_advertisement(invalid).await,
            Err(crate::BleError::Protocol(_))
        ));
    }
    assert_eq!(
        connection
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await
            .unwrap(),
        bytes
    );
    connection.disconnect().await.unwrap();
    owner.set_advertising(true).await.unwrap();
    let discovered = reader
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(discovered.manufacturer_data, metadata.manufacturer_data);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn managed_metadata_update_preserves_duplex_channel_and_scoped_clones() {
    use crate::advertisement::GattAdvertisement;
    let network = MockNetwork::new();
    let local = mock_link(&network, "metadata-local");
    let (_, control, remote) = BlockingVisibility::metadata(network.clone(), "metadata-remote");
    let (left, right) = connect_links(&local, &remote, "metadata-remote").await;
    let generation = right.state.generation;
    let subscriptions = control
        .lifecycle_subscriptions
        .load(std::sync::atomic::Ordering::SeqCst);
    let maximum = right.max_message_len();
    let retained = right.clone();
    let mut metadata = GattAdvertisement::default();
    metadata
        .manufacturer_data
        .insert(0xFFFF, vec![1, 2, 3, 4, 5]);
    metadata.local_name = Some("Changed".into());
    remote.update_advertisement(metadata.clone()).await.unwrap();
    left.send(vec![17]).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), vec![17]);
    assert_eq!(right.state.generation, generation);
    assert_eq!(right.max_message_len(), maximum);
    right.release().await.unwrap();
    metadata.manufacturer_data.clear();
    metadata
        .service_data
        .insert(ServiceUuid(uuid::Uuid::from_u128(77)), vec![9]);
    metadata.local_name = None;
    remote.update_advertisement(metadata).await.unwrap();
    retained.send(vec![18]).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), vec![18]);
    assert_eq!(control.starts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        control.connects.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(control.stops.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        control
            .lifecycle_subscriptions
            .load(std::sync::atomic::Ordering::SeqCst),
        subscriptions
    );
    retained.release().await.unwrap();
    local.close().await.unwrap();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn managed_startup_preserves_manufacturer_flags_fingerprint_and_local_name() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};

    let network = MockNetwork::new();
    let driver = Arc::new(BlockingVisibility {
        inner: Arc::new(MockDriver::new(
            PeerAddress("managed-advertisement".into()),
            network,
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        entered: tokio::sync::Notify::new(),
        disconnect_blocked: std::sync::atomic::AtomicBool::new(false),
        release_disconnect: tokio::sync::Notify::new(),
        stop_blocked: false,
        metadata: None,
    });
    let mut config = config();
    let service = config.service;
    let manufacturer = vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD];
    config
        .advertised_manufacturer_data
        .insert(0xFFFF, manufacturer.clone());
    config.advertised_local_name = Some("Desktop".into());
    let owner = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config));
    let endpoint = owner.endpoint_id();

    // Exercise Worker startup and real MockDriver validation, not a missing setter.
    let started = owner.set_advertising(true).await;
    let served = driver.inner.served_service();
    owner.close().await.unwrap();
    assert!(
        started.is_ok(),
        "managed startup must accept manufacturer flags, fingerprint and name: {started:?}"
    );

    let spec = served.expect("startup must retain the native service specification");
    spec.validate().unwrap();
    assert_eq!(spec.manufacturer_data.get(&0xFFFF), Some(&manufacturer));
    assert_eq!(spec.local_name, None);
    assert_eq!(spec.fast_advertisement, None, "no fast company configured");
    let characteristic = spec
        .characteristics
        .iter()
        .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
        .expect("the fixed readable advertisement characteristic");
    // The radio carries the 9-byte header naming the readable value, not the value.
    assert_eq!(
        managed_codec::RadioRecord::decode(&spec.service_data[&service]),
        Some(managed_codec::RadioRecord::Header {
            hash: managed_codec::RadioRecord::metadata_hash(&characteristic.initial_value),
            tag: endpoint.as_bytes()[..4].try_into().unwrap(),
        })
    );
    assert!(characteristic.readable);
    assert!(!characteristic.writable);
    assert!(!characteristic.notifiable);
    let readable = GattAdvertisement::decode(&characteristic.initial_value).unwrap();
    assert_eq!(readable.manufacturer_data.get(&0xFFFF), Some(&manufacturer));
    assert_eq!(readable.local_name.as_deref(), Some("Desktop"));
    let header = readable.service_data.get(&service).unwrap();
    assert_eq!(header.len(), 20);
    assert_eq!(&header[..4], &[b'B', b'G', 1, 1]);
    assert_eq!(&header[4..], endpoint.as_bytes());
    assert_eq!(
        header,
        &managed_codec::ManagedMetadata { endpoint }.encode()
    );
    assert_eq!(
        managed_codec::ManagedMetadata::decode(header)
            .unwrap()
            .endpoint,
        endpoint
    );
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
        metadata: None,
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
        metadata: None,
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
        metadata: None,
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
    // A peer without a radio record is read after the scan-response grace.
    tokio::time::sleep(crate::advertisement::SCAN_RESPONSE_GRACE + Duration::from_millis(100))
        .await;
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

// ---- Discovery from radio records (ADR-0012 §2) ----

const FINI_COMPANY: u16 = 0xFFFF;
const FINI_RECORD: [u8; 5] = [0x01, 0xAA, 0xBB, 0xCC, 0xDD];

/// A Fini-shaped config: flags and fingerprint under 0xFFFF, carried in the
/// fast advertisement.
fn fast_config(name: Option<&str>) -> DatagramConfig {
    let mut config = config();
    config
        .advertised_manufacturer_data
        .insert(FINI_COMPANY, FINI_RECORD.to_vec());
    config.advertised_local_name = name.map(Into::into);
    config.fast_advertisement_company = Some(FINI_COMPANY);
    config
}

fn advertiser_with(
    network: &Arc<MockNetwork>,
    address: &str,
    config: DatagramConfig,
) -> Arc<PeerLink> {
    PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress(address.into()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        PeerLinkConfig::new(config),
    )
}

type Discovery = crate::BoxStream<crate::Result<(EndpointId, crate::DiscoveredPeer)>>;

async fn next_item(
    discovery: &mut Discovery,
    within: Duration,
) -> Option<crate::Result<(EndpointId, crate::DiscoveredPeer)>> {
    tokio::time::timeout(within, discovery.next())
        .await
        .expect("discovery answers in time")
}

fn connects(control: &MetadataControl) -> usize {
    control.connects.load(std::sync::atomic::Ordering::SeqCst)
}

async fn first_sighting(driver: &BlockingVisibility) -> crate::DiscoveredPeer {
    driver
        .inner
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn a_fast_advertisement_lists_the_peer_without_a_connection() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-complete", fast_config(None));
    advertiser.set_advertising(true).await.unwrap();
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "fast-complete-reader", fast_config(None));
    for _ in 0..2 {
        let mut discovery = reader.discover().await.unwrap();
        let (endpoint, peer) = next_item(&mut discovery, Duration::from_millis(300))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(endpoint, advertiser.endpoint_id());
        assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
        assert!(
            next_item(&mut discovery, Duration::from_millis(300))
                .await
                .is_none(),
            "complete from the radio: nothing more to report"
        );
    }
    assert_eq!(connects(&control), 0);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn an_incomplete_fast_advertisement_is_listed_at_once_then_read_once() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-named", fast_config(Some("Pixel")));
    advertiser.set_advertising(true).await.unwrap();
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "fast-named-reader", fast_config(None));
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, listed) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(listed.name, None, "no room for the name next to the record");
    assert_eq!(
        listed.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    let (endpoint, named) = next_item(&mut discovery, Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(named.name.as_deref(), Some("Pixel"));
    assert!(next_item(&mut discovery, Duration::from_millis(300))
        .await
        .is_none());
    assert_eq!(connects(&control), 1, "one completion read");
    drop(discovery);
    // A second scan knows this endpoint and hash: no read.
    let mut again = reader.discover().await.unwrap();
    let mut names = Vec::new();
    while let Some(item) = next_item(&mut again, Duration::from_millis(300)).await {
        names.push(item.unwrap().1.name);
    }
    assert_eq!(names.last().cloned().flatten().as_deref(), Some("Pixel"));
    assert_eq!(connects(&control), 1, "the name came from the cache");
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_stalled_completion_read_neither_delays_the_listing_nor_holds_off_a_dial() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-stalled", fast_config(Some("Pixel")));
    let mut incoming = advertiser.incoming().unwrap();
    advertiser.set_advertising(true).await.unwrap();
    let (driver, _control, reader) =
        BlockingVisibility::metadata_with(network, "fast-stalled-reader", fast_config(None));
    // A peer that does not answer: the first connection hangs.
    let address = PeerAddress("fast-stalled".into());
    driver
        .inner
        .stall_connect(&address, Duration::from_secs(30));
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, listed) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(
        listed.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    // Let the completion read start and hang, then dial: the read yields.
    tokio::time::sleep(Duration::from_millis(200)).await;
    driver.inner.stall_connect(&address, Duration::ZERO);
    let started = tokio::time::Instant::now();
    let request = loop {
        match reader.request(&address).await {
            Err(crate::BleError::AdapterBusy(_)) if started.elapsed() < Duration::from_secs(2) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => break other,
        }
    };
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "refused for {:?}",
        started.elapsed()
    );
    let request = request.expect("the dial gets the address");
    let accepted = tokio::spawn(async move { incoming.next().await.unwrap().accept().await });
    let channel = tokio::time::timeout(Duration::from_secs(2), request.accept())
        .await
        .unwrap()
        .unwrap();
    accepted.await.unwrap().unwrap();
    drop(channel);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_header_is_read_once_per_hash_across_scans() {
    use crate::advertisement::GattAdvertisement;
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "header-peer", config());
    advertiser
        .update_advertisement(GattAdvertisement {
            manufacturer_data: [(FINI_COMPANY, FINI_RECORD.to_vec())].into(),
            local_name: Some("Desk".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("header-peer".into()), Some(-50));
    let (_, control, reader) = BlockingVisibility::metadata(network.clone(), "header-reader");
    async fn scan(reader: &PeerLink) -> Vec<(EndpointId, crate::DiscoveredPeer)> {
        let mut discovery = reader.discover().await.unwrap();
        let mut found = Vec::new();
        while let Some(item) = next_item(&mut discovery, Duration::from_secs(2)).await {
            found.push(item.unwrap());
        }
        found
    }
    let first = scan(&reader).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, advertiser.endpoint_id());
    assert_eq!(first[0].1.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1);
    let second = scan(&reader).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].1.name.as_deref(), Some("Desk"));
    assert_eq!(
        connects(&control),
        1,
        "an unchanged header is not read again"
    );
    // The Add Device flag changes: a new hash, one read.
    advertiser
        .update_advertisement(GattAdvertisement {
            manufacturer_data: [(FINI_COMPANY, vec![0x00, 0xAA, 0xBB, 0xCC, 0xDD])].into(),
            local_name: Some("Desk".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let third = scan(&reader).await;
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].1.manufacturer_data[&FINI_COMPANY][0], 0x00);
    assert_eq!(connects(&control), 2);
    // A stale platform entry (no signal strength) is not a sighting.
    network.set_rssi(&PeerAddress("header-peer".into()), None);
    assert!(scan(&reader).await.is_empty());
    assert_eq!(connects(&control), 2);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_peer_without_a_record_is_read_once_then_reported_from_that_read() {
    let network = MockNetwork::new();
    let address = PeerAddress("unnamed-peer".into());
    network.set_radio_capacity(&address, crate::drivers::mock::RadioCapacity::None);
    let advertiser = advertiser_with(&network, "unnamed-peer", fast_config(Some("Linux")));
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&address, Some(-50));
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "unnamed-reader", fast_config(None));
    for scan in 1..=2 {
        let started = tokio::time::Instant::now();
        let mut discovery = reader.discover().await.unwrap();
        let (endpoint, peer) = next_item(&mut discovery, Duration::from_secs(3))
            .await
            .unwrap()
            .unwrap();
        if scan == 1 {
            assert!(started.elapsed() >= crate::advertisement::SCAN_RESPONSE_GRACE);
        } else {
            assert!(
                started.elapsed() < crate::advertisement::SCAN_RESPONSE_GRACE,
                "the address is known: reported from the last read"
            );
        }
        assert_eq!(endpoint, advertiser.endpoint_id());
        assert_eq!(peer.name.as_deref(), Some("Linux"));
        assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
        assert_eq!(
            connects(&control),
            1,
            "one read; the next waits KNOWN_PEER_REFRESH"
        );
        drop(discovery);
    }
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn failed_header_reads_back_off_and_are_reported_once() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "backoff-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("backoff-peer".into()), Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "backoff-reader");
    let sighting = first_sighting(&driver).await;
    // Gone before the read: every connect fails.
    advertiser.close().await.unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    input.send(Ok(sighting)).await.unwrap();
    let started = tokio::time::Instant::now();
    let mut errors = 0;
    while let Ok(item) =
        tokio::time::timeout_at(started + Duration::from_millis(3600), discovery.next()).await
    {
        match item {
            Some(Err(_)) => errors += 1,
            Some(Ok(item)) => panic!("nothing to read, yet {item:?}"),
            None => break,
        }
    }
    // Reads at 0 s, 1 s and 3 s; the next one waits until 7 s.
    assert_eq!(connects(&control), 3);
    assert_eq!(errors, 1, "the failure is reported once");
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
}

#[tokio::test]
async fn a_rotated_address_with_the_same_header_is_not_read_again() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "rotating-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("rotating-peer".into()), Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "rotation-reader");
    let mut sighting = first_sighting(&driver).await;
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, _) = next_item(&mut discovery, Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(connects(&control), 1);
    sighting.address = PeerAddress("rotated-address".into());
    input.send(Ok(sighting)).await.unwrap();
    let (endpoint, peer) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(peer.address, PeerAddress("rotated-address".into()));
    assert_eq!(
        connects(&control),
        1,
        "same tag and hash: the cache answers"
    );
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn an_advertisement_update_changes_the_header_hash_with_the_readable_fields() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    let network = MockNetwork::new();
    let (driver, _, owner) =
        BlockingVisibility::metadata_with(network, "update-header", fast_config(Some("Pixel")));
    owner.set_advertising(true).await.unwrap();
    fn readable(spec: &crate::GattServiceSpec) -> Vec<u8> {
        spec.characteristics
            .iter()
            .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value
            .clone()
    }
    fn header(spec: &crate::GattServiceSpec) -> managed_codec::RadioRecord {
        managed_codec::RadioRecord::decode(&spec.service_data[&spec.uuid]).unwrap()
    }
    let before = driver.served_service().unwrap();
    let flagged = GattAdvertisement {
        manufacturer_data: [(FINI_COMPANY, vec![0x00, 0xAA, 0xBB, 0xCC, 0xDD])].into(),
        local_name: Some("Pixel".into()),
        ..Default::default()
    };
    owner.update_advertisement(flagged.clone()).await.unwrap();
    let after = driver.served_service().unwrap();
    assert_ne!(header(&before), header(&after));
    let value = readable(&after);
    assert_eq!(
        header(&after),
        managed_codec::RadioRecord::Header {
            hash: managed_codec::RadioRecord::metadata_hash(&value),
            tag: owner.endpoint_id().as_bytes()[..4].try_into().unwrap(),
        }
    );
    let mut expected = flagged;
    expected.service_data.insert(
        config().service,
        managed_codec::ManagedMetadata {
            endpoint: owner.endpoint_id(),
        }
        .encode(),
    );
    assert_eq!(GattAdvertisement::decode(&value).unwrap(), expected);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn the_folded_manufacturer_data_reaches_the_scanner_as_it_was_set() {
    let network = MockNetwork::new();
    let mut advertised = fast_config(None);
    advertised
        .advertised_manufacturer_data
        .insert(0x0001, vec![9]);
    let advertiser = advertiser_with(&network, "folding-peer", advertised);
    advertiser.set_advertising(true).await.unwrap();
    let (driver, control, reader) =
        BlockingVisibility::metadata_with(network.clone(), "folding-reader", fast_config(None));
    // On the radio the 0xFFFF entry is the fast record, not the caller's 5 bytes.
    let radio = first_sighting(&driver).await;
    assert_eq!(radio.manufacturer_data[&FINI_COMPANY][0], 0xFC);
    assert_eq!(radio.manufacturer_data[&FINI_COMPANY].len(), 26);
    let mut discovery = reader.discover().await.unwrap();
    let (_, peer) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
    assert_eq!(peer.manufacturer_data[&0x0001], vec![9]);
    assert_eq!(
        peer.service_data[&config().service],
        managed_codec::ManagedMetadata {
            endpoint: advertiser.endpoint_id()
        }
        .encode()
    );
    assert_eq!(connects(&control), 0);
    drop(discovery);
    // A scanner that does not know the company reads instead, and still gets the 5 bytes.
    let blind = mock_link(&network, "folding-blind");
    let mut discovery = blind.discover().await.unwrap();
    let (_, peer) = next_item(&mut discovery, Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
    drop(discovery);
    blind.close().await.unwrap();
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_publisher_at_its_own_address_joins_the_provider_read_once() {
    // A Windows peer: its provider's advertisement has the service and no record,
    // and a publisher at another address sends the fast advertisement.
    let network = MockNetwork::new();
    let provider = PeerAddress("windows-provider".into());
    network.set_radio_capacity(&provider, crate::drivers::mock::RadioCapacity::None);
    let (windows, _, advertiser) = BlockingVisibility::metadata_with(
        network.clone(),
        "windows-provider",
        fast_config(Some("Desk")),
    );
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&provider, Some(-50));
    let fast = windows
        .served_service()
        .unwrap()
        .fast_advertisement
        .unwrap();
    let publisher = crate::DiscoveredPeer {
        address: PeerAddress("windows-publisher".into()),
        name: None,
        services: Vec::new(),
        manufacturer_data: [(fast.company_id, fast.data)].into(),
        service_data: Default::default(),
        rssi: Some(-50),
    };
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "publisher-reader", fast_config(None));
    let (input, receiver) = tokio::sync::mpsc::channel(4);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    // Before any read the publisher's address is not reported: it is not connectable.
    input.send(Ok(publisher.clone())).await.unwrap();
    let (endpoint, read) = next_item(&mut discovery, Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(read.address, provider);
    assert_eq!(read.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1, "the provider is read once");
    // Now the publisher's record is reported at the provider's address, from the cache.
    input.send(Ok(publisher)).await.unwrap();
    let (endpoint, joined) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(joined.address, provider);
    assert_eq!(joined.name.as_deref(), Some("Desk"));
    assert_eq!(
        joined.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    drop(input);
    drop(discovery);
    // The next scan: the provider's address is answered from the publisher's key.
    let mut again = reader.discover().await.unwrap();
    let (endpoint, cached) = next_item(&mut again, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(cached.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1, "no second read");
    drop(again);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}
