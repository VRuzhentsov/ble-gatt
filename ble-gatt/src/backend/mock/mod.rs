//! In-process mock backend: no radio, no OS Bluetooth stack. Two
//! `MockBackend`s that share the same `MockNetwork` can scan/connect/serve
//! against each other, exercising the real `Backend`/`GattConnection` trait
//! contract end-to-end. Mirrors Fini's `transport::sim` adapter — a
//! first-class stand-in for CI-safe protocol tests, not a mock of one.
//!
//! Behind the `mock-broker` feature, the same simulation can also run as a
//! broker process that separate OS processes connect to over a socket —
//! see `docs/adr/0004-mock-broker-for-cross-process-e2e.md`. The default
//! (feature off) build is exactly today's in-process-only behavior, with
//! zero new dependencies.

mod local;
#[cfg(feature = "mock-broker")]
mod broker;
#[cfg(feature = "mock-broker")]
mod remote;
#[cfg(feature = "mock-broker")]
mod wire;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    ServiceUuid, WriteType,
};
use local::LocalRadio;

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// ATT MTU the mock reports for every connection. Deliberately *not*
/// `DEFAULT_ATT_MTU`: a realistic negotiated value, so tests that chunk
/// against `max_write_len()` exercise a non-trivial size rather than
/// accidentally passing because everything fit in one write.
const MOCK_ATT_MTU: u16 = 247;

enum Radio {
    Local(LocalRadio),
    #[cfg(feature = "mock-broker")]
    Remote(remote::RemoteClient),
}

/// Shared "radio" for a set of `MockBackend`s. Construct one per test and
/// hand an `Arc` clone to each simulated peer — there is no global registry,
/// so unrelated tests never see each other's peers.
///
/// `MockNetwork::new()` is always in-process (`Local`), exactly as before —
/// this type gaining an internal `Remote` variant behind `mock-broker` is
/// not observable from any existing caller: no test or downstream code
/// reaches into its fields, only its methods.
pub struct MockNetwork(Radio);

impl Default for MockNetwork {
    fn default() -> Self {
        Self(Radio::Local(LocalRadio::default()))
    }
}

impl MockNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Connect to a `MockNetwork::serve()` broker over TCP, so `MockBackend`s
    /// built against the returned network can bridge two separate OS
    /// processes instead of sharing this `Arc` in one. See
    /// docs/adr/0004-mock-broker-for-cross-process-e2e.md.
    #[cfg(feature = "mock-broker")]
    pub async fn remote(endpoint: impl tokio::net::ToSocketAddrs) -> Result<Arc<Self>> {
        let client = remote::RemoteClient::dial(endpoint).await?;
        Ok(Arc::new(Self(Radio::Remote(client))))
    }

    /// Run the broker loop against `listener` until it errors — never
    /// returns `Ok` on its own. Its business logic is a single
    /// `LocalRadio`, unmodified: every request handler calls the exact same
    /// methods `Radio::Local` dispatches to, so wire parity is structural
    /// rather than separately maintained. See docs/adr/0004.
    #[cfg(feature = "mock-broker")]
    pub async fn serve(listener: tokio::net::TcpListener) -> Result<()> {
        broker::serve(listener).await
    }

    /// Fault-injection and inspection methods below require a `Local`
    /// network — none of Part 1's remote wire protocol carries them (see
    /// docs/adr/0004). Calling one against a `MockNetwork::remote(..)`
    /// connection is a test-authoring bug, not a runtime condition to
    /// recover from, so this panics with a clear message rather than
    /// silently no-op-ing (a silent no-op here could produce a false test
    /// pass — a test believing it armed a fault that never fires).
    fn as_local(&self) -> &LocalRadio {
        match &self.0 {
            Radio::Local(r) => r,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(_) => panic!(
                "fault injection / inspection methods on MockNetwork require MockNetwork::new(), \
                 not a MockNetwork::remote() broker connection — not supported in Part 1, see \
                 docs/adr/0004-mock-broker-for-cross-process-e2e.md"
            ),
        }
    }

    /// Peers this network has seen an explicit `disconnect()` for.
    pub fn disconnected_peers(&self) -> Vec<PeerAddress> {
        self.as_local().disconnected_peers()
    }

    /// Emit a central-role loss for a *specific* session.
    pub fn simulate_loss_for_session(&self, central: &PeerAddress, peer: &PeerAddress, session: u64) {
        self.as_local().simulate_loss_for_session(central, peer, session)
    }

    /// The lifecycle event stream dropped `dropped` events before a
    /// subscriber could read them.
    pub fn simulate_event_lag(&self, to: &PeerAddress, dropped: u64) {
        self.as_local().simulate_event_lag(to, dropped)
    }

    /// The backend reports that it dropped notifications for `central`.
    pub fn simulate_notification_gap(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        self.as_local().simulate_notification_gap(peripheral, central)
    }

    /// A central drops its notify subscription without disconnecting.
    pub fn simulate_unsubscribe(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        self.as_local().simulate_unsubscribe(peripheral, central)
    }

    /// Delay every `subscribe` on this network, so a test can cancel
    /// `datagram::connect` while setup is genuinely in flight.
    pub fn stall_subscribe(&self, delay: std::time::Duration) {
        self.as_local().stall_subscribe(delay)
    }

    /// Make the next `scan` on any backend in this network end with an
    /// error item, the way Android's `onScanFailed` does.
    pub fn arm_scan_failure(&self, message: impl Into<String>) {
        self.as_local().arm_scan_failure(message)
    }

    /// Attach advertisement payload to an already-advertising peer, so
    /// scanners see it in `DiscoveredPeer`.
    pub fn set_advertisement_data(
        &self, peer: &PeerAddress, manufacturer_data: BTreeMap<u16, Vec<u8>>,
        service_data: BTreeMap<ServiceUuid, Vec<u8>>, rssi: Option<i16>,
    ) {
        self.as_local().set_advertisement_data(peer, manufacturer_data, service_data, rssi)
    }

    // --- Dispatch to Local or Remote, identically ---

    fn register_events_sender(&self, address: PeerAddress, sender: broadcast::Sender<GattEvent>) {
        match &self.0 {
            Radio::Local(r) => r.register_events_sender(address, sender),
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.register_events_sender(address, sender),
        }
    }

    async fn scan(&self, requester: &PeerAddress, service: ServiceUuid) -> Result<(Vec<DiscoveredPeer>, Option<String>)> {
        match &self.0 {
            Radio::Local(r) => r.scan(requester, service).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.scan(requester, service).await,
        }
    }

    async fn connect(&self, central: &PeerAddress, peer: &PeerAddress) -> Result<u64> {
        match &self.0 {
            Radio::Local(r) => r.connect(central, peer).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.connect(central, peer).await,
        }
    }

    async fn advertise(&self, address: &PeerAddress, service: GattServiceSpec) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.advertise(address, service).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.advertise(address, service).await,
        }
    }

    async fn stop_advertising(&self, address: &PeerAddress) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.stop_advertising(address).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.stop_advertising(address).await,
        }
    }

    async fn notify(&self, address: &PeerAddress, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.notify(address, characteristic, value).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.notify(address, characteristic, value).await,
        }
    }

    async fn notify_peer(
        &self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>,
        characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.notify_peer(address, peer, session, characteristic, value).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.notify_peer(address, peer, session, characteristic, value).await,
        }
    }

    async fn disconnect_peer(&self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.disconnect_peer(address, peer, session).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.disconnect_peer(address, peer, session).await,
        }
    }

    async fn read(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<Vec<u8>> {
        match &self.0 {
            Radio::Local(r) => r.read(session, central, peripheral, characteristic).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.read(session, central, peripheral, characteristic).await,
        }
    }

    async fn write_with_type(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
        value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.write_with_type(session, central, peripheral, characteristic, value, write_type).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.write_with_type(session, central, peripheral, characteristic, value, write_type).await,
        }
    }

    async fn subscribe(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        match &self.0 {
            Radio::Local(r) => r.subscribe(session, central, peripheral, characteristic).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.subscribe(session, central, peripheral, characteristic).await,
        }
    }

    async fn disconnect(&self, session: u64, central: &PeerAddress, peripheral: &PeerAddress) -> Result<()> {
        match &self.0 {
            Radio::Local(r) => r.disconnect(session, central, peripheral).await,
            #[cfg(feature = "mock-broker")]
            Radio::Remote(r) => r.disconnect(session, central, peripheral).await,
        }
    }
}

pub struct MockBackend {
    address: PeerAddress,
    network: Arc<MockNetwork>,
    capabilities: CapabilityReport,
    events_tx: broadcast::Sender<GattEvent>,
}

impl MockBackend {
    /// `capabilities` lets tests simulate a peer that can't do peripheral
    /// mode (e.g. most Android devices) without a second backend impl — see
    /// the plan's role-assignment-with-capability-fallback decision.
    pub fn new(address: PeerAddress, network: Arc<MockNetwork>, capabilities: CapabilityReport) -> Self {
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        network.register_events_sender(address.clone(), events_tx.clone());
        Self { address, network, capabilities, events_tx }
    }

    /// Simulate the peer dropping the link without warning — out of range,
    /// battery dead, firmware crash. There is no API-initiated disconnect
    /// involved, which is exactly the case `Backend::events()` exists to
    /// surface; tests use this to prove a consumer learns about it.
    pub fn simulate_peer_loss(&self, peer: &PeerAddress) {
        self.network.as_local().simulate_peer_loss(&self.address, peer);
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn capabilities(&self) -> CapabilityReport {
        self.capabilities
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let (matches, armed_failure) = self.network.scan(&self.address, service).await?;
        // Mirrors the real backends' asynchronous-failure contract: an
        // armed failure is delivered as an error *item*, after any peers
        // already matched, rather than as an error from `scan` itself.
        if let Some(message) = armed_failure {
            let items = matches.into_iter().map(Ok).chain(std::iter::once(Err(BleError::Gatt(message))));
            return Ok(Box::pin(tokio_stream::iter(items)));
        }
        Ok(Box::pin(tokio_stream::iter(matches.into_iter().map(Ok))))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let session = self.network.connect(&self.address, peer).await?;
        Ok(Box::new(MockGattConnection {
            session,
            central: self.address.clone(),
            peripheral: peer.clone(),
            network: self.network.clone(),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        if !self.capabilities.peripheral {
            return Err(BleError::PeripheralUnsupported);
        }
        self.network.advertise(&self.address, service).await
    }

    async fn stop_advertising(&self) -> Result<()> {
        self.network.stop_advertising(&self.address).await
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.network.notify(&self.address, characteristic, value).await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        self.network.disconnect_peer(&self.address, peer, session).await
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        self.network.notify_peer(&self.address, peer, session, characteristic, value).await
    }

    fn events(&self) -> BoxStream<GattEvent> {
        // This backend's own view, in both roles — available immediately,
        // not conditional on advertising, since a central-only consumer
        // needs it to learn about unsolicited peer loss. Purely local to
        // this backend regardless of Local/Remote: `register_events_sender`
        // is what wires the network side up to feed this channel.
        let rx = self.events_tx.subscribe();
        Box::pin(BroadcastStream::new(rx).map(|item| match item {
            Ok(event) => event,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => GattEvent::Lagged { dropped: n },
        }))
    }
}

struct MockGattConnection {
    session: u64,
    central: PeerAddress,
    peripheral: PeerAddress,
    network: Arc<MockNetwork>,
}

#[async_trait]
impl GattConnection for MockGattConnection {
    fn peer(&self) -> PeerAddress {
        self.peripheral.clone()
    }

    fn session(&self) -> Option<u64> {
        Some(self.session)
    }

    fn att_mtu(&self) -> u16 {
        MOCK_ATT_MTU
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        self.network.read(self.session, &self.central, &self.peripheral, characteristic).await
    }

    async fn write_with_type(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType) -> Result<()> {
        self.network
            .write_with_type(self.session, &self.central, &self.peripheral, characteristic, value, write_type)
            .await
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.network.subscribe(self.session, &self.central, &self.peripheral, characteristic).await
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.network.disconnect(self.session, &self.central, &self.peripheral).await
    }
}
