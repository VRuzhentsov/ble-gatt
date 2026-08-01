//! In-process mock backend: no radio, no OS Bluetooth stack. Two
//! `MockBackend`s that share the same `MockNetwork` can scan/connect/serve
//! against each other, exercising the real `Backend`/`GattConnection` trait
//! contract end-to-end. Mirrors Fini's `transport::sim` adapter — a
//! first-class stand-in for CI-safe protocol tests, not a mock of one.
//!
//! Built entirely on `tokio::sync::{broadcast, Mutex}` rather than
//! hand-rolled pub-sub plumbing.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, UnboundedReceiverStream};
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::GattCharacteristicSpec;
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// ATT MTU the mock reports for every connection. Deliberately *not*
/// `DEFAULT_ATT_MTU`: a realistic negotiated value, so tests that chunk
/// against `max_write_len()` exercise a non-trivial size rather than
/// accidentally passing because everything fit in one write.
const MOCK_ATT_MTU: u16 = 247;

/// Live notify sessions for one characteristic, keyed by subscriber.
type NotifySessions = HashMap<PeerAddress, mpsc::UnboundedSender<Result<Vec<u8>>>>;

struct PeripheralState {
    service: GattServiceSpec,
    values: HashMap<CharacteristicUuid, Vec<u8>>,
    /// One sender per *subscriber*, not one per characteristic. Modelling
    /// notify as a single broadcast made the cross-peer leak that
    /// `disconnect_peer` prevents impossible to express, let alone test.
    subscribers: HashMap<CharacteristicUuid, NotifySessions>,
    manufacturer_data: BTreeMap<u16, Vec<u8>>,
    service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    rssi: Option<i16>,
}

/// Shared "radio" for a set of `MockBackend`s. Construct one per test and
/// hand an `Arc` clone to each simulated peer — there is no global registry,
/// so unrelated tests never see each other's peers.
#[derive(Default)]
pub struct MockNetwork {
    peripherals: Mutex<HashMap<PeerAddress, PeripheralState>>,
    /// Every backend's own event sender, keyed by its address, so an event
    /// can be delivered to the party that actually observes it — a
    /// peripheral learns that a central connected to *it*, and a central
    /// learns that its peer vanished. Mirrors how the real backends behave:
    /// `events()` is this backend's view, not a global feed.
    event_senders: Mutex<HashMap<PeerAddress, broadcast::Sender<GattEvent>>>,
    /// Every `GattConnection::disconnect` this network has seen, so a test
    /// can assert that an abandoned setup actually released the platform
    /// connection rather than merely dropping the Rust handle.
    disconnect_log: Mutex<Vec<PeerAddress>>,
    subscribe_delay: Mutex<Option<std::time::Duration>>,
    /// Armed by `arm_scan_failure`, consumed by the next `scan`. Exists so
    /// the asynchronous scan-failure path — the one that makes "Bluetooth is
    /// off" distinguishable from "no peers nearby" — is reachable in tests
    /// without a radio.
    armed_scan_failure: Mutex<Option<String>>,
    /// Session ids handed to connections, so the mock reproduces the real
    /// backends' ability to distinguish successive links to one address.
    next_session: std::sync::atomic::AtomicU64,
    /// Newest session per (central, peripheral) pair, so the mock can
    /// reproduce a stale handle being superseded.
    live_sessions: Mutex<HashMap<(PeerAddress, PeerAddress), u64>>,
}

impl MockNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Peers this network has seen an explicit `disconnect()` for.
    pub fn disconnected_peers(&self) -> Vec<PeerAddress> {
        self.disconnect_log.lock().unwrap().clone()
    }

    /// Emit a central-role loss for a *specific* session.
    ///
    /// Lets a test replay a disconnect belonging to a connection that has
    /// already been replaced — the case where address-only matching tears
    /// down the link that succeeded it.
    pub fn simulate_loss_for_session(&self, central: &PeerAddress, peer: &PeerAddress, session: u64) {
        self.emit(
            central,
            GattEvent::Disconnected {
                peer: peer.clone(),
                local_role: Role::Central,
                session: Some(session),
            },
        );
    }

    /// The lifecycle event stream dropped `dropped` events before a
    /// subscriber could read them.
    ///
    /// Exists so the consequence is testable: among the lost events may be a
    /// served peer's `Disconnected`, and no later event need ever replace it.
    pub fn simulate_event_lag(&self, to: &PeerAddress, dropped: u64) {
        self.emit(to, GattEvent::Lagged { dropped });
    }

    /// The backend reports that it dropped notifications for `central` —
    /// Android's bounded notify queue overflowing, which the peer was
    /// already told had been delivered.
    ///
    /// Exists so the receiver-side consequence is testable without a radio:
    /// the fragment that went missing may be the one a pending message
    /// needed, in which case nothing further will ever arrive to wake a
    /// receiver parked in `recv()`.
    pub fn simulate_notification_gap(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        let peripherals = self.peripherals.lock().unwrap();
        if let Some(state) = peripherals.get(peripheral) {
            for peers in state.subscribers.values() {
                if let Some(tx) = peers.get(central) {
                    let _ = tx.send(Err(BleError::Gatt(
                        "notification queue overflowed: payloads were dropped".to_string(),
                    )));
                }
            }
        }
    }

    /// A central drops its notify subscription without disconnecting.
    ///
    /// Both real backends treat this as the end of the served session — a
    /// peer that cannot be reached by notify is gone as far as the server is
    /// concerned, even with the link still up — so the mock must too, or the
    /// lockout it causes is untestable here.
    pub fn simulate_unsubscribe(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        self.drop_subscriptions(peripheral, central);
        self.emit(
            peripheral,
            GattEvent::Disconnected {
                peer: central.clone(),
                local_role: Role::Peripheral,
                session: None,
            },
        );
    }

    /// Delay every `subscribe` on this network, so a test can cancel
    /// `datagram::connect` while setup is genuinely in flight.
    pub fn stall_subscribe(&self, delay: std::time::Duration) {
        *self.subscribe_delay.lock().unwrap() = Some(delay);
    }

    /// Make the next `scan` on any backend in this network end with an
    /// error item, the way Android's `onScanFailed` does.
    pub fn arm_scan_failure(&self, message: impl Into<String>) {
        *self.armed_scan_failure.lock().unwrap() = Some(message.into());
    }

    fn take_armed_scan_failure(&self) -> Option<String> {
        self.armed_scan_failure.lock().unwrap().take()
    }

    /// Forget every notify subscription `central` holds on `peripheral`.
    /// Without this the mock would keep delivering notifications to a peer
    /// the server has disconnected — the very leak `disconnect_peer` exists
    /// to close, papered over in the one place it can be tested.
    fn drop_subscriptions(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        if let Some(state) = self.peripherals.lock().unwrap().get_mut(peripheral) {
            for peers in state.subscribers.values_mut() {
                peers.remove(central);
            }
        }
    }

    fn emit(&self, to: &PeerAddress, event: GattEvent) {
        if let Some(tx) = self.event_senders.lock().unwrap().get(to) {
            let _ = tx.send(event);
        }
    }

    /// Attach advertisement payload to an already-advertising peer, so
    /// scanners see it in `DiscoveredPeer`. Out-of-band because a real
    /// advertisement carries this alongside — not inside — the GATT service
    /// definition.
    pub fn set_advertisement_data(
        &self, peer: &PeerAddress, manufacturer_data: BTreeMap<u16, Vec<u8>>,
        service_data: BTreeMap<ServiceUuid, Vec<u8>>, rssi: Option<i16>,
    ) {
        if let Some(state) = self.peripherals.lock().unwrap().get_mut(peer) {
            state.manufacturer_data = manufacturer_data;
            state.service_data = service_data;
            state.rssi = rssi;
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
        network
            .event_senders
            .lock()
            .unwrap()
            .insert(address.clone(), events_tx.clone());
        Self {
            address,
            network,
            capabilities,
            events_tx,
        }
    }

    /// Simulate the peer dropping the link without warning — out of range,
    /// battery dead, firmware crash. There is no API-initiated disconnect
    /// involved, which is exactly the case `Backend::events()` exists to
    /// surface; tests use this to prove a consumer learns about it.
    pub fn simulate_peer_loss(&self, peer: &PeerAddress) {
        // Clear the state as well as announcing it. Emitting events alone
        // left the central's handle passing `ensure_current` afterwards, so
        // reads, writes and subscribes still succeeded across a link this
        // method exists to say is gone — and the peripheral kept a notify
        // route to it. That is the mock accepting what both real backends
        // reject, which is how a divergence hides a bug rather than
        // surfacing it.
        self.network
            .live_sessions
            .lock()
            .unwrap()
            .remove(&(peer.clone(), self.address.clone()));
        self.network.drop_subscriptions(&self.address, peer);
        self.network.emit(
            &self.address,
            GattEvent::Disconnected {
                peer: peer.clone(),
                local_role: Role::Peripheral,
                session: None,
            },
        );
        self.network.emit(
            peer,
            GattEvent::Disconnected {
                peer: self.address.clone(),
                local_role: Role::Central,
                session: None,
            },
        );
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn capabilities(&self) -> CapabilityReport {
        self.capabilities
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let matches: Vec<DiscoveredPeer> = peripherals
            .iter()
            .filter(|(addr, state)| **addr != self.address && state.service.uuid == service)
            .map(|(addr, state)| DiscoveredPeer {
                address: addr.clone(),
                name: None,
                services: vec![state.service.uuid],
                // The mock has no real advertisement packet to parse. Tests
                // that care about identity-in-advertisement drive it through
                // `MockNetwork::set_advertisement_data`.
                manufacturer_data: state.manufacturer_data.clone(),
                service_data: state.service_data.clone(),
                rssi: state.rssi,
            })
            .collect();
        drop(peripherals);
        // Mirrors the real backends' asynchronous-failure contract: an
        // armed failure is delivered as an error *item*, after any peers
        // already matched, rather than as an error from `scan` itself.
        if let Some(message) = self.network.take_armed_scan_failure() {
            let items = matches
                .into_iter()
                .map(Ok)
                .chain(std::iter::once(Err(BleError::Gatt(message))));
            return Ok(Box::pin(tokio_stream::iter(items)));
        }
        Ok(Box::pin(tokio_stream::iter(matches.into_iter().map(Ok))))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        {
            let peripherals = self.network.peripherals.lock().unwrap();
            peripherals.get(peer).ok_or_else(|| BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "peer is not advertising".to_string(),
            })?;
        }
        // Only the central's own view is emitted here. The *peripheral*
        // learns about this peer from `subscribe`, matching both real
        // backends: announcing at connection time would let `serve` yield a
        // channel before the notify path exists, so a server that greets
        // immediately would fail with "no live notify session".
        self.network.emit(
            &self.address,
            GattEvent::Connected {
                peer: peer.clone(),
                local_role: Role::Central,
                session: None,
            },
        );
        let session = self
            .network
            .next_session
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.network
            .live_sessions
            .lock()
            .unwrap()
            .insert((self.address.clone(), peer.clone()), session);
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
        let mut values = HashMap::new();
        let mut subscribers = HashMap::new();
        for characteristic in &service.characteristics {
            values.insert(characteristic.uuid, characteristic.initial_value.clone());
            subscribers.insert(characteristic.uuid, HashMap::new());
        }
        let mut peripherals = self.network.peripherals.lock().unwrap();
        let previous = peripherals.remove(&self.address);
        peripherals.insert(
            self.address.clone(),
            PeripheralState {
                service,
                values,
                subscribers,
                // Advertisement payload survives a re-advertise: it's set
                // out-of-band by the test, not part of GattServiceSpec.
                manufacturer_data: previous
                    .as_ref()
                    .map(|p| p.manufacturer_data.clone())
                    .unwrap_or_default(),
                service_data: previous
                    .as_ref()
                    .map(|p| p.service_data.clone())
                    .unwrap_or_default(),
                rssi: previous.as_ref().and_then(|p| p.rssi),
            },
        );
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        // Announce every subscribed central as gone before tearing the
        // server down. Both real backends now do this — neither platform
        // delivers a disconnect callback when the *server* goes away, so
        // without it `datagram::serve` keeps serving peers that no longer
        // have a server to talk to, and locks out the next `serve`.
        let served: Vec<PeerAddress> = {
            let peripherals = self.network.peripherals.lock().unwrap();
            match peripherals.get(&self.address) {
                Some(state) => {
                    let unique: HashSet<PeerAddress> = state
                        .subscribers
                        .values()
                        .flat_map(|peers| peers.keys().cloned())
                        .collect();
                    unique.into_iter().collect()
                }
                None => Vec::new(),
            }
        };
        self.network.peripherals.lock().unwrap().remove(&self.address);
        for peer in served {
            self.network.emit(
                &self.address,
                GattEvent::Disconnected {
                    peer: peer.clone(),
                    local_role: Role::Peripheral,
                    session: None,
                },
            );
            self.network.emit(
                &peer,
                GattEvent::Disconnected {
                    peer: self.address.clone(),
                    local_role: Role::Central,
                    session: None,
                },
            );
        }
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.address)
            .ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let peers = state
            .subscribers
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        // Faithful to both real backends: notify is a broadcast, so every
        // subscriber gets it — including a central the protocol layer
        // believes it refused. That is the whole hazard.
        let mut delivered = false;
        for tx in peers.values() {
            if tx.send(Ok(value.clone())).is_ok() {
                delivered = true;
            }
        }
        // Reaching nobody is an error, not success. Both real backends now
        // report it: a reliable `send` that claims success while dropping
        // the payload is the worst failure mode this API can have.
        if !delivered {
            return Err(BleError::Gatt("notify reached no subscriber".to_string()));
        }
        Ok(())
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        // Mirrors the real backends: a stale caller must not drop the
        // session that replaced the one it meant.
        if let Some(session) = session {
            let live = self
                .network
                .live_sessions
                .lock()
                .unwrap()
                .get(&(peer.clone(), self.address.clone()))
                .copied();
            if live != Some(session) {
                return Ok(());
            }
        }
        // Mirrors the real backends: the refused central learns it was
        // dropped, and the server sees the peripheral-role disconnect that
        // clears its single-central slot.
        self.network.emit(
            peer,
            GattEvent::Disconnected {
                peer: self.address.clone(),
                local_role: Role::Central,
                session: None,
            },
        );
        self.network.emit(
            &self.address,
            GattEvent::Disconnected {
                peer: peer.clone(),
                local_role: Role::Peripheral,
                session: None,
            },
        );
        self.network.drop_subscriptions(&self.address, peer);
        // End the session too. Announcing a disconnect while leaving
        // `live_sessions` intact let the central's retained handle keep
        // reading, writing and subscribing after the server said it had
        // dropped it — the mock accepting what both real backends reject.
        self.network
            .live_sessions
            .lock()
            .unwrap()
            .remove(&(peer.clone(), self.address.clone()));
        Ok(())
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        if let Some(session) = session {
            let live = self
                .network
                .live_sessions
                .lock()
                .unwrap()
                .get(&(peer.clone(), self.address.clone()))
                .copied();
            if live != Some(session) {
                return Err(BleError::NotConnected(peer.0.clone()));
            }
        }
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.address)
            .ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let peers = state
            .subscribers
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let tx = peers.get(peer).ok_or_else(|| {
            BleError::Gatt(format!("{} has no live notify session", peer.0))
        })?;
        tx.send(Ok(value))
            .map_err(|_| BleError::Gatt(format!("{} has no live notify session", peer.0)))
    }

    fn events(&self) -> BoxStream<GattEvent> {
        // This backend's own view, in both roles — available immediately,
        // not conditional on advertising, since a central-only consumer
        // needs it to learn about unsolicited peer loss.
        let rx = self.events_tx.subscribe();
        Box::pin(BroadcastStream::new(rx).map(|item| match item {
            Ok(event) => event,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                GattEvent::Lagged { dropped: n }
            }
        }))
    }
}

struct MockGattConnection {
    session: u64,
    central: PeerAddress,
    peripheral: PeerAddress,
    network: Arc<MockNetwork>,
}

impl MockGattConnection {
    /// Reject a characteristic the peripheral never advertised with the
    /// property this operation needs.
    ///
    /// Linux registers no handler and Android omits the property flag for
    /// these, so a mock that accepted them let a test pass against a service
    /// contract no real device would honour.
    fn require_property(
        &self, characteristic: CharacteristicUuid, has: impl Fn(&GattCharacteristicSpec) -> bool,
        property: &str,
    ) -> Result<()> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        let spec = state
            .service
            .characteristics
            .iter()
            .find(|spec| spec.uuid == characteristic)
            .ok_or_else(|| {
                BleError::Gatt(format!("unknown characteristic {}", characteristic.0))
            })?;
        if !has(spec) {
            return Err(BleError::Gatt(format!(
                "characteristic {} is not {property}",
                characteristic.0
            )));
        }
        Ok(())
    }

    /// Refuse to act when a newer connection to this peer has superseded us.
    fn ensure_current(&self) -> Result<()> {
        let current = self
            .network
            .live_sessions
            .lock()
            .unwrap()
            .get(&(self.central.clone(), self.peripheral.clone()))
            .copied();
        match current {
            Some(now) if now == self.session => Ok(()),
            // Superseded by a newer dial, or closed outright — either way
            // this handle no longer owns the link.
            _ => Err(BleError::NotConnected(self.peripheral.0.clone())),
        }
    }
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
        self.ensure_current()?;
        self.require_property(characteristic, |spec| spec.readable, "readable")?;
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        state
            .values
            .get(&characteristic)
            .cloned()
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))
    }

    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, _write_type: WriteType,
    ) -> Result<()> {
        self.ensure_current()?;
        // Validate against the advertised spec before mutating anything.
        // Accepting any UUID meant a protocol test could pass with a
        // mistyped characteristic, or with one the peripheral never declared
        // writable, and only fail on a real device — where neither backend
        // exposes a writable characteristic for that input at all.
        {
            let peripherals = self.network.peripherals.lock().unwrap();
            let state = peripherals
                .get(&self.peripheral)
                .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
            let spec = state
                .service
                .characteristics
                .iter()
                .find(|spec| spec.uuid == characteristic)
                .ok_or_else(|| {
                    BleError::Gatt(format!("unknown characteristic {}", characteristic.0))
                })?;
            if !spec.writable {
                return Err(BleError::Gatt(format!(
                    "characteristic {} is not writable",
                    characteristic.0
                )));
            }
        }
        // The mock delivers both write types identically: it has no real
        // link to drop packets on, so modelling WithoutResponse as lossy
        // would invent a failure mode rather than reproduce one.
        {
            let mut peripherals = self.network.peripherals.lock().unwrap();
            let state = peripherals
                .get_mut(&self.peripheral)
                .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
            state.values.insert(characteristic, value.clone());
        }
        // Mirrors what the real backends do: both re-emit `Connected` ahead
        // of every write, because neither has a true server-side connection
        // signal. Reproducing that here is deliberate — the mock previously
        // emitted `Connected` only from `connect()`, which made
        // `datagram::serve` look correct in tests while it was broken on
        // every real backend.
        self.network.emit(
            &self.peripheral,
            GattEvent::Connected {
                peer: self.central.clone(),
                local_role: Role::Peripheral,
                // Carry the real session, so `serve` records one and the
                // session-aware disconnect and notify paths are actually
                // exercised here rather than always taking the `None`
                // fallback.
                session: Some(self.session),
            },
        );
        self.network.emit(
            &self.peripheral,
            GattEvent::CharacteristicWritten {
                peer: self.central.clone(),
                characteristic,
                value,
            },
        );
        Ok(())
    }

    async fn subscribe(
        &mut self, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.ensure_current()?;
        let delay = *self.network.subscribe_delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        // Re-checked *after* the await and held across the insert below.
        // Checking only before meant a stalled subscribe could be superseded
        // by a reconnect and then install its sender into the replacement's
        // state and announce a peer — accepting an operation both real
        // backends reject as stale, in the one place tests use to exercise
        // exactly that timing.
        //
        // Lock order is `live_sessions` then `peripherals`, matching
        // `disconnect`; taking them the other way round here would have been
        // a deadlock rather than a fix.
        self.require_property(characteristic, |spec| spec.notifiable, "notifiable")?;
        let live = self.network.live_sessions.lock().unwrap();
        if live
            .get(&(self.central.clone(), self.peripheral.clone()))
            .copied()
            != Some(self.session)
        {
            return Err(BleError::NotConnected(self.peripheral.0.clone()));
        }
        let mut peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get_mut(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        let peers = state
            .subscribers
            .get_mut(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        // Subscribing is purely client-side — the server gets no say, which
        // is exactly why refusing a central at the protocol layer does not
        // stop it receiving broadcasts.
        let (tx, rx) = mpsc::unbounded_channel();
        peers.insert(self.central.clone(), tx);
        drop(peripherals);
        drop(live);

        // The peripheral-role arrival signal, emitted only now that the
        // notify path back to this central exists. Both real backends do the
        // same — Android from the CCCD write, Linux from AcquireNotify — and
        // the mock announcing at connect made a server-first greeting look
        // safe in tests while it could still race in production.
        self.network.emit(
            &self.peripheral,
            GattEvent::Connected {
                peer: self.central.clone(),
                local_role: Role::Peripheral,
                session: None,
            },
        );
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Mirrors both real backends: a handle kept across a reconnect must
        // not tear down the session that replaced it.
        self.ensure_current()?;
        // Then actually close it. Emitting events without clearing state
        // left this handle passing `ensure_current` afterwards, so reads,
        // writes, subscribes and repeated disconnects all still succeeded
        // through a closed connection — the mock accepting what both real
        // backends reject, which is exactly the divergence that hides bugs
        // rather than surfacing them.
        self.network
            .live_sessions
            .lock()
            .unwrap()
            .remove(&(self.central.clone(), self.peripheral.clone()));
        self.network.drop_subscriptions(&self.peripheral, &self.central);
        self.network
            .disconnect_log
            .lock()
            .unwrap()
            .push(self.peripheral.clone());
        self.network.emit(
            &self.peripheral,
            GattEvent::Disconnected {
                peer: self.central.clone(),
                local_role: Role::Peripheral,
                session: None,
            },
        );
        self.network.emit(
            &self.central,
            GattEvent::Disconnected {
                peer: self.peripheral.clone(),
                local_role: Role::Central,
                session: None,
            },
        );
        Ok(())
    }
}
