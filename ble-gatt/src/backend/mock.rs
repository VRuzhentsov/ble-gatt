//! In-process mock backend: no radio, no OS Bluetooth stack. Two
//! `MockBackend`s that share the same `MockNetwork` can scan/connect/serve
//! against each other, exercising the real `Backend`/`GattConnection` trait
//! contract end-to-end. Mirrors Fini's `transport::sim` adapter — a
//! first-class stand-in for CI-safe protocol tests, not a mock of one.
//!
//! Built entirely on `tokio::sync::{broadcast, Mutex}` rather than
//! hand-rolled pub-sub plumbing.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;
const NOTIFY_CHANNEL_CAPACITY: usize = 64;

/// ATT MTU the mock reports for every connection. Deliberately *not*
/// `DEFAULT_ATT_MTU`: a realistic negotiated value, so tests that chunk
/// against `max_write_len()` exercise a non-trivial size rather than
/// accidentally passing because everything fit in one write.
const MOCK_ATT_MTU: u16 = 247;

struct PeripheralState {
    service: GattServiceSpec,
    values: HashMap<CharacteristicUuid, Vec<u8>>,
    notify_tx: HashMap<CharacteristicUuid, broadcast::Sender<Vec<u8>>>,
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
    /// Armed by `arm_scan_failure`, consumed by the next `scan`. Exists so
    /// the asynchronous scan-failure path — the one that makes "Bluetooth is
    /// off" distinguishable from "no peers nearby" — is reachable in tests
    /// without a radio.
    armed_scan_failure: Mutex<Option<String>>,
}

impl MockNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Make the next `scan` on any backend in this network end with an
    /// error item, the way Android's `onScanFailed` does.
    pub fn arm_scan_failure(&self, message: impl Into<String>) {
        *self.armed_scan_failure.lock().unwrap() = Some(message.into());
    }

    fn take_armed_scan_failure(&self) -> Option<String> {
        self.armed_scan_failure.lock().unwrap().take()
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
        self.network.emit(
            &self.address,
            GattEvent::Disconnected {
                peer: peer.clone(),
                local_role: Role::Peripheral,
            },
        );
        self.network.emit(
            peer,
            GattEvent::Disconnected {
                peer: self.address.clone(),
                local_role: Role::Central,
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
        // The peripheral observes the central arriving; the central observes
        // its own connection coming up. Each side sees its own view.
        // The peripheral sees a central arrive; the central sees its own
        // outbound link come up. Each side reports the role it played.
        self.network.emit(
            peer,
            GattEvent::Connected {
                peer: self.address.clone(),
                local_role: Role::Peripheral,
            },
        );
        self.network.emit(
            &self.address,
            GattEvent::Connected {
                peer: peer.clone(),
                local_role: Role::Central,
            },
        );
        Ok(Box::new(MockGattConnection {
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
        let mut notify_tx = HashMap::new();
        for characteristic in &service.characteristics {
            values.insert(characteristic.uuid, characteristic.initial_value.clone());
            notify_tx.insert(characteristic.uuid, broadcast::channel(NOTIFY_CHANNEL_CAPACITY).0);
        }
        let mut peripherals = self.network.peripherals.lock().unwrap();
        let previous = peripherals.remove(&self.address);
        peripherals.insert(
            self.address.clone(),
            PeripheralState {
                service,
                values,
                notify_tx,
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
        self.network.peripherals.lock().unwrap().remove(&self.address);
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.address)
            .ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let tx = state
            .notify_tx
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let _ = tx.send(value);
        Ok(())
    }

    fn events(&self) -> BoxStream<GattEvent> {
        // This backend's own view, in both roles — available immediately,
        // not conditional on advertising, since a central-only consumer
        // needs it to learn about unsolicited peer loss.
        let rx = self.events_tx.subscribe();
        Box::pin(BroadcastStream::new(rx).filter_map(|item| item.ok()))
    }
}

struct MockGattConnection {
    central: PeerAddress,
    peripheral: PeerAddress,
    network: Arc<MockNetwork>,
}

#[async_trait]
impl GattConnection for MockGattConnection {
    fn peer(&self) -> PeerAddress {
        self.peripheral.clone()
    }

    fn att_mtu(&self) -> u16 {
        MOCK_ATT_MTU
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
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

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        let tx = state
            .notify_tx
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let rx = tx.subscribe();
        Ok(Box::pin(BroadcastStream::new(rx).filter_map(|item| item.ok())))
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.network.emit(
            &self.peripheral,
            GattEvent::Disconnected {
                peer: self.central.clone(),
                local_role: Role::Peripheral,
            },
        );
        self.network.emit(
            &self.central,
            GattEvent::Disconnected {
                peer: self.peripheral.clone(),
                local_role: Role::Central,
            },
        );
        Ok(())
    }
}
