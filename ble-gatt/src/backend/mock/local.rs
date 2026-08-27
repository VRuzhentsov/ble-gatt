//! The in-process "radio": today's exact `MockNetwork` state and logic,
//! relocated (not rewritten) behind named async methods so `mock/mod.rs` can
//! dispatch to either this or, behind `mock-broker`, `remote::RemoteClient`
//! identically. `MockBackend`/`MockGattConnection` no longer reach into these
//! fields directly — every access goes through a method here.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::backend::BoxStream;
use crate::error::{BleError, Result};
use crate::models::{
    CharacteristicUuid, DiscoveredPeer, GattCharacteristicSpec, GattEvent, GattServiceSpec,
    PeerAddress, Role, ServiceUuid, WriteType,
};

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

/// Today's `MockNetwork` state, unchanged in shape — only relocated and
/// wrapped in named methods rather than reached into directly.
#[derive(Default)]
pub(crate) struct LocalRadio {
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
    next_session: AtomicU64,
    /// Newest session per (central, peripheral) pair, so the mock can
    /// reproduce a stale handle being superseded.
    live_sessions: Mutex<HashMap<(PeerAddress, PeerAddress), u64>>,
}

impl LocalRadio {
    // --- Registration / fault injection (Local-only; see mock/mod.rs) ---

    pub(crate) fn register_events_sender(&self, address: PeerAddress, sender: broadcast::Sender<GattEvent>) {
        self.event_senders.lock().unwrap().insert(address, sender);
    }

    pub(crate) fn disconnected_peers(&self) -> Vec<PeerAddress> {
        self.disconnect_log.lock().unwrap().clone()
    }

    pub(crate) fn simulate_loss_for_session(&self, central: &PeerAddress, peer: &PeerAddress, session: u64) {
        self.emit(
            central,
            GattEvent::Disconnected { peer: peer.clone(), local_role: Role::Central, session: Some(session) },
        );
    }

    pub(crate) fn simulate_event_lag(&self, to: &PeerAddress, dropped: u64) {
        self.emit(to, GattEvent::Lagged { dropped });
    }

    pub(crate) fn simulate_notification_gap(&self, peripheral: &PeerAddress, central: &PeerAddress) {
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

    pub(crate) fn simulate_unsubscribe(&self, peripheral: &PeerAddress, central: &PeerAddress) {
        let session = self
            .live_sessions
            .lock()
            .unwrap()
            .get(&(central.clone(), peripheral.clone()))
            .copied();
        self.drop_subscriptions(peripheral, central);
        self.emit(
            peripheral,
            GattEvent::Disconnected { peer: central.clone(), local_role: Role::Peripheral, session },
        );
    }

    /// Extracted verbatim from the old `MockBackend::simulate_peer_loss` body
    /// — `host` is the peripheral whose view of `peer` is being cleared.
    pub(crate) fn simulate_peer_loss(&self, host: &PeerAddress, peer: &PeerAddress) {
        self.live_sessions.lock().unwrap().remove(&(peer.clone(), host.clone()));
        self.drop_subscriptions(host, peer);
        self.emit(host, GattEvent::Disconnected { peer: peer.clone(), local_role: Role::Peripheral, session: None });
        self.emit(peer, GattEvent::Disconnected { peer: host.clone(), local_role: Role::Central, session: None });
    }

    pub(crate) fn stall_subscribe(&self, delay: std::time::Duration) {
        *self.subscribe_delay.lock().unwrap() = Some(delay);
    }

    pub(crate) fn arm_scan_failure(&self, message: impl Into<String>) {
        *self.armed_scan_failure.lock().unwrap() = Some(message.into());
    }

    pub(crate) fn set_advertisement_data(
        &self, peer: &PeerAddress, manufacturer_data: BTreeMap<u16, Vec<u8>>,
        service_data: BTreeMap<ServiceUuid, Vec<u8>>, rssi: Option<i16>,
    ) {
        if let Some(state) = self.peripherals.lock().unwrap().get_mut(peer) {
            state.manufacturer_data = manufacturer_data;
            state.service_data = service_data;
            state.rssi = rssi;
        }
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
            // `send` fails only when every receiver for `to` has been
            // dropped. Logged rather than silently discarded (as it was
            // before) after a real report of a peripheral write vanishing
            // with zero corresponding log output anywhere in the pipeline —
            // this and the broker's own now-fixed `RecvError::Lagged`
            // swallowing were the two candidate silent-discard points found
            // while investigating it. `debug`, not `warn`: unlike the
            // broker path, a momentary zero-receiver window here isn't
            // necessarily a bug in this mock (nothing pins `event_senders`
            // entries to a receiver's lifetime), so this is diagnostic
            // evidence for the next investigation, not an asserted fault.
            if let Err(err) = tx.send(event) {
                log::debug!("emit: {} has no live event receiver right now, discarding {:?}", to.0, err.0);
            }
        }
    }

    /// Refuse to act when a newer connection to this peer has superseded us.
    fn ensure_current(&self, session: u64, central: &PeerAddress, peripheral: &PeerAddress) -> Result<()> {
        let current = self.live_sessions.lock().unwrap().get(&(central.clone(), peripheral.clone())).copied();
        match current {
            Some(now) if now == session => Ok(()),
            _ => Err(BleError::NotConnected(peripheral.0.clone())),
        }
    }

    /// Reject a characteristic the peripheral never advertised with the
    /// property this operation needs.
    fn require_property(
        &self, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
        has: impl Fn(&GattCharacteristicSpec) -> bool, property: &str,
    ) -> Result<()> {
        let peripherals = self.peripherals.lock().unwrap();
        let state = peripherals.get(peripheral).ok_or_else(|| BleError::NotConnected(peripheral.0.clone()))?;
        let spec = state
            .service
            .characteristics
            .iter()
            .find(|spec| spec.uuid == characteristic)
            .ok_or_else(|| BleError::Gatt(format!("unknown characteristic {}", characteristic.0)))?;
        if !has(spec) {
            return Err(BleError::Gatt(format!("characteristic {} is not {property}", characteristic.0)));
        }
        Ok(())
    }

    // --- Backend/GattConnection-driving methods ---

    pub(crate) async fn scan(
        &self, requester: &PeerAddress, service: ServiceUuid,
    ) -> Result<(Vec<DiscoveredPeer>, Option<String>)> {
        let peripherals = self.peripherals.lock().unwrap();
        let matches: Vec<DiscoveredPeer> = peripherals
            .iter()
            .filter(|(addr, state)| *addr != requester && state.service.uuid == service)
            .map(|(addr, state)| DiscoveredPeer {
                address: addr.clone(),
                name: None,
                services: vec![state.service.uuid],
                manufacturer_data: state.manufacturer_data.clone(),
                service_data: state.service_data.clone(),
                rssi: state.rssi,
            })
            .collect();
        drop(peripherals);
        Ok((matches, self.take_armed_scan_failure()))
    }

    pub(crate) async fn connect(&self, central: &PeerAddress, peer: &PeerAddress) -> Result<u64> {
        {
            let peripherals = self.peripherals.lock().unwrap();
            peripherals
                .get(peer)
                .ok_or_else(|| BleError::ConnectFailed { peer: peer.0.clone(), reason: "peer is not advertising".to_string() })?;
        }
        // Only the central's own view is emitted here. The *peripheral*
        // learns about this peer from `subscribe`, matching both real
        // backends: announcing at connection time would let `serve` yield a
        // channel before the notify path exists, so a server that greets
        // immediately would fail with "no live notify session".
        self.emit(central, GattEvent::Connected { peer: peer.clone(), local_role: Role::Central, session: None });
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.live_sessions.lock().unwrap().insert((central.clone(), peer.clone()), session);
        Ok(session)
    }

    pub(crate) async fn advertise(&self, address: &PeerAddress, service: GattServiceSpec) -> Result<()> {
        let mut values = HashMap::new();
        let mut subscribers = HashMap::new();
        for characteristic in &service.characteristics {
            values.insert(characteristic.uuid, characteristic.initial_value.clone());
            subscribers.insert(characteristic.uuid, HashMap::new());
        }
        let manufacturer_data = service.manufacturer_data.clone();
        let service_data = service.service_data.clone();
        let mut peripherals = self.peripherals.lock().unwrap();
        let previous = peripherals.remove(address);
        peripherals.insert(
            address.clone(),
            PeripheralState {
                service,
                values,
                subscribers,
                manufacturer_data,
                service_data,
                rssi: previous.as_ref().and_then(|p| p.rssi),
            },
        );
        Ok(())
    }

    pub(crate) async fn stop_advertising(&self, address: &PeerAddress) -> Result<()> {
        // Announce every subscribed central as gone before tearing the
        // server down. Both real backends now do this — neither platform
        // delivers a disconnect callback when the *server* goes away, so
        // without it `datagram::serve` keeps serving peers that no longer
        // have a server to talk to, and locks out the next `serve`.
        let served: Vec<PeerAddress> = {
            let peripherals = self.peripherals.lock().unwrap();
            match peripherals.get(address) {
                Some(state) => {
                    let unique: HashSet<PeerAddress> =
                        state.subscribers.values().flat_map(|peers| peers.keys().cloned()).collect();
                    unique.into_iter().collect()
                }
                None => Vec::new(),
            }
        };
        self.peripherals.lock().unwrap().remove(address);
        for peer in served {
            self.emit(address, GattEvent::Disconnected { peer: peer.clone(), local_role: Role::Peripheral, session: None });
            self.emit(&peer, GattEvent::Disconnected { peer: address.clone(), local_role: Role::Central, session: None });
        }
        Ok(())
    }

    pub(crate) async fn notify(&self, address: &PeerAddress, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let peripherals = self.peripherals.lock().unwrap();
        let state = peripherals.get(address).ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let peers = state.subscribers.get(&characteristic).ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let mut delivered = false;
        for tx in peers.values() {
            if tx.send(Ok(value.clone())).is_ok() {
                delivered = true;
            }
        }
        if !delivered {
            return Err(BleError::Gatt("notify reached no subscriber".to_string()));
        }
        Ok(())
    }

    pub(crate) async fn notify_peer(
        &self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>,
        characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        if let Some(session) = session {
            let live = self.live_sessions.lock().unwrap().get(&(peer.clone(), address.clone())).copied();
            if live != Some(session) {
                return Err(BleError::NotConnected(peer.0.clone()));
            }
        }
        let peripherals = self.peripherals.lock().unwrap();
        let state = peripherals.get(address).ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let peers = state.subscribers.get(&characteristic).ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let tx = peers.get(peer).ok_or_else(|| BleError::Gatt(format!("{} has no live notify session", peer.0)))?;
        tx.send(Ok(value)).map_err(|_| BleError::Gatt(format!("{} has no live notify session", peer.0)))
    }

    pub(crate) async fn disconnect_peer(&self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        if let Some(session) = session {
            let live = self.live_sessions.lock().unwrap().get(&(peer.clone(), address.clone())).copied();
            if live != Some(session) {
                return Ok(());
            }
        }
        self.emit(peer, GattEvent::Disconnected { peer: address.clone(), local_role: Role::Central, session: None });
        self.emit(address, GattEvent::Disconnected { peer: peer.clone(), local_role: Role::Peripheral, session: None });
        self.drop_subscriptions(address, peer);
        self.live_sessions.lock().unwrap().remove(&(peer.clone(), address.clone()));
        Ok(())
    }

    pub(crate) async fn read(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<Vec<u8>> {
        self.ensure_current(session, central, peripheral)?;
        self.require_property(peripheral, characteristic, |spec| spec.readable, "readable")?;
        let peripherals = self.peripherals.lock().unwrap();
        let state = peripherals.get(peripheral).ok_or_else(|| BleError::NotConnected(peripheral.0.clone()))?;
        state.values.get(&characteristic).cloned().ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))
    }

    pub(crate) async fn write_with_type(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
        value: Vec<u8>, _write_type: WriteType,
    ) -> Result<()> {
        self.ensure_current(session, central, peripheral)?;
        // Validate against the advertised spec before mutating anything.
        {
            let peripherals = self.peripherals.lock().unwrap();
            let state = peripherals.get(peripheral).ok_or_else(|| BleError::NotConnected(peripheral.0.clone()))?;
            let spec = state
                .service
                .characteristics
                .iter()
                .find(|spec| spec.uuid == characteristic)
                .ok_or_else(|| BleError::Gatt(format!("unknown characteristic {}", characteristic.0)))?;
            if !spec.writable {
                return Err(BleError::Gatt(format!("characteristic {} is not writable", characteristic.0)));
            }
        }
        // The mock delivers both write types identically: it has no real
        // link to drop packets on, so modelling WithoutResponse as lossy
        // would invent a failure mode rather than reproduce one.
        {
            let mut peripherals = self.peripherals.lock().unwrap();
            let state = peripherals.get_mut(peripheral).ok_or_else(|| BleError::NotConnected(peripheral.0.clone()))?;
            state.values.insert(characteristic, value.clone());
        }
        // Mirrors what the real backends do: both re-emit `Connected` ahead
        // of every write, because neither has a true server-side connection
        // signal.
        self.emit(
            peripheral,
            GattEvent::Connected { peer: central.clone(), local_role: Role::Peripheral, session: Some(session) },
        );
        self.emit(
            peripheral,
            GattEvent::CharacteristicWritten { peer: central.clone(), characteristic, value },
        );
        Ok(())
    }

    pub(crate) async fn subscribe(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.ensure_current(session, central, peripheral)?;
        let delay = *self.subscribe_delay.lock().unwrap();
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
        self.require_property(peripheral, characteristic, |spec| spec.notifiable, "notifiable")?;
        let live = self.live_sessions.lock().unwrap();
        if live.get(&(central.clone(), peripheral.clone())).copied() != Some(session) {
            return Err(BleError::NotConnected(peripheral.0.clone()));
        }
        let mut peripherals = self.peripherals.lock().unwrap();
        let state = peripherals.get_mut(peripheral).ok_or_else(|| BleError::NotConnected(peripheral.0.clone()))?;
        let peers = state.subscribers.get_mut(&characteristic).ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let (tx, rx) = mpsc::unbounded_channel();
        peers.insert(central.clone(), tx);
        drop(peripherals);
        drop(live);

        // The peripheral-role arrival signal, emitted only now that the
        // notify path back to this central exists.
        self.emit(
            peripheral,
            GattEvent::Connected { peer: central.clone(), local_role: Role::Peripheral, session: Some(session) },
        );
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    pub(crate) async fn disconnect(&self, session: u64, central: &PeerAddress, peripheral: &PeerAddress) -> Result<()> {
        self.ensure_current(session, central, peripheral)?;
        self.live_sessions.lock().unwrap().remove(&(central.clone(), peripheral.clone()));
        self.drop_subscriptions(peripheral, central);
        self.disconnect_log.lock().unwrap().push(peripheral.clone());
        self.emit(peripheral, GattEvent::Disconnected { peer: central.clone(), local_role: Role::Peripheral, session: None });
        self.emit(central, GattEvent::Disconnected { peer: peripheral.clone(), local_role: Role::Central, session: None });
        Ok(())
    }

    /// Broker-only: synthesize what a graceful teardown would have done for
    /// every role `address` held, when its owning connection vanished
    /// without ever calling `stop_advertising`/`disconnect` itself (the
    /// actors-harness killing a process is exactly this). Reuses
    /// `stop_advertising`'s peripheral-role teardown verbatim, then sweeps
    /// every live session where `address` was the central.
    #[cfg(feature = "mock-broker")]
    pub(crate) async fn disconnect_all_for(&self, address: &PeerAddress) {
        let _ = self.stop_advertising(address).await;
        let sessions: Vec<(PeerAddress, PeerAddress)> = {
            let live = self.live_sessions.lock().unwrap();
            live.keys().filter(|(central, _peripheral)| central == address).cloned().collect()
        };
        for (central, peripheral) in sessions {
            self.drop_subscriptions(&peripheral, &central);
            self.live_sessions.lock().unwrap().remove(&(central.clone(), peripheral.clone()));
            self.emit(&peripheral, GattEvent::Disconnected { peer: central.clone(), local_role: Role::Peripheral, session: None });
            self.emit(&central, GattEvent::Disconnected { peer: peripheral.clone(), local_role: Role::Central, session: None });
        }
    }
}
