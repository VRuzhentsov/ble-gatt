//! Linux backend: BlueZ via `bluer` (async D-Bus binding, 442+ GitHub stars,
//! the standard pure-Rust BlueZ interface). Supports both central and
//! peripheral role, since BlueZ's D-Bus GATT API exposes both — see the
//! plan's library-research table for why no other permissively licensed
//! crate covers peripheral mode on every platform this project targets.
//!
//! Connection-lifecycle events (`GattEvent::Connected`/`Disconnected`) are
//! derived from GATT activity (first characteristic write from a device,
//! and a notify session stopping), not from BlueZ's own device-connection
//! D-Bus signal — that would need a second, independently-driven
//! `Device::events()` watcher per connected peer. Good enough for Stage 1
//! (peer lifecycle in Fini's integration is driven by its own auth
//! handshake over the link, not by this event), revisit if a consumer needs
//! precise link-level connect/disconnect timing.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use uuid::Uuid;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bluer::adv::Advertisement;
use bluer::gatt::local::{
    Application, Characteristic as LocalCharacteristic, CharacteristicNotify, CharacteristicNotifier,
    CharacteristicNotifyMethod, CharacteristicRead, CharacteristicWrite, CharacteristicWriteMethod,
    Service as LocalService,
};
use bluer::gatt::local::{ApplicationHandle, ReqResult};
use bluer::{Adapter, AdapterEvent, Session};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tokio::task::JoinSet;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;

pub struct LinuxBackend {
    // Held only to keep the D-Bus session connection alive for the
    // lifetime of the backend; `Adapter` clones its own `Arc` into the
    // session internals, so this field is otherwise unused.
    _session: Session,
    adapter: Adapter,
    values: Arc<StdMutex<HashMap<CharacteristicUuid, Vec<u8>>>>,
    /// Centrals currently being watched for a peripheral-role disconnect.
    /// Doubles as a spawn guard: writes are frequent, and a watcher per
    /// write would spawn an unbounded number of tasks per peer.
    served_peers: Arc<StdMutex<HashSet<PeerAddress>>>,
    notifiers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicNotifier>>>>,
    events_tx: broadcast::Sender<GattEvent>,
    app_handle: AsyncMutex<Option<ApplicationHandle>>,
    adv_handle: AsyncMutex<Option<bluer::adv::AdvertisementHandle>>,
    /// Aborts the inbound-connection watcher started by `advertise`.
    server_watch: AsyncMutex<Option<tokio::task::AbortHandle>>,
    /// Addresses this backend dialled itself, in the central role. BlueZ
    /// reports one `Connected` property per device regardless of who
    /// initiated, so without this an outbound connection would be
    /// misreported as a central arriving at our server.
    dialed: Arc<StdMutex<HashSet<PeerAddress>>>,
    /// Devices currently connected that this backend did not dial, with
    /// when each connected.
    ///
    /// Candidates only — a shared adapter carries connections for unrelated
    /// services and other processes, and BlueZ's `Device1.Connected` cannot
    /// say which GATT service a peer is using. Nothing is announced from
    /// this map on its own; it exists solely to put an address on a
    /// `StartNotify`, which BlueZ delivers without one. The timestamp breaks
    /// ties — see `resolve_subscriber`.
    inbound_candidates: Arc<StdMutex<HashMap<PeerAddress, Instant>>>,
}

impl LinuxBackend {
    pub async fn new() -> Result<Self> {
        let session = Session::new()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let powered = adapter
            .is_powered()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        if !powered {
            return Err(BleError::AdapterUnavailable(format!(
                "adapter {} is not powered on",
                adapter.name()
            )));
        }
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Ok(Self {
            _session: session,
            adapter,
            values: Arc::new(StdMutex::new(HashMap::new())),
            served_peers: Arc::new(StdMutex::new(HashSet::new())),
            notifiers: Arc::new(AsyncMutex::new(HashMap::new())),
            events_tx,
            app_handle: AsyncMutex::new(None),
            adv_handle: AsyncMutex::new(None),
            server_watch: AsyncMutex::new(None),
            dialed: Arc::new(StdMutex::new(HashSet::new())),
            inbound_candidates: Arc::new(StdMutex::new(HashMap::new())),
        })
    }
}

/// Decide which connected device just subscribed to our characteristic.
///
/// BlueZ delivers `StartNotify` without an address, so this has to be
/// inferred. Returning `None` when the answer is ambiguous is not an option:
/// the whole point of announcing on subscription is to support a central
/// that waits for the server to speak first, and such a central never
/// writes — so "fall back to the write path" would mean waiting forever the
/// moment any unrelated device shares the adapter.
///
/// So it always answers if there is any candidate at all, narrowing first
/// and only then breaking the tie:
///
/// 1. Prefer candidates advertising the service we serve. A peer of this
///    application advertises it; headphones and unrelated peripherals do
///    not, which removes the common source of ambiguity outright.
/// 2. Among those, take the most recently connected — the connection that
///    just produced this subscription is overwhelmingly the newest one.
async fn resolve_subscriber(
    adapter: &Adapter, service: Uuid, candidates: &Arc<StdMutex<HashMap<PeerAddress, Instant>>>,
) -> Option<PeerAddress> {
    let snapshot: Vec<(PeerAddress, Instant)> = {
        let candidates = candidates.lock().unwrap();
        candidates.iter().map(|(k, v)| (k.clone(), *v)).collect()
    };
    if snapshot.len() <= 1 {
        return snapshot.into_iter().next().map(|(peer, _)| peer);
    }

    let mut advertising_our_service = Vec::new();
    for (peer, at) in &snapshot {
        let Ok(address) = peer.0.parse() else {
            continue;
        };
        let Ok(device) = adapter.device(address) else {
            continue;
        };
        if device
            .uuids()
            .await
            .ok()
            .flatten()
            .is_some_and(|uuids| uuids.contains(&service))
        {
            advertising_our_service.push((peer.clone(), *at));
        }
    }

    let pool = if advertising_our_service.is_empty() {
        snapshot
    } else {
        advertising_our_service
    };
    pool.into_iter().max_by_key(|(_, at)| *at).map(|(peer, _)| peer)
}

/// Watch for centrals connecting to our GATT server and surface each as
/// `Connected { local_role: Peripheral }`.
///
/// BlueZ gives a GATT server no connection callback, so this infers presence
/// from the adapter's per-device `Connected` property. Two things it must
/// get right:
///
/// - **Exclude our own outbound dials.** BlueZ reports one `Connected`
///   property per device whoever initiated it, so without the `dialed` set
///   a central-role connection would be announced as an inbound one.
/// - **Deduplicate.** `served_peers` is shared with the write path, so a
///   peer that connects and then writes is announced once, not twice.
async fn watch_inbound_connections(
    adapter: Adapter, candidates: Arc<StdMutex<HashMap<PeerAddress, Instant>>>,
    dialed: Arc<StdMutex<HashSet<PeerAddress>>>,
) {
    // Children live in a JoinSet owned by this task, so aborting it (from
    // `stop_advertising`) drops the set and aborts every per-device watcher
    // with it — no leaked tasks per advertise cycle.
    let mut watchers = JoinSet::new();
    let mut watching: HashSet<PeerAddress> = HashSet::new();

    // Subscribe *before* enumerating, so a device appearing between the two
    // is caught by the stream rather than falling in the gap.
    let Ok(mut events) = adapter.events().await else {
        return;
    };
    for address in adapter.device_addresses().await.unwrap_or_default() {
        spawn_connect_watch(&mut watchers, &mut watching, &adapter, address, &candidates, &dialed);
    }

    while let Some(event) = events.next().await {
        let AdapterEvent::DeviceAdded(address) = event else {
            continue;
        };
        spawn_connect_watch(&mut watchers, &mut watching, &adapter, address, &candidates, &dialed);
    }
}

/// Track one device's `Connected` property.
///
/// Per-device rather than adapter-wide because `AdapterEvent::DeviceAdded`
/// fires when BlueZ *creates* the device object, not when it connects — a
/// central this backend has already scanned has a device object already, so
/// its connection produces no `DeviceAdded` at all.
fn spawn_connect_watch(
    watchers: &mut JoinSet<()>, watching: &mut HashSet<PeerAddress>, adapter: &Adapter,
    address: bluer::Address, candidates: &Arc<StdMutex<HashMap<PeerAddress, Instant>>>,
    dialed: &Arc<StdMutex<HashSet<PeerAddress>>>,
) {
    let peer = PeerAddress(address.to_string());
    if !watching.insert(peer.clone()) {
        return;
    }
    let adapter = adapter.clone();
    let candidates = candidates.clone();
    let dialed = dialed.clone();
    watchers.spawn(async move {
        let Ok(device) = adapter.device(address) else {
            return;
        };
        let Ok(mut changes) = device.events().await else {
            return;
        };
        let record = |connected: bool| {
            let mut candidates = candidates.lock().unwrap();
            if connected && !dialed.lock().unwrap().contains(&peer) {
                candidates.entry(peer.clone()).or_insert_with(Instant::now);
            } else {
                candidates.remove(&peer);
            }
        };
        // Poll once after subscribing: a device already connected when this
        // watcher starts emits no property change.
        record(device.is_connected().await.unwrap_or(false));
        while let Some(event) = changes.next().await {
            if let bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(value)) =
                event
            {
                record(value);
            }
        }
    });
}

/// Emit `Disconnected { local_role: Peripheral }` once `address` drops its
/// connection, then forget it so a later reconnect re-arms a fresh watcher.
///
/// Separate from the central-role watcher in `connect` because the two carry
/// different roles and different cleanup: `datagram::serve` filters strictly
/// on `local_role`, so emitting the central variant here would be silently
/// ignored.
fn spawn_peripheral_disconnect_watch(
    adapter: Adapter, address: bluer::Address, peer: PeerAddress,
    events_tx: broadcast::Sender<GattEvent>, served_peers: Arc<StdMutex<HashSet<PeerAddress>>>,
) {
    tokio::spawn(async move {
        // Any exit path must clear the guard, or a peer that failed to be
        // watched once could never be watched again.
        let _ = async {
            let device = adapter.device(address).ok()?;
            let mut changes = device.events().await.ok()?;
            while let Some(event) = changes.next().await {
                if matches!(
                    event,
                    bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false))
                ) {
                    let _ = events_tx.send(GattEvent::Disconnected {
                        peer: peer.clone(),
                        local_role: Role::Peripheral,
                    });
                    break;
                }
            }
            Some(())
        }
        .await;
        served_peers.lock().unwrap().remove(&peer);
    });
}

#[async_trait]
impl Backend for LinuxBackend {
    async fn capabilities(&self) -> CapabilityReport {
        let peripheral = self
            .adapter
            .supported_advertising_capabilities()
            .await
            .ok()
            .flatten()
            .is_some();
        CapabilityReport {
            central: true,
            peripheral,
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let adapter = self.adapter.clone();
        let events = adapter
            .discover_devices()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let target = service.0;

        let discovered = events.filter_map(move |event| {
            let adapter = adapter.clone();
            async move {
                let AdapterEvent::DeviceAdded(address) = event else {
                    return None;
                };
                let device = adapter.device(address).ok()?;
                let uuids = device.uuids().await.ok().flatten().unwrap_or_default();
                if !uuids.contains(&target) {
                    return None;
                }
                let name = device.name().await.ok().flatten();
                let manufacturer_data = device
                    .manufacturer_data()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                let service_data = device
                    .service_data()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(uuid, data)| (ServiceUuid(uuid), data))
                    .collect();
                let rssi = device.rssi().await.ok().flatten();
                Some(Ok(DiscoveredPeer {
                    address: PeerAddress(address.to_string()),
                    name,
                    services: uuids.into_iter().map(ServiceUuid).collect(),
                    manufacturer_data,
                    service_data,
                    rssi,
                }))
            }
        });
        Ok(Box::pin(discovered))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let address: bluer::Address = peer.0.parse().map_err(|_| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: "invalid Bluetooth address".to_string(),
        })?;
        let device = self.adapter.device(address).map_err(|err| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: err.to_string(),
        })?;
        // Recorded *before* dialling: BlueZ can publish the Connected
        // property before `connect()` returns, and the inbound watcher would
        // otherwise race us and announce our own outbound link as a central
        // arriving at our server.
        self.dialed.lock().unwrap().insert(peer.clone());
        if let Err(err) = device.connect().await {
            self.dialed.lock().unwrap().remove(peer);
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: err.to_string(),
            });
        }
        let _ = self.events_tx.send(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Central,
        });

        // Watch BlueZ's own Connected property so an *unsolicited* drop (peer
        // out of range, powered off) reaches `events()`. Without this a
        // caller mid-transfer has no way to distinguish "slow" from "gone" —
        // see `Backend::events`. The task ends when the device disconnects or
        // the property stream closes, so it does not leak per connection.
        let watch_device = device.clone();
        let watch_peer = peer.clone();
        let events_tx = self.events_tx.clone();
        let dialed = self.dialed.clone();
        tokio::spawn(async move {
            let Ok(mut changes) = watch_device.events().await else {
                return;
            };
            while let Some(event) = changes.next().await {
                let bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false)) =
                    event
                else {
                    continue;
                };
                let _ = events_tx.send(GattEvent::Disconnected {
                    peer: watch_peer.clone(),
                    local_role: Role::Central,
                });
                // Stop suppressing inbound reports for this address: a later
                // connection from it may genuinely be an inbound one.
                dialed.lock().unwrap().remove(&watch_peer);
                return;
            }
        });

        Ok(Box::new(LinuxGattConnection {
            peer: peer.clone(),
            device,
            att_mtu: AtomicU16::new(crate::backend::DEFAULT_ATT_MTU),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        let values = self.values.clone();
        let served_peers = self.served_peers.clone();
        let adapter = self.adapter.clone();
        let inbound_candidates = self.inbound_candidates.clone();
        let notifiers = self.notifiers.clone();
        let events_tx = self.events_tx.clone();

        let mut local_characteristics = Vec::with_capacity(service.characteristics.len());
        for spec in &service.characteristics {
            let uuid = spec.uuid;
            values.lock().unwrap().insert(uuid, spec.initial_value.clone());

            let read = spec.readable.then(|| {
                let values = values.clone();
                CharacteristicRead {
                    read: true,
                    fun: Box::new(move |_req| {
                        let values = values.clone();
                        Box::pin(async move {
                            let value = values.lock().unwrap().get(&uuid).cloned().unwrap_or_default();
                            ReqResult::Ok(value)
                        })
                    }),
                    ..Default::default()
                }
            });

            let write = spec.writable.then(|| {
                let values = values.clone();
                let events_tx = events_tx.clone();
                let served_peers = served_peers.clone();
                let adapter = adapter.clone();
                CharacteristicWrite {
                    write: true,
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, req| {
                        let values = values.clone();
                        let events_tx = events_tx.clone();
                        let served_peers = served_peers.clone();
                        let adapter = adapter.clone();
                        let address = req.device_address;
                        let peer = PeerAddress(address.to_string());
                        Box::pin(async move {
                            values.lock().unwrap().insert(uuid, value.clone());
                            let _ = events_tx.send(GattEvent::Connected {
                                peer: peer.clone(),
                                local_role: Role::Peripheral,
                            });
                            // BlueZ gives a GATT *server* no connection
                            // callback at all — the write itself is the only
                            // signal that a central is present. So the first
                            // write from a peer arms the same Connected-
                            // property watcher the central role uses.
                            //
                            // Without this there is no peripheral-role
                            // disconnect producer on Linux, and the stale
                            // entry in `datagram::serve`'s single-central map
                            // is never cleared — permanently refusing every
                            // reconnect after the first central leaves.
                            let newly_served = served_peers
                                .lock()
                                .unwrap()
                                .insert(peer.clone());
                            if newly_served {
                                spawn_peripheral_disconnect_watch(
                                    adapter,
                                    address,
                                    peer.clone(),
                                    events_tx.clone(),
                                    served_peers,
                                );
                            }
                            let _ = events_tx.send(GattEvent::CharacteristicWritten {
                                peer,
                                characteristic: uuid,
                                value,
                            });
                            ReqResult::Ok(())
                        })
                    })),
                    ..Default::default()
                }
            });

            let notify = spec.notifiable.then(|| {
                let notifiers = notifiers.clone();
                let events_tx = events_tx.clone();
                let served_peers = served_peers.clone();
                let candidates = inbound_candidates.clone();
                let adapter = adapter.clone();
                let service_uuid = service.uuid.0;
                CharacteristicNotify {
                    notify: true,
                    method: CharacteristicNotifyMethod::Fun(Box::new(move |notifier| {
                        let notifiers = notifiers.clone();
                        let events_tx = events_tx.clone();
                        let served_peers = served_peers.clone();
                        let candidates = candidates.clone();
                        let adapter = adapter.clone();
                        Box::pin(async move {
                            notifiers.lock().await.entry(uuid).or_default().push(notifier);

                            // A central subscribing to *our* characteristic
                            // is the peripheral-role arrival signal: unlike a
                            // bare `Device1.Connected`, it is attributable to
                            // this service, and it guarantees the notify path
                            // back to the peer already exists — so a server
                            // that greets on this event cannot lose the
                            // greeting.
                            //
                            // BlueZ does not say *who* subscribed
                            // (`CharacteristicNotifier` carries no address),
                            // so the address comes from the one connected
                            // device this backend did not dial. When that is
                            // ambiguous nothing is announced and the write
                            // path remains the fallback — a wrong address
                            // would be worse than a late one.
                            let Some(peer) =
                                resolve_subscriber(&adapter, service_uuid, &candidates).await
                            else {
                                return;
                            };
                            let newly_served =
                                served_peers.lock().unwrap().insert(peer.clone());
                            if !newly_served {
                                return;
                            }
                            let _ = events_tx.send(GattEvent::Connected {
                                peer: peer.clone(),
                                local_role: Role::Peripheral,
                            });
                            if let Ok(address) = peer.0.parse() {
                                spawn_peripheral_disconnect_watch(
                                    adapter,
                                    address,
                                    peer,
                                    events_tx,
                                    served_peers,
                                );
                            }
                        })
                    })),
                    ..Default::default()
                }
            });

            local_characteristics.push(LocalCharacteristic {
                uuid: uuid.0,
                read,
                write,
                notify,
                ..Default::default()
            });
        }

        let app = Application {
            services: vec![LocalService {
                uuid: service.uuid.0,
                primary: true,
                characteristics: local_characteristics,
                ..Default::default()
            }],
            ..Default::default()
        };

        let app_handle = self
            .adapter
            .serve_gatt_application(app)
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;

        let adv = Advertisement {
            service_uuids: [service.uuid.0].into_iter().collect(),
            discoverable: Some(true),
            ..Default::default()
        };
        let adv_handle = self
            .adapter
            .advertise(adv)
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;

        *self.app_handle.lock().await = Some(app_handle);
        *self.adv_handle.lock().await = Some(adv_handle);

        // A central that connects and waits for the server to speak first
        // would otherwise never be seen: before this, the only peripheral-
        // role presence signal on Linux was an inbound *write*, so
        // `serve()` yielded no channel and both sides waited forever.
        // Android and the mock both surface the peer at connection time;
        // this closes that divergence.
        let watch = tokio::spawn(watch_inbound_connections(
            self.adapter.clone(),
            self.inbound_candidates.clone(),
            self.dialed.clone(),
        ));
        if let Some(previous) = self.server_watch.lock().await.replace(watch.abort_handle()) {
            previous.abort();
        }
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        if let Some(watch) = self.server_watch.lock().await.take() {
            watch.abort();
        }
        // Announce the departure of every central we were serving *before*
        // forgetting them. Silently clearing the set left `datagram::serve`
        // holding those peers in its single-central map with their channels
        // still live, so a later `advertise` had the old task disconnecting
        // the new server's central as an interloper — locking the new
        // `serve` out until the stale peer happened to drop physically.
        let served: Vec<PeerAddress> = self.served_peers.lock().unwrap().drain().collect();
        for peer in served {
            let _ = self.events_tx.send(GattEvent::Disconnected {
                peer,
                local_role: Role::Peripheral,
            });
        }
        self.inbound_candidates.lock().unwrap().clear();
        *self.app_handle.lock().await = None;
        *self.adv_handle.lock().await = None;
        self.notifiers.lock().await.clear();
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let mut notifiers = self.notifiers.lock().await;
        let Some(subscribers) = notifiers.get_mut(&characteristic) else {
            return Err(BleError::Gatt("no active notify session for characteristic".to_string()));
        };
        subscribers.retain(|notifier| !notifier.is_stopped());
        for notifier in subscribers.iter_mut() {
            let _ = notifier.notify(value.clone()).await;
        }
        Ok(())
    }

    async fn disconnect_peer(&self, peer: &PeerAddress) -> Result<()> {
        let Ok(address) = peer.0.parse() else {
            return Err(BleError::Gatt(format!("malformed peer address {}", peer.0)));
        };
        // Already gone is success: this is called to guarantee absence, not
        // to assert presence.
        let Ok(device) = self.adapter.device(address) else {
            return Ok(());
        };
        device
            .disconnect()
            .await
            .map_err(|err| BleError::Gatt(format!("disconnecting {} failed: {err}", peer.0)))
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.events_tx.subscribe();
        Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|item| async move { item.ok() }),
        )
    }
}

struct LinuxGattConnection {
    peer: PeerAddress,
    device: bluer::Device,
    /// Negotiated ATT MTU, refreshed from BlueZ whenever a characteristic
    /// operation gives us a cheap opportunity to read it. Starts at the
    /// spec-mandated minimum so `max_write_len()` is never optimistic before
    /// the real value is known.
    att_mtu: AtomicU16,
}

impl LinuxGattConnection {
    async fn find_characteristic(
        &self, characteristic: CharacteristicUuid,
    ) -> Result<bluer::gatt::remote::Characteristic> {
        let services = self
            .device
            .services()
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        for service in services {
            let chars = service.characteristics().await.map_err(|err| BleError::Gatt(err.to_string()))?;
            for candidate in chars {
                let uuid = candidate.uuid().await.map_err(|err| BleError::Gatt(err.to_string()))?;
                if uuid == characteristic.0 {
                    return Ok(candidate);
                }
            }
        }
        Err(BleError::Gatt(format!("characteristic {} not found on peer", characteristic.0)))
    }
}

#[async_trait]
impl GattConnection for LinuxGattConnection {
    fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    fn att_mtu(&self) -> u16 {
        self.att_mtu.load(Ordering::Relaxed)
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let target = self.find_characteristic(characteristic).await?;
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        target.read().await.map_err(|err| BleError::Gatt(err.to_string()))
    }

    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        let target = self.find_characteristic(characteristic).await?;
        // Cache the negotiated MTU opportunistically: BlueZ only publishes a
        // characteristic's MTU once the link is up, so this is the first
        // point it can be observed without a speculative extra round trip.
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        let request = bluer::gatt::remote::CharacteristicWriteRequest {
            op_type: match write_type {
                WriteType::WithResponse => bluer::gatt::WriteOp::Request,
                WriteType::WithoutResponse => bluer::gatt::WriteOp::Command,
            },
            ..Default::default()
        };
        target
            .write_ext(&value, &request)
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>> {
        let target = self.find_characteristic(characteristic).await?;
        // Refresh here too. `datagram::connect` only subscribes before it
        // fixes its fragment budget, so without this a Linux datagram
        // channel stayed at the 23-byte spec minimum — 14 payload bytes per
        // fragment — for its whole life, however large the negotiated MTU.
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        let notify_stream = target.notify().await.map_err(|err| BleError::Gatt(err.to_string()))?;
        Ok(Box::pin(notify_stream))
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.device.disconnect().await.map_err(|err| BleError::Gatt(err.to_string()))
    }
}
