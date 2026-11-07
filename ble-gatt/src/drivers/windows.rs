//! Windows driver: WinRT `Windows.Devices.Bluetooth` through Microsoft's
//! `windows` crate (layer: Frameworks & Drivers; see `docs/architecture.md`).
//! Design and open questions: `docs/adr/0011-windows-winrt-driver.md`.
//!
//! - Central: `BluetoothLEAdvertisementWatcher` scans, `BluetoothLEDevice`
//!   plus a `GattSession` with `MaintainConnection` holds the link, and
//!   `GattCharacteristic` reads, writes and subscribes. btleplug's WinRT
//!   driver uses the same classes.
//! - Peripheral: `GattServiceProvider` publishes the GATT server and
//!   advertises it (connectable, with the service UUID and one service data
//!   entry). WinRT cannot add manufacturer data to that advertisement, so a
//!   second, non-connectable `BluetoothLEAdvertisementPublisher` carries it
//!   (ADR-0011 D4).
//!
//! WinRT calls event handlers on its own thread pool, never on a Tokio
//! thread. Handlers capture a `Weak<Inner>` so a dropped driver is not kept
//! alive by a registration, and only do work that cannot block: they push
//! onto channels, and Tokio tasks do the rest.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_stream::StreamExt;
use uuid::Uuid;
use windows::core::{IInspectable, Ref, GUID};
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementDataSection, BluetoothLEAdvertisementPublisher,
    BluetoothLEAdvertisementPublisherStatus, BluetoothLEAdvertisementReceivedEventArgs,
    BluetoothLEAdvertisementWatcher, BluetoothLEAdvertisementWatcherStoppedEventArgs,
    BluetoothLEManufacturerData, BluetoothLEScanningMode,
};
use windows::Devices::Bluetooth::GenericAttributeProfile::{
    GattCharacteristic, GattCharacteristicProperties, GattClientCharacteristicConfigurationDescriptorValue,
    GattCommunicationStatus, GattDeviceService, GattLocalCharacteristic, GattLocalCharacteristicParameters,
    GattProtectionLevel, GattServiceProvider, GattServiceProviderAdvertisementStatus,
    GattServiceProviderAdvertisingParameters, GattSession, GattSessionStatus,
    GattSessionStatusChangedEventArgs, GattSubscribedClient, GattValueChangedEventArgs, GattWriteOption,
    GattWriteRequestedEventArgs,
};
use windows::Devices::Bluetooth::{
    BluetoothAdapter, BluetoothAddressType, BluetoothCacheMode, BluetoothError, BluetoothLEDevice,
    BluetoothLEPreferredConnectionParameters, BluetoothLEPreferredConnectionParametersRequest,
};
use windows::Devices::Radios::{Radio, RadioState};
use windows::Foundation::TypedEventHandler;
use windows::Storage::Streams::{DataReader, DataWriter, IBuffer};

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    advertised_name, truncate_name, CapabilityReport, CharacteristicUuid, ConnectionPriority, DiscoveredPeer,
    GattEvent, GattServiceSpec, PeerAddress, RadioStatus, Role, ServiceUuid, WriteType, NAME_SERVICE_DATA_MAX_LEN,
};
use crate::hal::{BoxStream, Driver, GattConnection, DEFAULT_ATT_MTU};

/// Scan results buffered for a slow consumer before new ones are dropped.
/// Same depth as the Android driver.
const DISCOVERY_QUEUE_DEPTH: usize = 64;

/// Inbound notifications buffered per subscription. Same depth as the
/// Android driver.
const NOTIFY_QUEUE_DEPTH: usize = 256;

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// How long `connect` waits for GATT discovery, which is where WinRT really
/// opens the link. WinRT has its own, shorter, internal timeouts; this only
/// bounds a call that never returns.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long `advertise` waits for WinRT to report that an advertisement
/// started or was aborted.
const ADVERTISE_START_TIMEOUT: Duration = Duration::from_secs(5);

const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// ATT error "Write Not Permitted" (Bluetooth Core, Vol 3, Part F, 3.4.1.1):
/// the reply to a write from a central that `disconnect_peer` excluded.
const ATT_WRITE_NOT_PERMITTED: u8 = 0x03;

// AD types (Bluetooth Assigned Numbers, 2.3) that carry service data.
const AD_SERVICE_DATA_16: u8 = 0x16;
const AD_SERVICE_DATA_32: u8 = 0x20;
const AD_SERVICE_DATA_128: u8 = 0x21;

/// The Bluetooth Base UUID, `00000000-0000-1000-8000-00805F9B34FB`.
const BLUETOOTH_BASE_UUID: u128 = 0x0000_0000_0000_1000_8000_0080_5F9B_34FB;

fn win_err(context: &str, err: windows::core::Error) -> BleError {
    BleError::Gatt(format!("{context}: {err}"))
}

fn to_guid(uuid: Uuid) -> GUID {
    GUID::from_u128(uuid.as_u128())
}

fn from_guid(guid: GUID) -> Uuid {
    Uuid::from_u128(guid.to_u128())
}

fn to_buffer(bytes: &[u8]) -> Result<IBuffer> {
    let writer = DataWriter::new().map_err(|err| win_err("DataWriter", err))?;
    writer.WriteBytes(bytes).map_err(|err| win_err("DataWriter::WriteBytes", err))?;
    writer.DetachBuffer().map_err(|err| win_err("DataWriter::DetachBuffer", err))
}

fn from_buffer(buffer: &IBuffer) -> windows::core::Result<Vec<u8>> {
    let len = buffer.Length()? as usize;
    let mut bytes = vec![0u8; len];
    if len > 0 {
        DataReader::FromBuffer(buffer)?.ReadBytes(&mut bytes)?;
    }
    Ok(bytes)
}

/// `0x112233445566` → `"11:22:33:44:55:66"`, the form the Linux and Android
/// drivers report.
fn format_address(address: u64) -> String {
    let b = address.to_be_bytes();
    format!("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}", b[2], b[3], b[4], b[5], b[6], b[7])
}

fn parse_address(address: &str) -> Option<u64> {
    let parts: Vec<&str> = address.split(':').collect();
    if parts.len() != 6 {
        return None;
    }
    parts
        .iter()
        .try_fold(0u64, |acc, part| u8::from_str_radix(part, 16).ok().map(|byte| (acc << 8) | byte as u64))
}

/// The remote address in a WinRT Bluetooth LE device id
/// (`BluetoothLE#BluetoothLE<local>-<remote>`), formatted like
/// [`format_address`]. A GATT server only learns a central through its
/// `GattSession`, whose `DeviceId` is this string. Falls back to the whole
/// id, which still identifies the central, when the format is not the
/// expected one.
fn address_from_device_id(id: &str) -> String {
    id.rsplit('-')
        .next()
        .filter(|tail| parse_address(tail).is_some())
        .map(|tail| tail.to_ascii_uppercase())
        .unwrap_or_else(|| id.to_string())
}

fn session_address(session: &GattSession) -> windows::core::Result<String> {
    Ok(address_from_device_id(&session.DeviceId()?.Id()?.to_string()))
}

/// Service data from the raw advertisement sections. WinRT parses service
/// UUIDs and manufacturer data, but leaves service data as raw sections.
fn parse_service_data(sections: impl IntoIterator<Item = BluetoothLEAdvertisementDataSection>) -> BTreeMap<ServiceUuid, Vec<u8>> {
    let mut out = BTreeMap::new();
    for section in sections {
        let (Ok(kind), Ok(data)) = (section.DataType(), section.Data()) else {
            continue;
        };
        let Ok(bytes) = from_buffer(&data) else {
            continue;
        };
        let uuid_len = match kind {
            AD_SERVICE_DATA_16 => 2,
            AD_SERVICE_DATA_32 => 4,
            AD_SERVICE_DATA_128 => 16,
            _ => continue,
        };
        if bytes.len() < uuid_len {
            continue;
        }
        // AD structures are little-endian.
        let uuid = if uuid_len == 16 {
            let mut be = [0u8; 16];
            for (i, byte) in bytes[..16].iter().enumerate() {
                be[15 - i] = *byte;
            }
            Uuid::from_bytes(be)
        } else {
            let short = bytes[..uuid_len].iter().rev().fold(0u128, |acc, byte| (acc << 8) | *byte as u128);
            Uuid::from_u128(BLUETOOTH_BASE_UUID | (short << 96))
        };
        out.insert(ServiceUuid(uuid), bytes[uuid_len..].to_vec());
    }
    out
}

fn communication_error(context: &str, status: GattCommunicationStatus, peer: &str) -> BleError {
    if status == GattCommunicationStatus::Unreachable {
        BleError::NotConnected(peer.to_string())
    } else if status == GattCommunicationStatus::AccessDenied {
        BleError::Gatt(format!("{context}: access denied"))
    } else if status == GattCommunicationStatus::ProtocolError {
        BleError::Gatt(format!("{context}: ATT protocol error"))
    } else {
        BleError::Gatt(format!("{context}: status {}", status.0))
    }
}

fn radio_status_of(state: windows::core::Result<RadioState>) -> RadioStatus {
    match state {
        Ok(RadioState::On) => RadioStatus::On,
        Ok(_) => RadioStatus::Off,
        Err(_) => RadioStatus::Off,
    }
}

/// A central attached to the local GATT server.
struct ServerPeer {
    session: u64,
    /// Held so its `SessionStatusChanged` registration stays alive.
    gatt_session: GattSession,
    status_token: i64,
    /// Set by `disconnect_peer`. WinRT has no call that drops a central
    /// from a GATT server, so an excluded central stays connected until it
    /// leaves; meanwhile it gets no notifications and its writes are
    /// refused (ADR-0011 D6).
    excluded: bool,
}

/// The live GATT server and its advertisements.
struct Server {
    provider: GattServiceProvider,
    characteristics: HashMap<CharacteristicUuid, GattLocalCharacteristic>,
    /// `(characteristic, WriteRequested token, SubscribedClientsChanged token)`.
    tokens: Vec<(GattLocalCharacteristic, Option<i64>, Option<i64>)>,
    /// Carries the manufacturer data (and service data under other UUIDs)
    /// that `GattServiceProvider` cannot advertise. `None` when the spec has
    /// none.
    publisher: Option<BluetoothLEAdvertisementPublisher>,
    /// Drains `WriteRequested` in arrival order.
    write_task: tokio::task::JoinHandle<()>,
}

impl Server {
    fn shut_down(self) {
        self.write_task.abort();
        if let Some(publisher) = &self.publisher {
            let _ = publisher.Stop();
        }
        for (characteristic, write, subscribed) in &self.tokens {
            if let Some(token) = write {
                let _ = characteristic.RemoveWriteRequested(*token);
            }
            if let Some(token) = subscribed {
                let _ = characteristic.RemoveSubscribedClientsChanged(*token);
            }
        }
        let _ = self.provider.StopAdvertising();
    }
}

/// One central-role link the driver tracks, so a second dial to the same
/// peer is refused rather than opening a second `BluetoothLEDevice`.
struct CentralLink {
    session: u64,
}

struct ScanState {
    generation: u64,
    active: bool,
}

struct Inner {
    events_tx: broadcast::Sender<GattEvent>,
    next_id: AtomicU64,
    /// Address type per address seen in a scan. Android advertises from a
    /// random address, and WinRT needs to be told the type to dial it.
    address_types: StdMutex<HashMap<u64, BluetoothAddressType>>,
    scan: StdMutex<ScanState>,
    central_links: StdMutex<HashMap<String, CentralLink>>,
    /// Serialises `advertise`, `update_advertisement` and
    /// `stop_advertising`; all three replace `server`.
    server_lock: tokio::sync::Mutex<()>,
    server: StdMutex<Option<Server>>,
    served_service: StdMutex<Option<GattServiceSpec>>,
    server_peers: StdMutex<HashMap<String, ServerPeer>>,
    radio: Option<Radio>,
}

impl Inner {
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Record a central seen through `gatt_session`, minting a session for
    /// it the first time. Emits `Connected { Peripheral }` on every call:
    /// `Driver::events` documents that it is idempotent.
    fn note_server_peer(self: &Arc<Self>, gatt_session: &GattSession) -> Option<(String, u64)> {
        let address = match session_address(gatt_session) {
            Ok(address) => address,
            Err(err) => {
                log::warn!("server: cannot read the central's device id: {err}");
                return None;
            }
        };
        let session = {
            let mut peers = self.server_peers.lock().unwrap();
            if let Some(peer) = peers.get(&address) {
                peer.session
            } else {
                let session = self.next_id();
                let weak = Arc::downgrade(self);
                let watched = address.clone();
                let handler = TypedEventHandler::new(
                    move |_: Ref<GattSession>, args: Ref<GattSessionStatusChangedEventArgs>| {
                        if args.ok()?.Status()? == GattSessionStatus::Closed {
                            if let Some(inner) = weak.upgrade() {
                                inner.server_peer_closed(&watched, session);
                            }
                        }
                        Ok(())
                    },
                );
                let status_token = match gatt_session.SessionStatusChanged(&handler) {
                    Ok(token) => token,
                    Err(err) => {
                        log::warn!("server: cannot watch {address}'s session: {err}");
                        return None;
                    }
                };
                log::info!("server: central {address} attached session={session}");
                peers.insert(
                    address.clone(),
                    ServerPeer {
                        session,
                        gatt_session: gatt_session.clone(),
                        status_token,
                        excluded: false,
                    },
                );
                session
            }
        };
        let _ = self.events_tx.send(GattEvent::Connected {
            peer: PeerAddress(address.clone()),
            local_role: Role::Peripheral,
            session: Some(session),
        });
        Some((address, session))
    }

    fn server_peer_closed(&self, address: &str, session: u64) {
        let removed = {
            let mut peers = self.server_peers.lock().unwrap();
            match peers.get(address) {
                Some(peer) if peer.session == session => peers.remove(address),
                _ => None,
            }
        };
        if let Some(peer) = removed {
            let _ = peer.gatt_session.RemoveSessionStatusChanged(peer.status_token);
            log::info!("server: central {address} left session={session}");
            let _ = self.events_tx.send(GattEvent::Disconnected {
                peer: PeerAddress(address.to_string()),
                local_role: Role::Peripheral,
                session: Some(session),
            });
        }
    }

    fn forget_server_peers(&self) {
        let peers: Vec<(String, ServerPeer)> = self.server_peers.lock().unwrap().drain().collect();
        for (address, peer) in peers {
            let _ = peer.gatt_session.RemoveSessionStatusChanged(peer.status_token);
            let _ = self.events_tx.send(GattEvent::Disconnected {
                peer: PeerAddress(address),
                local_role: Role::Peripheral,
                session: Some(peer.session),
            });
        }
    }

    fn is_excluded(&self, address: &str) -> bool {
        self.server_peers.lock().unwrap().get(address).is_some_and(|peer| peer.excluded)
    }

    /// The subscribed clients of `characteristic` on the live server.
    fn subscribed_clients(&self, characteristic: CharacteristicUuid) -> Result<Vec<(String, GattSubscribedClient)>> {
        let local = self
            .server
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| BleError::Gatt("nothing is being served".into()))?
            .characteristics
            .get(&characteristic)
            .cloned()
            .ok_or_else(|| BleError::Gatt(format!("characteristic {} is not served", characteristic.0)))?;
        let clients = local.SubscribedClients().map_err(|err| win_err("SubscribedClients", err))?;
        Ok(clients
            .into_iter()
            .filter_map(|client| {
                let address = client.Session().and_then(|session| session_address(&session)).ok()?;
                Some((address, client))
            })
            .collect())
    }

    fn local_characteristic(&self, characteristic: CharacteristicUuid) -> Result<GattLocalCharacteristic> {
        self.server
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|server| server.characteristics.get(&characteristic).cloned())
            .ok_or_else(|| BleError::Gatt(format!("characteristic {} is not served", characteristic.0)))
    }
}

pub struct WindowsDriver {
    inner: Arc<Inner>,
    radio_token: Option<i64>,
}

impl WindowsDriver {
    /// Fails with [`BleError::AdapterUnavailable`] when the computer has no
    /// Bluetooth adapter or the adapter has no Bluetooth LE.
    pub async fn new() -> Result<Self> {
        let adapter = default_adapter().await?;
        if !adapter.IsLowEnergySupported().unwrap_or(false) {
            return Err(BleError::AdapterUnavailable("the Bluetooth adapter has no Bluetooth LE".into()));
        }
        let radio = match adapter.GetRadioAsync() {
            Ok(op) => op.await.ok(),
            Err(_) => None,
        };
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            events_tx,
            next_id: AtomicU64::new(1),
            address_types: StdMutex::new(HashMap::new()),
            scan: StdMutex::new(ScanState { generation: 0, active: false }),
            central_links: StdMutex::new(HashMap::new()),
            server_lock: tokio::sync::Mutex::new(()),
            server: StdMutex::new(None),
            served_service: StdMutex::new(None),
            server_peers: StdMutex::new(HashMap::new()),
            radio,
        });
        let radio_token = inner.radio.as_ref().and_then(|radio| {
            let weak = Arc::downgrade(&inner);
            let last = Arc::new(StdMutex::new(radio_status_of(radio.State())));
            let handler = TypedEventHandler::new(move |sender: Ref<Radio>, _: Ref<IInspectable>| {
                let status = radio_status_of(sender.ok()?.State());
                let mut last = last.lock().unwrap();
                if *last != status {
                    *last = status;
                    if let Some(inner) = weak.upgrade() {
                        log::info!("radio: {status:?}");
                        let _ = inner.events_tx.send(GattEvent::RadioChanged { status });
                    }
                }
                Ok(())
            });
            radio.StateChanged(&handler).ok()
        });
        log::info!("windows driver: ready, radio {:?}", inner.radio.as_ref().map(|r| radio_status_of(r.State())));
        Ok(Self { inner, radio_token })
    }
}

impl Drop for WindowsDriver {
    fn drop(&mut self) {
        if let (Some(radio), Some(token)) = (&self.inner.radio, self.radio_token) {
            let _ = radio.RemoveStateChanged(token);
        }
        if let Some(server) = self.inner.server.lock().unwrap().take() {
            server.shut_down();
        }
    }
}

async fn default_adapter() -> Result<BluetoothAdapter> {
    let op = BluetoothAdapter::GetDefaultAsync()
        .map_err(|err| BleError::AdapterUnavailable(format!("BluetoothAdapter::GetDefaultAsync: {err}")))?;
    // WinRT returns a null adapter, which `windows` reports as an error,
    // when the computer has no Bluetooth adapter.
    op.await
        .map_err(|err| BleError::AdapterUnavailable(format!("no Bluetooth adapter: {err}")))
}

#[async_trait]
impl Driver for WindowsDriver {
    /// What the default adapter reports (`IsCentralRoleSupported`,
    /// `IsPeripheralRoleSupported`), and `false` for both when there is no
    /// adapter or no Bluetooth LE. `IsPeripheralRoleSupported` is the
    /// radio and driver's answer to "can this computer run a GATT server";
    /// many older adapters say no (ADR-0011 D2).
    async fn capabilities(&self) -> CapabilityReport {
        let adapter = match default_adapter().await {
            Ok(adapter) => adapter,
            Err(err) => {
                log::error!("capabilities: {err}");
                return CapabilityReport::default();
            }
        };
        let le = adapter.IsLowEnergySupported().unwrap_or(false);
        let central = le && adapter.IsCentralRoleSupported().unwrap_or(false);
        let peripheral = le && adapter.IsPeripheralRoleSupported().unwrap_or(false);
        log::info!("capabilities: le={le} central={central} peripheral={peripheral}");
        CapabilityReport { central, peripheral }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let generation = {
            let mut scan = self.inner.scan.lock().unwrap();
            if scan.active {
                return Err(BleError::Gatt("a scan is already active; concurrent scans are not supported".into()));
            }
            scan.active = true;
            scan.generation += 1;
            scan.generation
        };
        let started = start_watcher(&self.inner, service);
        match started {
            Ok((watcher, tokens, rx, error)) => {
                log::info!("scan: started for service {} generation={generation}", service.0);
                Ok(Box::pin(ScanStream {
                    inner: self.inner.clone(),
                    rx: ReceiverStream::new(rx),
                    error,
                    done: false,
                    watcher,
                    tokens,
                    generation,
                }))
            }
            Err(err) => {
                let mut scan = self.inner.scan.lock().unwrap();
                if scan.generation == generation {
                    scan.active = false;
                }
                Err(err)
            }
        }
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let failed = |reason: String| BleError::ConnectFailed { peer: peer.0.clone(), reason };
        let address = parse_address(&peer.0).ok_or_else(|| failed("not a Bluetooth address".into()))?;
        let session = self.inner.next_id();
        {
            let mut links = self.inner.central_links.lock().unwrap();
            if links.contains_key(&peer.0) {
                return Err(failed("a connection to this peer is already open or in progress".into()));
            }
            links.insert(peer.0.clone(), CentralLink { session });
        }
        // Released on every failure below, including a cancelled future.
        let mut guard = LinkGuard { inner: self.inner.clone(), address: peer.0.clone(), session, armed: true };

        let address_type = self.inner.address_types.lock().unwrap().get(&address).copied();
        let opened = tokio::time::timeout(CONNECT_TIMEOUT, open_link(address, address_type)).await;
        let (device, gatt_session, services, characteristics) = match opened {
            Err(_) => return Err(failed(format!("no answer in {CONNECT_TIMEOUT:?}"))),
            Ok(Err(reason)) => return Err(failed(reason)),
            Ok(Ok(link)) => link,
        };

        let closed = Arc::new(AtomicBool::new(false));
        let status_token = {
            let weak = Arc::downgrade(&self.inner);
            let closed = closed.clone();
            let watched = peer.0.clone();
            let handler = TypedEventHandler::new(
                move |_: Ref<GattSession>, args: Ref<GattSessionStatusChangedEventArgs>| {
                    if args.ok()?.Status()? == GattSessionStatus::Closed {
                        if let Some(inner) = weak.upgrade() {
                            central_link_closed(&inner, &watched, session, &closed);
                        }
                    }
                    Ok(())
                },
            );
            gatt_session.SessionStatusChanged(&handler).ok()
        };
        guard.armed = false;
        log::info!(
            "connect: {} connected session={session} mtu={}",
            peer.0,
            gatt_session.MaxPduSize().unwrap_or(DEFAULT_ATT_MTU)
        );
        let _ = self.inner.events_tx.send(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Central,
            session: Some(session),
        });
        Ok(Box::new(WindowsGattConnection {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            session,
            device,
            gatt_session,
            status_token,
            services,
            characteristics,
            subscriptions: Arc::new(StdMutex::new(Vec::new())),
            priority_request: None,
            closed,
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        let _serialise = self.inner.server_lock.lock().await;
        if let Some(old) = self.inner.server.lock().unwrap().take() {
            log::info!("advertise: replacing the served service");
            old.shut_down();
        }
        self.inner.forget_server_peers();
        *self.inner.served_service.lock().unwrap() = None;

        let server = start_server(&self.inner, &service).await?;
        *self.inner.server.lock().unwrap() = Some(server);
        *self.inner.served_service.lock().unwrap() = Some(service.clone());
        log::info!("advertise: serving {}", service.uuid.0);
        Ok(())
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        let _serialise = self.inner.server_lock.lock().await;
        let served = self
            .inner
            .served_service
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| BleError::Gatt("update_advertisement: nothing is being advertised".into()))?;
        if !served.same_service(&service) {
            return Err(BleError::Gatt("update_advertisement: the spec describes a different service".into()));
        }
        let (provider, old_publisher) = {
            let mut server = self.inner.server.lock().unwrap();
            let server = server
                .as_mut()
                .ok_or_else(|| BleError::Gatt("update_advertisement: nothing is being advertised".into()))?;
            (server.provider.clone(), server.publisher.take())
        };
        if let Some(publisher) = old_publisher {
            let _ = publisher.Stop();
        }
        // The provider's advertisement only carries the name (or the service
        // data under the served UUID); restart it only when that changed.
        if provider_service_data(&served) != provider_service_data(&service) {
            let _ = provider.StopAdvertising();
            start_provider_advertising(&provider, &service).await?;
        }
        let publisher = start_publisher(&service).await?;
        if let Some(server) = self.inner.server.lock().unwrap().as_mut() {
            server.publisher = publisher;
        }
        *self.inner.served_service.lock().unwrap() = Some(service);
        log::info!("update_advertisement: applied");
        Ok(())
    }

    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service.lock().unwrap().clone()
    }

    async fn stop_advertising(&self) -> Result<()> {
        let _serialise = self.inner.server_lock.lock().await;
        if let Some(server) = self.inner.server.lock().unwrap().take() {
            server.shut_down();
            log::info!("stop_advertising: stopped");
        }
        self.inner.forget_server_peers();
        *self.inner.served_service.lock().unwrap() = None;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.stop_advertising().await
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let clients = self.inner.subscribed_clients(characteristic)?;
        let local = self.inner.local_characteristic(characteristic)?;
        let mut delivered = 0usize;
        for (address, client) in clients {
            if self.inner.is_excluded(&address) {
                continue;
            }
            if notify_client(&local, &value, &client).await.is_ok() {
                delivered += 1;
            }
        }
        if delivered == 0 {
            return Err(BleError::Gatt(format!("notify {}: no subscribed central received it", characteristic.0)));
        }
        Ok(())
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        {
            let peers = self.inner.server_peers.lock().unwrap();
            let current = peers.get(&peer.0).ok_or_else(|| BleError::NotConnected(peer.0.clone()))?;
            if session.is_some_and(|session| session != current.session) {
                return Err(BleError::NotConnected(format!("{} (session {session:?} has ended)", peer.0)));
            }
            if current.excluded {
                return Err(BleError::NotConnected(format!("{} (disconnected by this side)", peer.0)));
            }
        }
        let client = self
            .inner
            .subscribed_clients(characteristic)?
            .into_iter()
            .find(|(address, _)| *address == peer.0)
            .map(|(_, client)| client)
            .ok_or_else(|| BleError::NotConnected(format!("{} is not subscribed to {}", peer.0, characteristic.0)))?;
        let local = self.inner.local_characteristic(characteristic)?;
        notify_client(&local, &value, &client).await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        let mut peers = self.inner.server_peers.lock().unwrap();
        if let Some(current) = peers.get_mut(&peer.0) {
            if session.is_none_or(|session| session == current.session) {
                current.excluded = true;
                log::info!(
                    "disconnect_peer: {} session={} excluded; WinRT cannot drop a central from a GATT server",
                    peer.0,
                    current.session
                );
            }
        }
        Ok(())
    }

    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        {
            let peers = self.inner.server_peers.lock().unwrap();
            let current = peers.get(&peer.0)?;
            if session.is_some_and(|session| session != current.session) {
                return None;
            }
        }
        let served = self.inner.served_service.lock().unwrap().clone()?;
        for characteristic in served.characteristics.iter().filter(|c| c.notifiable) {
            let Ok(clients) = self.inner.subscribed_clients(characteristic.uuid) else {
                continue;
            };
            if let Some((_, client)) = clients.into_iter().find(|(address, _)| *address == peer.0) {
                return client.MaxNotificationSize().ok().map(|len| len as usize);
            }
        }
        None
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let stream = BroadcastStream::new(self.inner.events_tx.subscribe()).map(|item| match item {
            Ok(event) => event,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(dropped)) => {
                GattEvent::Lagged { dropped }
            }
        });
        Box::pin(stream)
    }

    async fn radio_status(&self) -> RadioStatus {
        match &self.inner.radio {
            Some(radio) => radio_status_of(radio.State()),
            None => RadioStatus::Unsupported,
        }
    }
}

// --- Central role ---

type WatcherParts = (
    BluetoothLEAdvertisementWatcher,
    (i64, i64),
    mpsc::Receiver<Result<DiscoveredPeer>>,
    Arc<StdMutex<Option<BleError>>>,
);

/// What one address has advertised so far. Active scanning delivers the
/// advertisement and the scan response as separate `Received` events, and
/// only the advertisement carries the service UUID, so the two are merged
/// here before a peer is reported.
#[derive(Default, PartialEq)]
struct SeenPeer {
    services: Vec<ServiceUuid>,
    manufacturer_data: BTreeMap<u16, Vec<u8>>,
    service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    local_name: Option<String>,
}

fn start_watcher(inner: &Arc<Inner>, service: ServiceUuid) -> Result<WatcherParts> {
    let watcher = BluetoothLEAdvertisementWatcher::new().map_err(|err| win_err("AdvertisementWatcher", err))?;
    watcher
        .SetScanningMode(BluetoothLEScanningMode::Active)
        .map_err(|err| win_err("SetScanningMode", err))?;
    // Windows 10 2004 and later; harmless to skip where it is missing.
    let _ = watcher.SetAllowExtendedAdvertisements(true);

    let (tx, rx) = mpsc::channel(DISCOVERY_QUEUE_DEPTH);
    let tx = Arc::new(StdMutex::new(Some(tx)));
    let error = Arc::new(StdMutex::new(None));
    let seen: Arc<StdMutex<HashMap<u64, SeenPeer>>> = Arc::new(StdMutex::new(HashMap::new()));

    let received = {
        let tx = tx.clone();
        let weak = Arc::downgrade(inner);
        TypedEventHandler::new(
            move |_: Ref<BluetoothLEAdvertisementWatcher>, args: Ref<BluetoothLEAdvertisementReceivedEventArgs>| {
                let args = args.ok()?;
                let address = args.BluetoothAddress()?;
                if let (Some(inner), Ok(kind)) = (weak.upgrade(), args.BluetoothAddressType()) {
                    inner.address_types.lock().unwrap().insert(address, kind);
                }
                let advertisement = args.Advertisement()?;
                let mut seen = seen.lock().unwrap();
                let entry = seen.entry(address).or_default();
                let before = (
                    entry.manufacturer_data.clone(),
                    entry.service_data.clone(),
                    entry.local_name.clone(),
                    entry.services.contains(&service),
                );
                for uuid in advertisement.ServiceUuids()? {
                    let uuid = ServiceUuid(from_guid(uuid));
                    if !entry.services.contains(&uuid) {
                        entry.services.push(uuid);
                    }
                }
                for data in advertisement.ManufacturerData()? {
                    entry.manufacturer_data.insert(data.CompanyId()?, from_buffer(&data.Data()?)?);
                }
                entry.service_data.extend(parse_service_data(advertisement.DataSections()?));
                let name = advertisement.LocalName()?.to_string();
                if !name.is_empty() {
                    entry.local_name = Some(name);
                }
                if !entry.services.contains(&service) {
                    return Ok(());
                }
                let after = (
                    entry.manufacturer_data.clone(),
                    entry.service_data.clone(),
                    entry.local_name.clone(),
                    true,
                );
                // Report a peer when it first matches and again whenever
                // its data changes (the scan response arriving later), like
                // the other drivers; not on every repeated packet.
                if before == after {
                    return Ok(());
                }
                let peer = DiscoveredPeer {
                    address: PeerAddress(format_address(address)),
                    name: advertised_name(service, &entry.service_data, entry.local_name.clone()),
                    services: entry.services.clone(),
                    manufacturer_data: entry.manufacturer_data.clone(),
                    service_data: entry.service_data.clone(),
                    rssi: args.RawSignalStrengthInDBm().ok(),
                };
                if let Some(tx) = tx.lock().unwrap().as_ref() {
                    if tx.try_send(Ok(peer)).is_err() {
                        log::debug!("scan: result queue full, dropping a result");
                    }
                }
                Ok(())
            },
        )
    };
    let stopped = {
        let tx = tx.clone();
        let error = error.clone();
        TypedEventHandler::new(
            move |_: Ref<BluetoothLEAdvertisementWatcher>, args: Ref<BluetoothLEAdvertisementWatcherStoppedEventArgs>| {
                let reason = args.ok().and_then(|args| args.Error()).unwrap_or(BluetoothError::OtherError);
                log::warn!("scan: watcher stopped, error {}", reason.0);
                let err = if reason == BluetoothError::RadioNotAvailable || reason == BluetoothError::DisabledByUser {
                    BleError::AdapterUnavailable("the Bluetooth radio is off".into())
                } else if reason == BluetoothError::ConsentRequired || reason == BluetoothError::DisabledByPolicy {
                    BleError::AdapterUnavailable(format!("Windows refused the scan (error {})", reason.0))
                } else {
                    BleError::Gatt(format!("the scan stopped (error {})", reason.0))
                };
                *error.lock().unwrap() = Some(err);
                // Dropping the sender wakes the stream, which then reports
                // the error.
                tx.lock().unwrap().take();
                Ok(())
            },
        )
    };
    let received_token = watcher.Received(&received).map_err(|err| win_err("Watcher::Received", err))?;
    let stopped_token = watcher.Stopped(&stopped).map_err(|err| win_err("Watcher::Stopped", err))?;
    if let Err(err) = watcher.Start() {
        let _ = watcher.RemoveReceived(received_token);
        let _ = watcher.RemoveStopped(stopped_token);
        return Err(BleError::AdapterUnavailable(format!("cannot start the scan: {err}")));
    }
    Ok((watcher, (received_token, stopped_token), rx, error))
}

struct ScanStream {
    inner: Arc<Inner>,
    rx: ReceiverStream<Result<DiscoveredPeer>>,
    error: Arc<StdMutex<Option<BleError>>>,
    done: bool,
    watcher: BluetoothLEAdvertisementWatcher,
    tokens: (i64, i64),
    generation: u64,
}

impl tokio_stream::Stream for ScanStream {
    type Item = Result<DiscoveredPeer>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if self.done {
            return Poll::Ready(None);
        }
        // A terminal error goes first, out of band: if it waited behind a
        // full queue, a slow consumer would read a truncated peer list and
        // then an ordinary end of stream.
        let pending = self.error.lock().unwrap().take();
        if let Some(err) = pending {
            self.done = true;
            return Poll::Ready(Some(Err(err)));
        }
        match std::pin::Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(None) => {
                self.done = true;
                let err = self.error.lock().unwrap().take();
                Poll::Ready(err.map(Err))
            }
            other => other,
        }
    }
}

impl Drop for ScanStream {
    fn drop(&mut self) {
        let _ = self.watcher.RemoveStopped(self.tokens.1);
        let _ = self.watcher.Stop();
        let _ = self.watcher.RemoveReceived(self.tokens.0);
        let mut scan = self.inner.scan.lock().unwrap();
        if scan.generation == self.generation {
            scan.active = false;
        }
        log::info!("scan: stopped generation={}", self.generation);
    }
}

type OpenedLink = (BluetoothLEDevice, GattSession, Vec<GattDeviceService>, HashMap<Uuid, GattCharacteristic>);

/// Open the device, hold the link with a `GattSession`, and discover every
/// characteristic. WinRT opens the radio link lazily, at the first GATT
/// request, so service discovery is what actually connects.
async fn open_link(address: u64, address_type: Option<BluetoothAddressType>) -> std::result::Result<OpenedLink, String> {
    let device_op = match address_type {
        Some(kind) => BluetoothLEDevice::FromBluetoothAddressWithBluetoothAddressTypeAsync(address, kind),
        None => BluetoothLEDevice::FromBluetoothAddressAsync(address),
    }
    .map_err(|err| format!("FromBluetoothAddressAsync: {err}"))?;
    let device = device_op.await.map_err(|err| format!("unknown device: {err}"))?;
    let device_id = device.BluetoothDeviceId().map_err(|err| format!("BluetoothDeviceId: {err}"))?;
    let gatt_session = GattSession::FromDeviceIdAsync(&device_id)
        .map_err(|err| format!("GattSession: {err}"))?
        .await
        .map_err(|err| format!("GattSession: {err}"))?;
    // Keeps the link up between operations. Without it Windows may drop an
    // idle link after a few seconds, which looks like the peer leaving.
    gatt_session.SetMaintainConnection(true).map_err(|err| format!("MaintainConnection: {err}"))?;

    let fail = |reason: String| {
        let _ = gatt_session.SetMaintainConnection(false);
        let _ = gatt_session.Close();
        let _ = device.Close();
        reason
    };
    let services = match discover_services(&device).await {
        Ok(services) => services,
        Err(reason) => return Err(fail(reason)),
    };
    let mut characteristics = HashMap::new();
    for service in &services {
        let result = match service.GetCharacteristicsWithCacheModeAsync(BluetoothCacheMode::Uncached) {
            Ok(op) => op.await,
            Err(err) => Err(err),
        };
        let Ok(result) = result else { continue };
        if result.Status() != Ok(GattCommunicationStatus::Success) {
            continue;
        }
        if let Ok(list) = result.Characteristics() {
            for characteristic in list {
                if let Ok(uuid) = characteristic.Uuid() {
                    characteristics.insert(from_guid(uuid), characteristic);
                }
            }
        }
    }
    Ok((device, gatt_session, services, characteristics))
}

async fn discover_services(device: &BluetoothLEDevice) -> std::result::Result<Vec<GattDeviceService>, String> {
    let result = device
        .GetGattServicesWithCacheModeAsync(BluetoothCacheMode::Uncached)
        .map_err(|err| format!("GetGattServicesAsync: {err}"))?
        .await
        .map_err(|err| format!("GetGattServicesAsync: {err}"))?;
    let status = result.Status().map_err(|err| format!("GetGattServicesAsync: {err}"))?;
    if status != GattCommunicationStatus::Success {
        return Err(match status {
            GattCommunicationStatus::Unreachable => "unreachable".to_string(),
            GattCommunicationStatus::AccessDenied => "access denied".to_string(),
            other => format!("service discovery failed (status {})", other.0),
        });
    }
    Ok(result
        .Services()
        .map_err(|err| format!("Services: {err}"))?
        .into_iter()
        .collect())
}

fn central_link_closed(inner: &Inner, address: &str, session: u64, closed: &AtomicBool) {
    if closed.swap(true, Ordering::SeqCst) {
        return;
    }
    {
        let mut links = inner.central_links.lock().unwrap();
        if links.get(address).is_some_and(|link| link.session == session) {
            links.remove(address);
        }
    }
    log::info!("connection: {address} closed session={session}");
    let _ = inner.events_tx.send(GattEvent::Disconnected {
        peer: PeerAddress(address.to_string()),
        local_role: Role::Central,
        session: Some(session),
    });
}

/// Frees a `central_links` slot when `connect` fails or is cancelled.
struct LinkGuard {
    inner: Arc<Inner>,
    address: String,
    session: u64,
    armed: bool,
}

impl Drop for LinkGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut links = self.inner.central_links.lock().unwrap();
            if links.get(&self.address).is_some_and(|link| link.session == self.session) {
                links.remove(&self.address);
            }
        }
    }
}

struct WindowsGattConnection {
    inner: Arc<Inner>,
    address: String,
    session: u64,
    device: BluetoothLEDevice,
    gatt_session: GattSession,
    status_token: Option<i64>,
    services: Vec<GattDeviceService>,
    characteristics: HashMap<Uuid, GattCharacteristic>,
    /// `(characteristic, ValueChanged token)` per live subscription, shared
    /// with each `NotifyStream` so a dropped stream removes its own.
    subscriptions: Arc<StdMutex<Vec<(GattCharacteristic, i64)>>>,
    /// Must stay alive: closing it withdraws the request.
    priority_request: Option<BluetoothLEPreferredConnectionParametersRequest>,
    closed: Arc<AtomicBool>,
}

impl WindowsGattConnection {
    fn characteristic(&self, characteristic: CharacteristicUuid) -> Result<&GattCharacteristic> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BleError::NotConnected(self.address.clone()));
        }
        self.characteristics
            .get(&characteristic.0)
            .ok_or_else(|| BleError::Gatt(format!("characteristic {} not found on peer", characteristic.0)))
    }

    fn release(&mut self) {
        for (characteristic, token) in self.subscriptions.lock().unwrap().drain(..) {
            let _ = characteristic.RemoveValueChanged(token);
        }
        if let Some(request) = self.priority_request.take() {
            let _ = request.Close();
        }
        if let Some(token) = self.status_token.take() {
            let _ = self.gatt_session.RemoveSessionStatusChanged(token);
        }
        let _ = self.gatt_session.SetMaintainConnection(false);
        let _ = self.gatt_session.Close();
        for service in self.services.drain(..) {
            let _ = service.Close();
        }
        self.characteristics.clear();
        let _ = self.device.Close();
        central_link_closed(&self.inner, &self.address, self.session, &self.closed);
    }
}

impl Drop for WindowsGattConnection {
    fn drop(&mut self) {
        self.release();
    }
}

#[async_trait]
impl GattConnection for WindowsGattConnection {
    fn peer(&self) -> PeerAddress {
        PeerAddress(self.address.clone())
    }

    fn session(&self) -> Option<u64> {
        Some(self.session)
    }

    /// `GattSession::MaxPduSize` is the negotiated ATT MTU; Windows
    /// requests a large one by itself on connect.
    fn att_mtu(&self) -> u16 {
        self.gatt_session.MaxPduSize().unwrap_or(DEFAULT_ATT_MTU).max(DEFAULT_ATT_MTU)
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let target = self.characteristic(characteristic)?.clone();
        let result = target
            .ReadValueWithCacheModeAsync(BluetoothCacheMode::Uncached)
            .map_err(|err| win_err("ReadValueAsync", err))?
            .await
            .map_err(|err| win_err("ReadValueAsync", err))?;
        let status = result.Status().map_err(|err| win_err("ReadValueAsync", err))?;
        if status != GattCommunicationStatus::Success {
            return Err(communication_error("read", status, &self.address));
        }
        from_buffer(&result.Value().map_err(|err| win_err("ReadValueAsync", err))?)
            .map_err(|err| win_err("ReadValueAsync", err))
    }

    async fn write_with_type(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType) -> Result<()> {
        let target = self.characteristic(characteristic)?.clone();
        let option = match write_type {
            WriteType::WithResponse => GattWriteOption::WriteWithResponse,
            WriteType::WithoutResponse => GattWriteOption::WriteWithoutResponse,
        };
        // `IBuffer` is not `Send`, so it must be gone before the `.await`.
        let op = target
            .WriteValueWithResultAndOptionAsync(&to_buffer(&value)?, option)
            .map_err(|err| win_err("WriteValueAsync", err))?;
        let result = op.await.map_err(|err| win_err("WriteValueAsync", err))?;
        let status = result.Status().map_err(|err| win_err("WriteValueAsync", err))?;
        if status != GattCommunicationStatus::Success {
            return Err(communication_error("write", status, &self.address));
        }
        Ok(())
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        let target = self.characteristic(characteristic)?.clone();
        let (tx, rx) = mpsc::channel(NOTIFY_QUEUE_DEPTH);
        let overflow = Arc::new(AtomicBool::new(false));
        // Registered before the CCCD write, so a notification sent right
        // after the peer accepts the subscription is not missed. The handler
        // object is not `Send`; only its token outlives this block.
        let token = {
            let overflow = overflow.clone();
            let handler =
                TypedEventHandler::new(move |_: Ref<GattCharacteristic>, args: Ref<GattValueChangedEventArgs>| {
                    let bytes = from_buffer(&args.ok()?.CharacteristicValue()?)?;
                    if tx.try_send(bytes).is_err() {
                        overflow.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                });
            target.ValueChanged(&handler).map_err(|err| win_err("ValueChanged", err))?
        };
        let written = match target.WriteClientCharacteristicConfigurationDescriptorWithResultAsync(
            GattClientCharacteristicConfigurationDescriptorValue::Notify,
        ) {
            Ok(op) => op.await.and_then(|result| result.Status()),
            Err(err) => Err(err),
        };
        match written {
            Ok(GattCommunicationStatus::Success) => {}
            Ok(status) => {
                let _ = target.RemoveValueChanged(token);
                return Err(communication_error("subscribe", status, &self.address));
            }
            Err(err) => {
                let _ = target.RemoveValueChanged(token);
                return Err(win_err("subscribe", err));
            }
        }
        self.subscriptions.lock().unwrap().push((target.clone(), token));
        Ok(Box::pin(NotifyStream {
            rx: ReceiverStream::new(rx),
            overflow,
            characteristic: target,
            token,
            subscriptions: self.subscriptions.clone(),
        }))
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.release();
        Ok(())
    }

    /// `BluetoothLEDevice::RequestPreferredConnectionParameters`, Windows 11
    /// and later. Earlier Windows reports [`BleError::Unsupported`].
    async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BleError::NotConnected(self.address.clone()));
        }
        let parameters = match priority {
            ConnectionPriority::LowPower => BluetoothLEPreferredConnectionParameters::PowerOptimized(),
            ConnectionPriority::Balanced => BluetoothLEPreferredConnectionParameters::Balanced(),
            ConnectionPriority::High => BluetoothLEPreferredConnectionParameters::ThroughputOptimized(),
        }
        .map_err(|err| BleError::Unsupported(format!("connection priority on this Windows: {err}")))?;
        let request = self
            .device
            .RequestPreferredConnectionParameters(&parameters)
            .map_err(|err| BleError::Unsupported(format!("connection priority on this Windows: {err}")))?;
        if let Some(previous) = self.priority_request.replace(request) {
            let _ = previous.Close();
        }
        Ok(())
    }
}

struct NotifyStream {
    rx: ReceiverStream<Vec<u8>>,
    overflow: Arc<AtomicBool>,
    characteristic: GattCharacteristic,
    token: i64,
    subscriptions: Arc<StdMutex<Vec<(GattCharacteristic, i64)>>>,
}

impl tokio_stream::Stream for NotifyStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        if self.overflow.swap(false, Ordering::SeqCst) {
            return std::task::Poll::Ready(Some(Err(BleError::Gatt(
                "notification queue overflowed: payloads were dropped and at least one message is lost".into(),
            ))));
        }
        std::pin::Pin::new(&mut self.rx).poll_next(cx).map(|item| item.map(Ok))
    }
}

impl Drop for NotifyStream {
    fn drop(&mut self) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if let Some(index) = subscriptions.iter().position(|(_, token)| *token == self.token) {
            subscriptions.remove(index);
            let _ = self.characteristic.RemoveValueChanged(self.token);
        }
    }
}

// --- Peripheral role ---

/// The service data the provider's own advertisement carries under the
/// served UUID: the name, sent as service data like the Android driver
/// does, or the caller's service data under that UUID (`validate` forbids
/// both).
fn provider_service_data(spec: &GattServiceSpec) -> Option<Vec<u8>> {
    match &spec.local_name {
        Some(name) => Some(truncate_name(name, NAME_SERVICE_DATA_MAX_LEN).as_bytes().to_vec()),
        None => spec.service_data.get(&spec.uuid).cloned(),
    }
}

fn server_error(context: &str, error: BluetoothError) -> BleError {
    if error == BluetoothError::RadioNotAvailable || error == BluetoothError::DisabledByUser {
        BleError::AdapterUnavailable(format!("{context}: the Bluetooth radio is off"))
    } else if error == BluetoothError::NotSupported {
        BleError::PeripheralUnsupported
    } else {
        BleError::Gatt(format!("{context}: Bluetooth error {}", error.0))
    }
}

async fn start_server(inner: &Arc<Inner>, spec: &GattServiceSpec) -> Result<Server> {
    let result = GattServiceProvider::CreateAsync(to_guid(spec.uuid.0))
        .map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?
        .await
        .map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    let error = result.Error().map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    if error != BluetoothError::Success {
        return Err(server_error("GattServiceProvider::CreateAsync", error));
    }
    let provider = result.ServiceProvider().map_err(|err| win_err("ServiceProvider", err))?;
    let local_service = provider.Service().map_err(|err| win_err("GattServiceProvider::Service", err))?;

    let (writes_tx, writes_rx) = mpsc::unbounded_channel::<(CharacteristicUuid, GattWriteRequestedEventArgs)>();
    let mut characteristics = HashMap::new();
    let mut tokens = Vec::new();
    for spec_char in &spec.characteristics {
        let parameters = GattLocalCharacteristicParameters::new().map_err(|err| win_err("CharacteristicParameters", err))?;
        let mut properties = GattCharacteristicProperties::None;
        if spec_char.readable {
            properties |= GattCharacteristicProperties::Read;
        }
        if spec_char.writable {
            properties |= GattCharacteristicProperties::Write | GattCharacteristicProperties::WriteWithoutResponse;
        }
        if spec_char.notifiable {
            properties |= GattCharacteristicProperties::Notify;
        }
        let set = || -> windows::core::Result<()> {
            parameters.SetCharacteristicProperties(properties)?;
            parameters.SetReadProtectionLevel(GattProtectionLevel::Plain)?;
            parameters.SetWriteProtectionLevel(GattProtectionLevel::Plain)?;
            Ok(())
        };
        set().map_err(|err| win_err("CharacteristicParameters", err))?;
        if spec_char.readable {
            // Windows answers reads of a static value itself.
            parameters
                .SetStaticValue(&to_buffer(&spec_char.initial_value)?)
                .map_err(|err| win_err("SetStaticValue", err))?;
        }
        let created = local_service
            .CreateCharacteristicAsync(to_guid(spec_char.uuid.0), &parameters)
            .map_err(|err| win_err("CreateCharacteristicAsync", err))?
            .await
            .map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        let error = created.Error().map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        if error != BluetoothError::Success {
            return Err(server_error("CreateCharacteristicAsync", error));
        }
        let local = created.Characteristic().map_err(|err| win_err("Characteristic", err))?;

        let write_token = if spec_char.writable {
            let writes_tx = writes_tx.clone();
            let uuid = spec_char.uuid;
            let handler = TypedEventHandler::new(
                move |_: Ref<GattLocalCharacteristic>, args: Ref<GattWriteRequestedEventArgs>| {
                    // Queued in arrival order and handled on one task, so
                    // the fragments of a datagram stay in order.
                    let _ = writes_tx.send((uuid, args.ok()?.clone()));
                    Ok(())
                },
            );
            Some(local.WriteRequested(&handler).map_err(|err| win_err("WriteRequested", err))?)
        } else {
            None
        };
        let subscribed_token = if spec_char.notifiable {
            let weak = Arc::downgrade(inner);
            let handler = TypedEventHandler::new(move |sender: Ref<GattLocalCharacteristic>, _: Ref<IInspectable>| {
                let Some(inner) = weak.upgrade() else { return Ok(()) };
                for client in sender.ok()?.SubscribedClients()? {
                    if let Ok(session) = client.Session() {
                        inner.note_server_peer(&session);
                    }
                }
                Ok(())
            });
            Some(local.SubscribedClientsChanged(&handler).map_err(|err| win_err("SubscribedClientsChanged", err))?)
        } else {
            None
        };
        tokens.push((local.clone(), write_token, subscribed_token));
        characteristics.insert(spec_char.uuid, local);
    }
    drop(writes_tx);

    let write_task = tokio::spawn(drain_writes(Arc::downgrade(inner), writes_rx));
    let mut server = Server { provider: provider.clone(), characteristics, tokens, publisher: None, write_task };

    if let Err(err) = start_provider_advertising(&provider, spec).await {
        server.shut_down();
        return Err(err);
    }
    match start_publisher(spec).await {
        Ok(publisher) => server.publisher = publisher,
        Err(err) => {
            server.shut_down();
            return Err(err);
        }
    }
    Ok(server)
}

async fn start_provider_advertising(provider: &GattServiceProvider, spec: &GattServiceSpec) -> Result<()> {
    let parameters = GattServiceProviderAdvertisingParameters::new().map_err(|err| win_err("AdvertisingParameters", err))?;
    parameters.SetIsConnectable(true).map_err(|err| win_err("SetIsConnectable", err))?;
    parameters.SetIsDiscoverable(true).map_err(|err| win_err("SetIsDiscoverable", err))?;
    if let Some(data) = provider_service_data(spec) {
        parameters
            .SetServiceData(&to_buffer(&data)?)
            .map_err(|err| win_err("SetServiceData", err))?;
    }
    provider
        .StartAdvertisingWithParameters(&parameters)
        .map_err(|err| win_err("StartAdvertising", err))?;
    let deadline = tokio::time::Instant::now() + ADVERTISE_START_TIMEOUT;
    loop {
        let status = provider.AdvertisementStatus().map_err(|err| win_err("AdvertisementStatus", err))?;
        if status == GattServiceProviderAdvertisementStatus::Started {
            return Ok(());
        }
        if status == GattServiceProviderAdvertisementStatus::StartedWithoutAllAdvertisementData {
            log::warn!("advertise: started without all advertisement data (the service data did not fit)");
            return Ok(());
        }
        if status == GattServiceProviderAdvertisementStatus::Aborted {
            return Err(BleError::Gatt("advertise: Windows aborted the advertisement".into()));
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = provider.StopAdvertising();
            return Err(BleError::Gatt(format!("advertise: not started after {ADVERTISE_START_TIMEOUT:?} (status {})", status.0)));
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

/// A non-connectable advertisement with the service UUID, the manufacturer
/// data, and service data under other UUIDs: what `GattServiceProvider`
/// cannot send (ADR-0011 D4). `None` when the spec has none of it.
async fn start_publisher(spec: &GattServiceSpec) -> Result<Option<BluetoothLEAdvertisementPublisher>> {
    let extra_service_data: Vec<(&ServiceUuid, &Vec<u8>)> =
        spec.service_data.iter().filter(|(uuid, _)| **uuid != spec.uuid).collect();
    if spec.manufacturer_data.is_empty() && extra_service_data.is_empty() {
        return Ok(None);
    }
    let publisher = BluetoothLEAdvertisementPublisher::new().map_err(|err| win_err("AdvertisementPublisher", err))?;
    let advertisement = publisher.Advertisement().map_err(|err| win_err("Publisher::Advertisement", err))?;
    // The service UUID too, so a scanner that filters on it (every driver of
    // this library does) receives this advertisement.
    advertisement
        .ServiceUuids()
        .and_then(|uuids| uuids.Append(to_guid(spec.uuid.0)))
        .map_err(|err| win_err("Publisher::ServiceUuids", err))?;
    for (company, data) in &spec.manufacturer_data {
        let entry = BluetoothLEManufacturerData::Create(*company, &to_buffer(data)?)
            .map_err(|err| win_err("ManufacturerData", err))?;
        advertisement
            .ManufacturerData()
            .and_then(|list| list.Append(&entry))
            .map_err(|err| win_err("Publisher::ManufacturerData", err))?;
    }
    for (uuid, data) in extra_service_data {
        let mut bytes: Vec<u8> = uuid.0.as_bytes().iter().rev().copied().collect();
        bytes.extend_from_slice(data);
        let section = BluetoothLEAdvertisementDataSection::Create(AD_SERVICE_DATA_128, &to_buffer(&bytes)?)
            .map_err(|err| win_err("DataSection", err))?;
        advertisement
            .DataSections()
            .and_then(|list| list.Append(&section))
            .map_err(|err| win_err("Publisher::DataSections", err))?;
    }
    publisher
        .Start()
        .map_err(|err| BleError::Gatt(format!("advertise: the manufacturer data advertisement was refused: {err}")))?;
    let deadline = tokio::time::Instant::now() + ADVERTISE_START_TIMEOUT;
    loop {
        let status = publisher.Status().map_err(|err| win_err("Publisher::Status", err))?;
        if status == BluetoothLEAdvertisementPublisherStatus::Started {
            return Ok(Some(publisher));
        }
        if status == BluetoothLEAdvertisementPublisherStatus::Aborted {
            return Err(BleError::Gatt("advertise: Windows aborted the manufacturer data advertisement".into()));
        }
        if tokio::time::Instant::now() >= deadline {
            // `Waiting`: the radio has no free advertising slot yet. Windows
            // starts it later by itself.
            log::warn!("advertise: manufacturer data advertisement still waiting (status {})", status.0);
            return Ok(Some(publisher));
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

/// Answers inbound writes in the order they arrived and publishes them as
/// `CharacteristicWritten`.
async fn drain_writes(
    inner: Weak<Inner>, mut writes: mpsc::UnboundedReceiver<(CharacteristicUuid, GattWriteRequestedEventArgs)>,
) {
    while let Some((characteristic, args)) = writes.recv().await {
        let Some(inner) = inner.upgrade() else { return };
        if let Err(err) = handle_write(&inner, characteristic, &args).await {
            log::warn!("server: write to {} failed: {err}", characteristic.0);
        }
    }
}

async fn handle_write(inner: &Arc<Inner>, characteristic: CharacteristicUuid, args: &GattWriteRequestedEventArgs) -> windows::core::Result<()> {
    let deferral = args.GetDeferral()?;
    let outcome = async {
        let session = args.Session()?;
        let request = args.GetRequestAsync()?.await?;
        let with_response = request.Option()? == GattWriteOption::WriteWithResponse;
        let Some((address, _)) = inner.note_server_peer(&session) else {
            return Ok(());
        };
        if inner.is_excluded(&address) {
            if with_response {
                request.RespondWithProtocolError(ATT_WRITE_NOT_PERMITTED)?;
            }
            return Ok(());
        }
        let value = from_buffer(&request.Value()?)?;
        if with_response {
            request.Respond()?;
        }
        log::debug!("server: {} bytes from {address} on {}", value.len(), characteristic.0);
        let _ = inner.events_tx.send(GattEvent::CharacteristicWritten {
            peer: PeerAddress(address),
            characteristic,
            value,
        });
        Ok(())
    }
    .await;
    deferral.Complete()?;
    outcome
}

async fn notify_client(local: &GattLocalCharacteristic, value: &[u8], client: &GattSubscribedClient) -> Result<()> {
    // `IBuffer` is not `Send`, so it must be gone before the `.await`.
    let op = local
        .NotifyValueForSubscribedClientAsync(&to_buffer(value)?, client)
        .map_err(|err| win_err("NotifyValueAsync", err))?;
    let result = op.await.map_err(|err| win_err("NotifyValueAsync", err))?;
    let status = result.Status().map_err(|err| win_err("NotifyValueAsync", err))?;
    if status != GattCommunicationStatus::Success {
        return Err(communication_error("notify", status, "subscribed central"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_round_trips() {
        let address = 0x0011_2233_4455_66AB_u64 & 0xFFFF_FFFF_FFFF;
        assert_eq!(format_address(0x1122_3344_55AB), "11:22:33:44:55:AB");
        assert_eq!(parse_address("11:22:33:44:55:AB"), Some(0x1122_3344_55AB));
        assert_eq!(parse_address(&format_address(address)), Some(address));
        assert_eq!(parse_address("11:22:33"), None);
    }

    #[test]
    fn the_remote_address_is_read_from_a_device_id() {
        assert_eq!(
            address_from_device_id("BluetoothLE#BluetoothLEb4:6b:fc:1a:2b:3c-5e:aa:bb:cc:dd:ee"),
            "5E:AA:BB:CC:DD:EE"
        );
        assert_eq!(address_from_device_id("something-else"), "something-else");
    }

    #[test]
    fn a_uuid_round_trips_through_a_guid() {
        let uuid = Uuid::parse_str("6e400001-b5a3-f393-e0a9-e50e24dcca9e").unwrap();
        assert_eq!(from_guid(to_guid(uuid)), uuid);
    }
}
