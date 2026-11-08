//! Windows driver: WinRT `Windows.Devices.Bluetooth` through Microsoft's
//! `windows` crate (layer: Frameworks & Drivers; see `docs/architecture.md`).
//! Design and open questions: `docs/adr/0011-windows-winrt-driver.md`.
//!
//! The split follows btleplug's `src/winrtble/`:
//!
//! | Module | Job | Source |
//! |---|---|---|
//! | `watcher` | Central: scanning | adapted from btleplug `ble/watcher.rs` |
//! | `device` | Central: one link, its GATT client operations | adapted from btleplug `ble/device.rs`, `ble/characteristic.rs` |
//! | `peripheral` | Peripheral: GATT server and advertising | this library's own (btleplug has no peripheral role) |
//! | `utils` | WinRT ↔ `crate::entities` conversions | partly adapted from btleplug |
//! | this file | The `Driver` port: sessions, events, radio | this library's own |
//!
//! btleplug is Copyright (c) 2020-2021, Nonpolynomial, BSD-3-Clause; see
//! `LICENSES/btleplug-BSD-3-Clause.txt`.
//!
//! WinRT calls event handlers on its own thread pool, never on a Tokio
//! thread. Handlers capture a `Weak<Inner>` so a dropped driver is not kept
//! alive by a registration, and only do work that cannot block: they push
//! onto channels, and Tokio tasks do the rest. Some `windows` types
//! (`IBuffer`, handler objects) are not `Send`; none is held across an
//! `.await`.

mod device;
mod peripheral;
mod utils;
mod watcher;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use windows::core::{IInspectable, Ref};
use windows::Devices::Bluetooth::{BluetoothAdapter, BluetoothAddressType};
use windows::Devices::Radios::Radio;
use windows::Foundation::TypedEventHandler;

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress, RadioStatus,
    Role, ServiceUuid,
};
use crate::hal::{BoxStream, Driver, GattConnection};

use peripheral::{ServerPeer, Server};
use utils::{parse_address, radio_status_of};

/// Scan results buffered for a slow consumer before new ones are dropped.
/// Same depth as the Android driver.
const DISCOVERY_QUEUE_DEPTH: usize = 64;

/// Inbound notifications buffered per subscription. Same depth as the
/// Android driver.
const NOTIFY_QUEUE_DEPTH: usize = 256;

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// How long `connect` waits in total. WinRT has its own, shorter, internal
/// timeouts; this only bounds a call that never returns.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long `advertise` waits for WinRT to report that an advertisement
/// started or was aborted.
const ADVERTISE_START_TIMEOUT: Duration = Duration::from_secs(5);

const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
    /// Session of each live or pending central-role link, so a second dial
    /// to the same peer is refused rather than opening a second
    /// `BluetoothLEDevice`.
    central_links: StdMutex<HashMap<String, u64>>,
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
            let last = StdMutex::new(radio_status_of(radio.State()));
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
    op.await.map_err(|err| BleError::AdapterUnavailable(format!("no Bluetooth adapter: {err}")))
}

#[async_trait]
impl Driver for WindowsDriver {
    /// What the default adapter reports (`IsCentralRoleSupported`,
    /// `IsPeripheralRoleSupported`), and `false` for both when there is no
    /// adapter or no Bluetooth LE (ADR-0011 D2).
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

    // --- Central role ---

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
        match watcher::start(&self.inner, service, generation) {
            Ok(stream) => {
                log::info!("scan: started for service {} generation={generation}", service.0);
                Ok(Box::pin(stream))
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
            links.insert(peer.0.clone(), session);
        }
        // Frees the slot on every failure below, including a cancelled future.
        let mut guard =
            device::LinkGuard { inner: self.inner.clone(), address: peer.0.clone(), session, armed: true };

        let address_type = self.inner.address_types.lock().unwrap().get(&address).copied();
        let link = match tokio::time::timeout(CONNECT_TIMEOUT, device::open(address, address_type)).await {
            Err(_) => return Err(failed(format!("no answer in {CONNECT_TIMEOUT:?}"))),
            Ok(Err(reason)) => return Err(failed(reason)),
            Ok(Ok(link)) => link,
        };
        let connection = device::WindowsGattConnection::new(self.inner.clone(), peer.0.clone(), session, link);
        guard.armed = false;
        log::info!("connect: {} connected session={session} mtu={}", peer.0, connection.mtu());
        let _ = self.inner.events_tx.send(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Central,
            session: Some(session),
        });
        Ok(Box::new(connection))
    }

    // --- Peripheral role ---

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        let _serialise = self.inner.server_lock.lock().await;
        if let Some(old) = self.inner.server.lock().unwrap().take() {
            log::info!("advertise: replacing the served service");
            old.shut_down();
        }
        self.inner.forget_server_peers();
        *self.inner.served_service.lock().unwrap() = None;

        let server = peripheral::start_server(&self.inner, &service).await?;
        *self.inner.server.lock().unwrap() = Some(server);
        *self.inner.served_service.lock().unwrap() = Some(service.clone());
        log::info!("advertise: serving {}", service.uuid.0);
        Ok(())
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        let _serialise = self.inner.server_lock.lock().await;
        let not_advertising = || BleError::Gatt("update_advertisement: nothing is being advertised".into());
        let served = self.inner.served_service.lock().unwrap().clone().ok_or_else(not_advertising)?;
        if !served.same_service(&service) {
            return Err(BleError::Gatt("update_advertisement: the spec describes a different service".into()));
        }
        let (provider, old_publisher) = {
            let mut server = self.inner.server.lock().unwrap();
            let server = server.as_mut().ok_or_else(not_advertising)?;
            (server.provider.clone(), server.publisher.take())
        };
        if let Some(publisher) = old_publisher {
            let _ = publisher.Stop();
        }
        // The provider's advertisement only carries the name (or the service
        // data under the served UUID); restart it only when that changed.
        if peripheral::provider_service_data(&served) != peripheral::provider_service_data(&service) {
            let _ = provider.StopAdvertising();
            peripheral::start_provider_advertising(&provider, &service).await?;
        }
        let publisher = peripheral::start_publisher(&service).await?;
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
            if peripheral::notify_client(&local, &value, &client).await.is_ok() {
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
        peripheral::notify_client(&local, &value, &client).await
    }

    /// Excludes the central: WinRT has no call that drops a central from a
    /// GATT server (ADR-0011 D6).
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

    // --- Lifecycle ---

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
