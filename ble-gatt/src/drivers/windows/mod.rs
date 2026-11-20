//! Windows driver: WinRT `Windows.Devices.Bluetooth` through Microsoft's
//! `windows` crate (layer: Frameworks & Drivers; see `docs/architecture.md`).
//! Design and open questions: `docs/adr/0011-windows-winrt-driver.md`.
//!
//! | Module | Job |
//! |---|---|
//! | `watcher` | Central: scanning |
//! | `device` | Central: one link, its GATT client operations |
//! | `peripheral` | Peripheral: GATT server and advertising |
//! | `publisher` | Peripheral: the fast advertisement next to the GATT server |
//! | `utils` | WinRT ↔ `crate::entities` conversions |
//! | this file | The `Driver` port: sessions, events, radio |
//!
//! WinRT calls event handlers on its own thread pool, never on a Tokio
//! thread. Handlers capture a `Weak<Inner>` so a dropped driver is not kept
//! alive by a registration, and only do work that cannot block: they push
//! onto channels, and Tokio tasks do the rest. Some `windows` types
//! (`IBuffer`, handler objects) are not `Send`; none is held across an
//! `.await`.

mod device;
mod peripheral;
mod publisher;
mod utils;
mod watcher;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use crate::drivers::event_bus::EventBus;
use async_trait::async_trait;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use windows::core::{IInspectable, Ref};
use windows::Devices::Bluetooth::{BluetoothAdapter, BluetoothAddressType};
use windows::Devices::Radios::Radio;
use windows::Foundation::TypedEventHandler;

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    RadioStatus, Role, ServiceUuid,
};
use crate::hal::{BoxStream, Driver, GattConnection};

use peripheral::{Server, ServerPeer};
use utils::WindowsCodec;

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
    events_tx: EventBus,
    next_id: AtomicU64,
    /// Address type per address seen in a scan. Android advertises from a
    /// random address, and WinRT needs to be told the type to dial it.
    address_types: StdMutex<HashMap<u64, BluetoothAddressType>>,
    scan: StdMutex<ScanState>,
    /// Session of each live or pending central-role link, so a second dial
    /// to the same peer is refused rather than opening a second
    /// `BluetoothLEDevice`.
    central_links: StdMutex<HashMap<String, u64>>,
    central_cleanup: StdMutex<HashMap<u64, Box<dyn FnOnce() + Send>>>,
    /// Serialize server replacement, advertisement updates, visibility changes and teardown.
    server_lock: tokio::sync::Mutex<()>,
    server: StdMutex<Option<Server>>,
    server_generation: StdMutex<u64>,
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
        let adapter = Self::default_adapter().await?;
        if !adapter.IsLowEnergySupported().unwrap_or(false) {
            return Err(BleError::AdapterUnavailable(
                "the Bluetooth adapter has no Bluetooth LE".into(),
            ));
        }
        let radio = match adapter.GetRadioAsync() {
            Ok(op) => op.await.ok(),
            Err(_) => None,
        };
        let events_tx = EventBus::new(EVENT_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            events_tx,
            next_id: AtomicU64::new(1),
            address_types: StdMutex::new(HashMap::new()),
            scan: StdMutex::new(ScanState {
                generation: 0,
                active: false,
            }),
            central_links: StdMutex::new(HashMap::new()),
            central_cleanup: StdMutex::new(HashMap::new()),
            server_lock: tokio::sync::Mutex::new(()),
            server: StdMutex::new(None),
            server_generation: StdMutex::new(0),
            served_service: StdMutex::new(None),
            server_peers: StdMutex::new(HashMap::new()),
            radio,
        });
        let radio_token = inner.radio.as_ref().and_then(|radio| {
            let weak = Arc::downgrade(&inner);
            let last = StdMutex::new(RadioStatus::from_windows(radio.State()));
            let handler =
                TypedEventHandler::new(move |sender: Ref<Radio>, _: Ref<IInspectable>| {
                    let status = RadioStatus::from_windows(sender.ok()?.State());
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
        log::info!(
            "windows driver: ready, radio {:?}",
            inner
                .radio
                .as_ref()
                .map(|r| RadioStatus::from_windows(r.State()))
        );
        Ok(Self { inner, radio_token })
    }
    async fn default_adapter() -> Result<BluetoothAdapter> {
        let op = BluetoothAdapter::GetDefaultAsync().map_err(|err| {
            BleError::AdapterUnavailable(format!("BluetoothAdapter::GetDefaultAsync: {err}"))
        })?;
        // WinRT returns a null adapter, which `windows` reports as an error,
        // when the computer has no Bluetooth adapter.
        op.await
            .map_err(|err| BleError::AdapterUnavailable(format!("no Bluetooth adapter: {err}")))
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

#[async_trait]
impl Driver for WindowsDriver {
    /// What the default adapter reports (`IsCentralRoleSupported`,
    /// `IsPeripheralRoleSupported`), and `false` for both when there is no
    /// adapter or no Bluetooth LE (ADR-0011 D2).
    async fn capabilities(&self) -> CapabilityReport {
        let adapter = match Self::default_adapter().await {
            Ok(adapter) => adapter,
            Err(err) => {
                log::error!("capabilities: {err}");
                return CapabilityReport::default();
            }
        };
        let supports_low_energy = adapter.IsLowEnergySupported().unwrap_or(false);
        let central = supports_low_energy && adapter.IsCentralRoleSupported().unwrap_or(false);
        let peripheral =
            supports_low_energy && adapter.IsPeripheralRoleSupported().unwrap_or(false);
        log::info!(
            "capabilities: le={supports_low_energy} central={central} peripheral={peripheral}"
        );
        CapabilityReport {
            central,
            peripheral,
        }
    }

    // --- Central role ---

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let generation = {
            let mut scan = self.inner.scan.lock().unwrap();
            if scan.active {
                return Err(BleError::Gatt(
                    "a scan is already active; concurrent scans are not supported".into(),
                ));
            }
            scan.active = true;
            scan.generation += 1;
            scan.generation
        };
        self.inner.pause_publisher(true);
        match watcher::ScanStream::start(&self.inner, service, generation) {
            Ok(stream) => {
                log::info!(
                    "scan: started for service {} generation={generation}",
                    service.0
                );
                Ok(Box::pin(stream))
            }
            Err(err) => {
                let ended = {
                    let mut scan = self.inner.scan.lock().unwrap();
                    let ended = scan.generation == generation;
                    if ended {
                        scan.active = false;
                    }
                    ended
                };
                if ended {
                    self.inner.pause_publisher(false);
                }
                Err(err)
            }
        }
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let failed = |reason: String| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason,
        };
        let address = WindowsCodec::parse_address(&peer.0)
            .ok_or_else(|| failed("not a Bluetooth address".into()))?;
        let session = self.inner.next_id();
        {
            let mut links = self.inner.central_links.lock().unwrap();
            if links.contains_key(&peer.0) {
                return Err(failed(
                    "a connection to this peer is already open or in progress".into(),
                ));
            }
            links.insert(peer.0.clone(), session);
        }
        // Release the pending connection slot on failure or cancellation.
        let mut pending_link_guard = device::LinkGuard {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            session,
            armed: true,
        };

        let address_type = self
            .inner
            .address_types
            .lock()
            .unwrap()
            .get(&address)
            .copied();
        let link = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            device::OpenedLink::open(address, address_type),
        )
        .await
        {
            Err(_) => return Err(failed(format!("no answer in {CONNECT_TIMEOUT:?}"))),
            Ok(Err(reason)) => return Err(failed(reason)),
            Ok(Ok(link)) => link,
        };
        let connection =
            device::WindowsGattConnection::new(self.inner.clone(), peer.0.clone(), session, link);
        let links = self.inner.central_links.lock().unwrap();
        if links.get(&peer.0) != Some(&session) {
            drop(links);
            return Err(failed(
                "the connection acquisition was cancelled by close".into(),
            ));
        }
        pending_link_guard.armed = false;
        log::info!(
            "connect: {} connected session={session} mtu={}",
            peer.0,
            connection.mtu()
        );
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
        let _server_guard = self.inner.server_lock.lock().await;
        if let Some(old) = self.inner.server.lock().unwrap().take() {
            log::info!("advertise: replacing the served service");
            old.shut_down();
        }
        self.inner.forget_server_peers();
        *self.inner.served_service.lock().unwrap() = None;

        let server = self.inner.start_server(&service).await?;
        *self.inner.server.lock().unwrap() = Some(server);
        *self.inner.served_service.lock().unwrap() = Some(service.clone());
        log::info!("advertise: serving {}", service.uuid.0);
        Ok(())
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        service.validate()?;
        let _server_guard = self.inner.server_lock.lock().await;
        let not_advertising =
            || BleError::Gatt("update_advertisement: nothing is being advertised".into());
        let served = self
            .inner
            .served_service
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(not_advertising)?;
        if !served.same_service(&service) {
            return Err(BleError::Gatt(
                "update_advertisement: the spec describes a different service".into(),
            ));
        }
        let (provider, desired_visibility) = {
            let server = self.inner.server.lock().unwrap();
            let server = server.as_ref().ok_or_else(not_advertising)?;
            (server.provider.clone(), server.desired_visibility)
        };
        // Restart the provider advertisement only when its service data changes.
        // The GATT advertisement carries manufacturer data on Windows (ADR-0012).
        if desired_visibility && served.provider_service_data() != service.provider_service_data() {
            provider
                .StopAdvertising()
                .map_err(|err| BleError::windows_error("StopAdvertising", err))?;
            Server::start_provider_advertising(&provider, &service).await?;
        }
        if desired_visibility {
            self.inner.republish(&service).await;
        }
        *self.inner.served_service.lock().unwrap() = Some(service);
        log::info!("update_advertisement: applied");
        Ok(())
    }

    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service.lock().unwrap().clone()
    }

    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
        let server = self.inner.server.lock().unwrap();
        server
            .as_ref()
            .ok_or_else(|| BleError::Gatt("set_characteristic_value: nothing is served".into()))?
            .set_value(characteristic, value)
    }

    async fn set_advertising_enabled(&self, enabled: bool) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
        let service = self
            .inner
            .served_service
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| BleError::Gatt("advertising visibility: nothing is served".into()))?;
        let (provider, was_visible) = {
            let mut server = self.inner.server.lock().unwrap();
            let server = server
                .as_mut()
                .ok_or_else(|| BleError::Gatt("advertising visibility: no GATT server".into()))?;
            let was_visible = server.desired_visibility;
            if enabled {
                server.desired_visibility = true;
            }
            (server.provider.clone(), was_visible)
        };
        Server::set_provider_advertising(&provider, &service, enabled, was_visible).await?;
        if !enabled {
            if let Some(server) = self.inner.server.lock().unwrap().as_mut() {
                server.desired_visibility = false;
                server.publisher = None;
            }
        } else {
            self.inner.republish(&service).await;
        }
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
        if let Some(server) = self.inner.server.lock().unwrap().take() {
            server.shut_down();
            log::info!("stop_advertising: stopped");
        }
        self.inner.forget_server_peers();
        *self.inner.served_service.lock().unwrap() = None;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let stopped = self.stop_advertising().await;
        self.inner.central_links.lock().unwrap().clear();
        let cleanup: Vec<_> = self
            .inner
            .central_cleanup
            .lock()
            .unwrap()
            .drain()
            .map(|(_, cleanup)| cleanup)
            .collect();
        for release in cleanup {
            release();
        }
        stopped
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
        let clients = self.inner.subscribed_clients(characteristic)?;
        let local = self.inner.local_characteristic(characteristic)?;
        let mut delivered = 0usize;
        for (address, client) in clients {
            if self.inner.is_excluded(&address) {
                continue;
            }
            if ServerPeer::notify_client(&local, &value, &client)
                .await
                .is_ok()
            {
                delivered += 1;
            }
        }
        if delivered == 0 {
            return Err(BleError::Gatt(format!(
                "notify {}: no subscribed central received it",
                characteristic.0
            )));
        }
        Ok(())
    }

    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
        {
            let peers = self.inner.server_peers.lock().unwrap();
            let current = peers
                .get(&peer.0)
                .ok_or_else(|| BleError::NotConnected(peer.0.clone()))?;
            if session.is_some_and(|session| session != current.session) {
                return Err(BleError::NotConnected(format!(
                    "{} (session {session:?} has ended)",
                    peer.0
                )));
            }
            if current.excluded {
                return Err(BleError::NotConnected(format!(
                    "{} (excluded by this server)",
                    peer.0
                )));
            }
        }
        let client = self
            .inner
            .subscribed_clients(characteristic)?
            .into_iter()
            .find(|(address, _)| *address == peer.0)
            .map(|(_, client)| client)
            .ok_or_else(|| {
                BleError::NotConnected(format!(
                    "{} is not subscribed to {}",
                    peer.0, characteristic.0
                ))
            })?;
        let local = self.inner.local_characteristic(characteristic)?;
        ServerPeer::notify_client(&local, &value, &client).await
    }

    /// Excludes the central: WinRT has no call that drops a central from a
    /// GATT server (ADR-0011, native events and cleanup limits).
    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        let _server_guard = self.inner.server_lock.lock().await;
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

    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.inner.events_tx.lifecycle_events()
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let stream =
            BroadcastStream::new(self.inner.events_tx.subscribe()).map(|item| match item {
                Ok(event) => event,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(dropped)) => {
                    GattEvent::Lagged { dropped }
                }
            });
        Box::pin(stream)
    }

    async fn radio_status(&self) -> RadioStatus {
        match &self.inner.radio {
            Some(radio) => RadioStatus::from_windows(radio.State()),
            None => RadioStatus::Unsupported,
        }
    }
}
