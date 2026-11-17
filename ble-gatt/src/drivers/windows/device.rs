//! Central role, one link: `BluetoothLEDevice`, `GattSession` and
//! `GattCharacteristic`.
//!
//! Adapted from btleplug (`src/winrtble/ble/device.rs` and
//! `src/winrtble/ble/characteristic.rs`), Copyright (c) 2020-2021,
//! Nonpolynomial, BSD-3-Clause: see `LICENSES/btleplug-BSD-3-Clause.txt`.
//! Taken from it:
//!
//! - Uncached GATT discovery falls back to the Windows cache after
//!   [`GATT_CACHE_TIMEOUT`], because some Windows Bluetooth drivers never
//!   answer an uncached request (btleplug issue #325).
//! - Link loss is `BluetoothLEDevice.ConnectionStatusChanged` to
//!   `Disconnected`.
//! - `subscribe` writes Notify or Indicate to the CCCD, whichever the
//!   characteristic has, and never leaves two `ValueChanged` handlers on one
//!   characteristic.
//! - `request_connection_priority` checks the status of the WinRT request.
//!
//! Kept from bluest instead: the address type from the scan for the dial,
//! and `GattSession.MaintainConnection`, without which Windows may drop an
//! idle link after a few seconds.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;
use windows::core::{IInspectable, Ref};
use windows::Devices::Bluetooth::GenericAttributeProfile::{
    GattCharacteristic, GattCharacteristicProperties, GattCharacteristicsResult,
    GattClientCharacteristicConfigurationDescriptorValue, GattCommunicationStatus,
    GattDeviceService, GattDeviceServicesResult, GattSession, GattSessionStatus,
    GattSessionStatusChangedEventArgs, GattValueChangedEventArgs, GattWriteOption,
};
use windows::Devices::Bluetooth::{
    BluetoothAddressType, BluetoothCacheMode, BluetoothConnectionStatus, BluetoothLEDevice,
    BluetoothLEPreferredConnectionParameters, BluetoothLEPreferredConnectionParametersRequest,
    BluetoothLEPreferredConnectionParametersRequestStatus,
};
use windows::Foundation::TypedEventHandler;

use super::utils::WindowsCodec;
use super::{Inner, NOTIFY_QUEUE_DEPTH};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    CharacteristicUuid, ConnectionPriority, GattEvent, PeerAddress, Role, WriteType,
};
use crate::hal::{BoxStream, GattConnection, DEFAULT_ATT_MTU};

/// How long an uncached discovery may take before the cached result is used
/// instead. btleplug's value.
const GATT_CACHE_TIMEOUT: Duration = Duration::from_secs(5);

/// Own each acquired object before another await can cancel construction.
#[derive(Default)]
struct AcquiringLink {
    device: Option<BluetoothLEDevice>,
    gatt_session: Option<GattSession>,
    services: Vec<GattDeviceService>,
}

impl Drop for AcquiringLink {
    fn drop(&mut self) {
        if let Some(session) = self.gatt_session.take() {
            let _ = session.SetMaintainConnection(false);
            let _ = session.Close();
        }
        for service in self.services.drain(..) {
            let _ = service.Close();
        }
        if let Some(device) = self.device.take() {
            let _ = device.Close();
        }
    }
}

/// The opened parts of one link.
pub(super) struct OpenedLink {
    device: BluetoothLEDevice,
    gatt_session: GattSession,
    services: Vec<GattDeviceService>,
    characteristics: HashMap<Uuid, GattCharacteristic>,
}

/// Open the device, hold the link with a `GattSession`, and discover every
/// characteristic. WinRT opens the radio link lazily, at the first GATT
/// request, so service discovery is what actually connects.
impl OpenedLink {
    pub(super) async fn open(
        address: u64,
        address_type: Option<BluetoothAddressType>,
    ) -> std::result::Result<OpenedLink, String> {
        let device_op = match address_type {
            Some(kind) => {
                BluetoothLEDevice::FromBluetoothAddressWithBluetoothAddressTypeAsync(address, kind)
            }
            None => BluetoothLEDevice::FromBluetoothAddressAsync(address),
        }
        .map_err(|err| format!("FromBluetoothAddressAsync: {err}"))?;
        let device = device_op
            .await
            .map_err(|err| format!("unknown device: {err}"))?;
        let mut acquired = AcquiringLink {
            device: Some(device.clone()),
            gatt_session: None,
            services: Vec::new(),
        };
        let device_id = device
            .BluetoothDeviceId()
            .map_err(|err| format!("BluetoothDeviceId: {err}"))?;
        let session_op = GattSession::FromDeviceIdAsync(&device_id)
            .map_err(|err| format!("GattSession: {err}"))?;
        let gatt_session = session_op
            .await
            .map_err(|err| format!("GattSession: {err}"))?;
        acquired.gatt_session = Some(gatt_session.clone());
        gatt_session
            .SetMaintainConnection(true)
            .map_err(|err| format!("MaintainConnection: {err}"))?;

        acquired.services = Self::discover_services(&device).await?;
        let mut characteristics = HashMap::new();
        for service in &acquired.services {
            for characteristic in Self::discover_characteristics(service).await {
                if let Ok(uuid) = characteristic.Uuid() {
                    characteristics.insert(WindowsCodec::from_guid(uuid), characteristic);
                }
            }
        }
        acquired.device.take();
        acquired.gatt_session.take();
        Ok(OpenedLink {
            device,
            gatt_session,
            services: std::mem::take(&mut acquired.services),
            characteristics,
        })
    }

    async fn services_with_cache_mode(
        device: &BluetoothLEDevice,
        mode: BluetoothCacheMode,
    ) -> windows::core::Result<GattDeviceServicesResult> {
        device.GetGattServicesWithCacheModeAsync(mode)?.await
    }

    async fn discover_services(
        device: &BluetoothLEDevice,
    ) -> std::result::Result<Vec<GattDeviceService>, String> {
        let uncached = tokio::time::timeout(
            GATT_CACHE_TIMEOUT,
            Self::services_with_cache_mode(device, BluetoothCacheMode::Uncached),
        )
        .await;
        let result = match uncached {
            Ok(result) => result,
            Err(_) => {
                log::warn!(
                    "connect: uncached service discovery timed out, using the Windows cache"
                );
                Self::services_with_cache_mode(device, BluetoothCacheMode::Cached).await
            }
        }
        .map_err(|err| format!("GetGattServicesAsync: {err}"))?;
        let status = result
            .Status()
            .map_err(|err| format!("GetGattServicesAsync: {err}"))?;
        // Check the status first. An empty `Services` list can also mean discovery failed.
        if status != GattCommunicationStatus::Success {
            return Err(match status {
                GattCommunicationStatus::Unreachable => "unreachable".to_string(),
                GattCommunicationStatus::AccessDenied => "access denied".to_string(),
                other => format!("service discovery failed (status {})", other.0),
            });
        }
        // Collected into a `Vec`: the WinRT list view is not `Send`.
        Ok(result
            .Services()
            .map_err(|err| format!("Services: {err}"))?
            .into_iter()
            .collect())
    }

    async fn characteristics_with_cache_mode(
        service: &GattDeviceService,
        mode: BluetoothCacheMode,
    ) -> windows::core::Result<GattCharacteristicsResult> {
        service
            .GetCharacteristicsWithCacheModeAsync(mode)?
            .into_future()
            .await
    }

    /// Every characteristic of `service`, or none when Windows cannot list
    /// them. btleplug also skips such a service instead of failing the
    /// connection.
    async fn discover_characteristics(service: &GattDeviceService) -> Vec<GattCharacteristic> {
        let uncached = tokio::time::timeout(
            GATT_CACHE_TIMEOUT,
            Self::characteristics_with_cache_mode(service, BluetoothCacheMode::Uncached),
        )
        .await;
        let result = match uncached {
            Ok(result) => result,
            Err(_) => {
                log::warn!(
                    "connect: uncached characteristic discovery timed out, using the Windows cache"
                );
                Self::characteristics_with_cache_mode(service, BluetoothCacheMode::Cached).await
            }
        };
        match result {
            Ok(result) if result.Status() == Ok(GattCommunicationStatus::Success) => result
                .Characteristics()
                .map(|list| list.into_iter().collect())
                .unwrap_or_default(),
            Ok(result) => {
                log::debug!(
                    "connect: characteristics not listed (status {:?})",
                    result.Status().map(|s| s.0)
                );
                Vec::new()
            }
            Err(err) => {
                log::debug!("connect: characteristics not listed: {err}");
                Vec::new()
            }
        }
    }
}

impl Inner {
    /// Report this link's loss once and free its slot in `central_links`.
    pub(super) fn link_closed(&self, address: &str, session: u64, closed: &AtomicBool) {
        if closed.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let mut links = self.central_links.lock().unwrap();
            if links.get(address) == Some(&session) {
                links.remove(address);
            }
        }
        log::info!("connection: {address} closed session={session}");
        let _ = self.events_tx.send(GattEvent::Disconnected {
            peer: PeerAddress(address.to_string()),
            local_role: Role::Central,
            session: Some(session),
        });
    }
}

/// Frees a `central_links` slot when `connect` fails or is cancelled.
pub(super) struct LinkGuard {
    pub(super) inner: Arc<Inner>,
    pub(super) address: String,
    pub(super) session: u64,
    pub(super) armed: bool,
}

impl Drop for LinkGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut links = self.inner.central_links.lock().unwrap();
            if links.get(&self.address) == Some(&self.session) {
                links.remove(&self.address);
            }
        }
    }
}

pub(super) struct WindowsGattConnection {
    inner: Arc<Inner>,
    address: String,
    session: u64,
    device: BluetoothLEDevice,
    gatt_session: GattSession,
    characteristics: HashMap<Uuid, GattCharacteristic>,
    /// `(characteristic, ValueChanged token)` per live subscription, shared
    /// with each `NotifyStream` so a dropped stream removes its own.
    subscriptions: Arc<StdMutex<Vec<(Uuid, GattCharacteristic, i64)>>>,
    /// Must stay alive: closing it withdraws the request.
    priority_request: Arc<StdMutex<Option<BluetoothLEPreferredConnectionParametersRequest>>>,
    closed: Arc<AtomicBool>,
}

impl WindowsGattConnection {
    /// Watch the opened link for loss and wrap it.
    pub(super) fn new(inner: Arc<Inner>, address: String, session: u64, link: OpenedLink) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let connection_token = {
            let weak = Arc::downgrade(&inner);
            let closed = closed.clone();
            let watched = address.clone();
            let handler = TypedEventHandler::new(
                move |sender: Ref<BluetoothLEDevice>, _: Ref<IInspectable>| {
                    if sender.ok()?.ConnectionStatus()? == BluetoothConnectionStatus::Disconnected {
                        if let Some(inner) = weak.upgrade() {
                            inner.link_closed(&watched, session, &closed);
                        }
                    }
                    Ok(())
                },
            );
            link.device.ConnectionStatusChanged(&handler).ok()
        };
        // A second signal: the session closes when Windows gives up on the
        // link even if no connection status change was raised.
        let session_token = {
            let weak = Arc::downgrade(&inner);
            let closed = closed.clone();
            let watched = address.clone();
            let handler = TypedEventHandler::new(
                move |_: Ref<GattSession>, args: Ref<GattSessionStatusChangedEventArgs>| {
                    if args.ok()?.Status()? == GattSessionStatus::Closed {
                        if let Some(inner) = weak.upgrade() {
                            inner.link_closed(&watched, session, &closed);
                        }
                    }
                    Ok(())
                },
            );
            link.gatt_session.SessionStatusChanged(&handler).ok()
        };
        let subscriptions = Arc::new(StdMutex::new(Vec::<(Uuid, GattCharacteristic, i64)>::new()));
        let weak = Arc::downgrade(&inner);
        let watched = address.clone();
        let device = link.device.clone();
        let gatt_session = link.gatt_session.clone();
        let services = link.services;
        let registered = subscriptions.clone();
        let released = closed.clone();
        let priority_request = Arc::new(StdMutex::new(
            None::<BluetoothLEPreferredConnectionParametersRequest>,
        ));
        let priority_cleanup = priority_request.clone();
        inner.central_cleanup.lock().unwrap().insert(
            session,
            Box::new(move || {
                if let Some(request) = priority_cleanup.lock().unwrap().take() {
                    let _ = request.Close();
                }
                for (_, characteristic, token) in registered.lock().unwrap().drain(..) {
                    let _ = characteristic.RemoveValueChanged(token);
                }
                if let Some(token) = connection_token {
                    let _ = device.RemoveConnectionStatusChanged(token);
                }
                if let Some(token) = session_token {
                    let _ = gatt_session.RemoveSessionStatusChanged(token);
                }
                let _ = gatt_session.SetMaintainConnection(false);
                let _ = gatt_session.Close();
                for service in services {
                    let _ = service.Close();
                }
                let _ = device.Close();
                if let Some(inner) = weak.upgrade() {
                    inner.link_closed(&watched, session, &released);
                } else {
                    released.store(true, Ordering::SeqCst);
                }
            }),
        );
        Self {
            inner,
            address,
            session,
            device: link.device,
            gatt_session: link.gatt_session,
            characteristics: link.characteristics,
            subscriptions,
            priority_request,
            closed,
        }
    }

    pub(super) fn mtu(&self) -> u16 {
        self.gatt_session
            .MaxPduSize()
            .unwrap_or(DEFAULT_ATT_MTU)
            .max(DEFAULT_ATT_MTU)
    }

    fn characteristic(&self, characteristic: CharacteristicUuid) -> Result<&GattCharacteristic> {
        if self.closed.load(Ordering::SeqCst)
            || self.inner.central_links.lock().unwrap().get(&self.address) != Some(&self.session)
        {
            return Err(BleError::NotConnected(self.address.clone()));
        }
        self.characteristics.get(&characteristic.0).ok_or_else(|| {
            BleError::Gatt(format!(
                "characteristic {} not found on peer",
                characteristic.0
            ))
        })
    }

    fn release(&mut self) {
        let cleanup = self
            .inner
            .central_cleanup
            .lock()
            .unwrap()
            .remove(&self.session);
        if let Some(release) = cleanup {
            release();
        }
        self.characteristics.clear();
    }
}

/// Select Notify, then Indicate, then `None`, based on the characteristic
/// properties (btleplug's `to_descriptor_value`).
impl WindowsGattConnection {
    fn descriptor_value(
        properties: GattCharacteristicProperties,
    ) -> GattClientCharacteristicConfigurationDescriptorValue {
        if (properties & GattCharacteristicProperties::Notify) != GattCharacteristicProperties::None
        {
            GattClientCharacteristicConfigurationDescriptorValue::Notify
        } else if (properties & GattCharacteristicProperties::Indicate)
            != GattCharacteristicProperties::None
        {
            GattClientCharacteristicConfigurationDescriptorValue::Indicate
        } else {
            GattClientCharacteristicConfigurationDescriptorValue::None
        }
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
        self.mtu()
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let target = self.characteristic(characteristic)?.clone();
        let op = target
            .ReadValueWithCacheModeAsync(BluetoothCacheMode::Uncached)
            .map_err(|err| BleError::windows_error("ReadValueAsync", err))?;
        let result = op
            .await
            .map_err(|err| BleError::windows_error("ReadValueAsync", err))?;
        let status = result
            .Status()
            .map_err(|err| BleError::windows_error("ReadValueAsync", err))?;
        if status != GattCommunicationStatus::Success {
            return Err(BleError::communication_error("read", status, &self.address));
        }
        WindowsCodec::from_buffer(
            &result
                .Value()
                .map_err(|err| BleError::windows_error("ReadValueAsync", err))?,
        )
        .map_err(|err| BleError::windows_error("ReadValueAsync", err))
    }

    async fn write_with_type(
        &mut self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> Result<()> {
        let target = self.characteristic(characteristic)?.clone();
        let option = match write_type {
            WriteType::WithResponse => GattWriteOption::WriteWithResponse,
            WriteType::WithoutResponse => GattWriteOption::WriteWithoutResponse,
        };
        // `IBuffer` is not `Send`, so it must be gone before the `.await`.
        let op = target
            .WriteValueWithResultAndOptionAsync(&WindowsCodec::to_buffer(&value)?, option)
            .map_err(|err| BleError::windows_error("WriteValueAsync", err))?;
        let result = op
            .await
            .map_err(|err| BleError::windows_error("WriteValueAsync", err))?;
        let status = result
            .Status()
            .map_err(|err| BleError::windows_error("WriteValueAsync", err))?;
        if status != GattCommunicationStatus::Success {
            return Err(BleError::communication_error(
                "write",
                status,
                &self.address,
            ));
        }
        Ok(())
    }

    async fn subscribe(
        &mut self,
        characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        let target = self.characteristic(characteristic)?.clone();
        let config = Self::descriptor_value(
            target
                .CharacteristicProperties()
                .map_err(|err| BleError::windows_error("subscribe", err))?,
        );
        if config == GattClientCharacteristicConfigurationDescriptorValue::None {
            return Err(BleError::Gatt(format!(
                "characteristic {} has neither notify nor indicate",
                characteristic.0
            )));
        }
        // A new subscription replaces the old one: never two handlers.
        {
            let mut subscriptions = self.subscriptions.lock().unwrap();
            subscriptions.retain(|(uuid, old, token)| {
                if *uuid == characteristic.0 {
                    let _ = old.RemoveValueChanged(*token);
                    false
                } else {
                    true
                }
            });
        }
        let (tx, rx) = mpsc::channel(NOTIFY_QUEUE_DEPTH);
        let overflow = Arc::new(AtomicBool::new(false));
        // Registered before the CCCD write, so a notification sent right
        // after the peer accepts the subscription is not missed. The handler
        // object is not `Send`; only its token outlives this block.
        let token = {
            let overflow = overflow.clone();
            let handler = TypedEventHandler::new(
                move |_: Ref<GattCharacteristic>, args: Ref<GattValueChangedEventArgs>| {
                    let bytes = WindowsCodec::from_buffer(&args.ok()?.CharacteristicValue()?)?;
                    if tx.try_send(bytes).is_err() {
                        overflow.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                },
            );
            target
                .ValueChanged(&handler)
                .map_err(|err| BleError::windows_error("ValueChanged", err))?
        };
        let mut registration = ValueChangedGuard {
            characteristic: target.clone(),
            token: Some(token),
        };
        let subscription_status =
            match target.WriteClientCharacteristicConfigurationDescriptorWithResultAsync(config) {
                Ok(op) => op.await.and_then(|result| result.Status()),
                Err(err) => Err(err),
            };
        match subscription_status {
            Ok(GattCommunicationStatus::Success) => {}
            Ok(status) => {
                return Err(BleError::communication_error(
                    "subscribe",
                    status,
                    &self.address,
                ));
            }
            Err(err) => {
                return Err(BleError::windows_error("subscribe", err));
            }
        }
        // Close invalidates this slot before draining callbacks. Keep validation and publication atomic with it.
        let links = self.inner.central_links.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || links.get(&self.address) != Some(&self.session) {
            return Err(BleError::NotConnected(self.address.clone()));
        }
        self.subscriptions
            .lock()
            .unwrap()
            .push((characteristic.0, target.clone(), token));
        registration.token.take();
        drop(links);
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
    /// and later. Earlier Windows, or a refused request, reports
    /// [`BleError::Unsupported`].
    async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        let links = self.inner.central_links.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || links.get(&self.address) != Some(&self.session) {
            return Err(BleError::NotConnected(self.address.clone()));
        }
        let unsupported = |err: windows::core::Error| {
            BleError::Unsupported(format!("connection priority on this Windows: {err}"))
        };
        let parameters = match priority {
            ConnectionPriority::LowPower => {
                BluetoothLEPreferredConnectionParameters::PowerOptimized()
            }
            ConnectionPriority::Balanced => BluetoothLEPreferredConnectionParameters::Balanced(),
            ConnectionPriority::High => {
                BluetoothLEPreferredConnectionParameters::ThroughputOptimized()
            }
        }
        .map_err(unsupported)?;
        let request = self
            .device
            .RequestPreferredConnectionParameters(&parameters)
            .map_err(unsupported)?;
        let status = match request.Status() {
            Ok(status) => status,
            Err(error) => {
                let _ = request.Close();
                return Err(unsupported(error));
            }
        };
        if status != BluetoothLEPreferredConnectionParametersRequestStatus::Success {
            let _ = request.Close();
            return Err(BleError::Unsupported(format!(
                "connection priority refused (status {})",
                status.0
            )));
        }
        if let Some(previous) = self.priority_request.lock().unwrap().replace(request) {
            let _ = previous.Close();
        }
        drop(links);
        Ok(())
    }
}

/// Remove only this callback if setup fails or its future is cancelled.
struct ValueChangedGuard {
    characteristic: GattCharacteristic,
    token: Option<i64>,
}

impl Drop for ValueChangedGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self.characteristic.RemoveValueChanged(token);
        }
    }
}

struct NotifyStream {
    rx: ReceiverStream<Vec<u8>>,
    overflow: Arc<AtomicBool>,
    characteristic: GattCharacteristic,
    token: i64,
    subscriptions: Arc<StdMutex<Vec<(Uuid, GattCharacteristic, i64)>>>,
}

impl tokio_stream::Stream for NotifyStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.overflow.swap(false, Ordering::SeqCst) {
            return std::task::Poll::Ready(Some(Err(BleError::notification_receive_gap())));
        }
        std::pin::Pin::new(&mut self.rx)
            .poll_next(cx)
            .map(|item| item.map(Ok))
    }
}

impl Drop for NotifyStream {
    fn drop(&mut self) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if let Some(index) = subscriptions.iter().position(|(_, characteristic, token)| {
            characteristic == &self.characteristic && *token == self.token
        }) {
            subscriptions.remove(index);
            let _ = self.characteristic.RemoveValueChanged(self.token);
        }
    }
}
