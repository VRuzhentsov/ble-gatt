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

use std::collections::HashMap;
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
    notifiers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicNotifier>>>>,
    events_tx: broadcast::Sender<GattEvent>,
    app_handle: AsyncMutex<Option<ApplicationHandle>>,
    adv_handle: AsyncMutex<Option<bluer::adv::AdvertisementHandle>>,
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
            notifiers: Arc::new(AsyncMutex::new(HashMap::new())),
            events_tx,
            app_handle: AsyncMutex::new(None),
            adv_handle: AsyncMutex::new(None),
        })
    }
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

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<DiscoveredPeer>> {
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
                Some(DiscoveredPeer {
                    address: PeerAddress(address.to_string()),
                    name,
                    services: uuids.into_iter().map(ServiceUuid).collect(),
                    manufacturer_data,
                    service_data,
                    rssi,
                })
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
        device.connect().await.map_err(|err| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: err.to_string(),
        })?;
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
                    peer: watch_peer,
                    local_role: Role::Central,
                });
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
                CharacteristicWrite {
                    write: true,
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, req| {
                        let values = values.clone();
                        let events_tx = events_tx.clone();
                        let peer = PeerAddress(req.device_address.to_string());
                        Box::pin(async move {
                            values.lock().unwrap().insert(uuid, value.clone());
                            let _ = events_tx.send(GattEvent::Connected {
                                peer: peer.clone(),
                                local_role: Role::Peripheral,
                            });
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
                CharacteristicNotify {
                    notify: true,
                    method: CharacteristicNotifyMethod::Fun(Box::new(move |notifier| {
                        let notifiers = notifiers.clone();
                        Box::pin(async move {
                            notifiers.lock().await.entry(uuid).or_default().push(notifier);
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
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
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
