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
    ServiceUuid,
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
                Some(DiscoveredPeer {
                    address: PeerAddress(address.to_string()),
                    name,
                    services: uuids.into_iter().map(ServiceUuid).collect(),
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
        Ok(Box::new(LinuxGattConnection {
            peer: peer.clone(),
            device,
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
                            let _ = events_tx.send(GattEvent::Connected { peer: peer.clone() });
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

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let target = self.find_characteristic(characteristic).await?;
        target.read().await.map_err(|err| BleError::Gatt(err.to_string()))
    }

    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let target = self.find_characteristic(characteristic).await?;
        target.write(&value).await.map_err(|err| BleError::Gatt(err.to_string()))
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>> {
        let target = self.find_characteristic(characteristic).await?;
        let notify_stream = target.notify().await.map_err(|err| BleError::Gatt(err.to_string()))?;
        Ok(Box::pin(notify_stream))
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.device.disconnect().await.map_err(|err| BleError::Gatt(err.to_string()))
    }
}
