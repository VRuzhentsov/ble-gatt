//! Peripheral role: `GattServiceProvider` serves the GATT service and sends
//! its connectable advertisement.
//!
//! btleplug has no peripheral role, so this part is this library's own,
//! shaped like the GATT servers of the Linux and Android drivers and like
//! Google Nearby's Windows GATT server (`google/nearby`,
//! `internal/platform/implementation/windows/ble_gatt_server.cc`).
//!
//! The provider's advertisement carries the service UUID and one service
//! data entry, nothing else. Manufacturer data and a name do not fit
//! (ADR-0011 D4); they reach scanners through the GATT advertisement
//! instead (ADR-0012), a readable characteristic whose value
//! `set_characteristic_value` replaces.

use std::collections::HashMap;
#[path = "advertising_start.rs"]
mod advertising_start;
use std::sync::{Arc, Mutex as StdMutex, Weak};

use tokio::sync::mpsc;
use windows::core::{IInspectable, Ref};
use windows::Devices::Bluetooth::BluetoothError;
use windows::Devices::Bluetooth::GenericAttributeProfile::{
    GattCharacteristicProperties, GattCommunicationStatus, GattLocalCharacteristic,
    GattLocalCharacteristicParameters, GattProtectionLevel, GattReadRequestedEventArgs, GattServiceProvider,
    GattServiceProviderAdvertisementStatus, GattServiceProviderAdvertisingParameters, GattSession,
    GattSessionStatus, GattSessionStatusChangedEventArgs, GattSubscribedClient, GattWriteOption,
    GattWriteRequestedEventArgs,
};
use windows::Foundation::TypedEventHandler;

use super::utils::{communication_error, from_buffer, session_address, to_buffer, to_guid, win_buffer, win_err};
use super::{Inner, ADVERTISE_START_TIMEOUT, STATUS_POLL_INTERVAL};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    truncate_name, CharacteristicUuid, GattEvent, GattServiceSpec, PeerAddress, Role, NAME_SERVICE_DATA_MAX_LEN,
};

/// ATT error "Write Not Permitted" (Bluetooth Core, Vol 3, Part F, 3.4.1.1):
/// the reply to a write from a central that `disconnect_peer` excluded.
const ATT_WRITE_NOT_PERMITTED: u8 = 0x03;

/// ATT error "Invalid Offset": a long read past the end of the value.
const ATT_INVALID_OFFSET: u8 = 0x07;

/// A central attached to the local GATT server.
pub(super) struct ServerPeer {
    pub(super) session: u64,
    /// Held so its `SessionStatusChanged` registration stays alive.
    gatt_session: GattSession,
    status_token: i64,
    /// Set by `disconnect_peer`. WinRT has no call that drops a central
    /// from a GATT server, so an excluded central stays connected until it
    /// leaves; meanwhile it gets no notifications and its writes are
    /// refused (ADR-0011 D6).
    pub(super) excluded: bool,
}

/// One inbound request, queued so a single task answers them in order.
enum ServerRequest {
    Write(CharacteristicUuid, GattWriteRequestedEventArgs),
    Read(CharacteristicUuid, GattReadRequestedEventArgs),
}

/// `(characteristic, ReadRequested, WriteRequested, SubscribedClientsChanged)` tokens.
type Tokens = (GattLocalCharacteristic, Option<i64>, Option<i64>, Option<i64>);

/// The live GATT server.
pub(super) struct Server {
    inner: Weak<Inner>,
    generation: u64,
    pub(super) provider: GattServiceProvider,
    pub(super) desired_visibility: bool,
    characteristics: HashMap<CharacteristicUuid, GattLocalCharacteristic>,
    /// What reads return, per readable characteristic.
    values: Arc<StdMutex<HashMap<CharacteristicUuid, Vec<u8>>>>,
    tokens: Vec<Tokens>,
    /// Answers reads and writes in arrival order.
    request_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut current = inner.server_generation.lock().unwrap();
            if *current == self.generation {
                *current = inner.next_id();
                inner.forget_server_peers();
            }
        }
        if let Some(task) = self.request_task.take() {
            task.abort();
        }
        for (characteristic, read, write, subscribed) in &self.tokens {
            if let Some(token) = read {
                let _ = characteristic.RemoveReadRequested(*token);
            }
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

impl Server {
    pub(super) fn shut_down(self) {
        drop(self);
    }

    /// `Driver::set_characteristic_value`.
    pub(super) fn set_value(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let mut values = self.values.lock().unwrap();
        let stored_value = values
            .get_mut(&characteristic)
            .ok_or_else(|| BleError::Gatt(format!("characteristic {} is not served as readable", characteristic.0)))?;
        *stored_value = value;
        Ok(())
    }
}

impl Inner {
    /// Record the central from `gatt_session`. Assign a driver session on the first call.
    /// Emit `Connected { Peripheral }` on every call, as permitted by `Driver::events`.
    pub(super) fn note_server_peer(self: &Arc<Self>, gatt_session: &GattSession, generation: u64) -> Option<(String, u64)> {
        let current = self.server_generation.lock().unwrap();
        if *current != generation {
            return None;
        }
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
                if peer.excluded {
                    return None;
                }
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
                    ServerPeer { session, gatt_session: gatt_session.clone(), status_token, excluded: false },
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

    pub(super) fn forget_server_peers(&self) {
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

    pub(super) fn is_excluded(&self, address: &str) -> bool {
        self.server_peers.lock().unwrap().get(address).is_some_and(|peer| peer.excluded)
    }

    pub(super) fn local_characteristic(&self, characteristic: CharacteristicUuid) -> Result<GattLocalCharacteristic> {
        self.server
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| BleError::Gatt("nothing is being served".into()))?
            .characteristics
            .get(&characteristic)
            .cloned()
            .ok_or_else(|| BleError::Gatt(format!("characteristic {} is not served", characteristic.0)))
    }

    /// The subscribed clients of `characteristic` on the live server, with
    /// their addresses.
    pub(super) fn subscribed_clients(
        &self, characteristic: CharacteristicUuid,
    ) -> Result<Vec<(String, GattSubscribedClient)>> {
        let local = self.local_characteristic(characteristic)?;
        let clients = local.SubscribedClients().map_err(|err| win_err("SubscribedClients", err))?;
        Ok(clients
            .into_iter()
            .filter_map(|client| {
                let address = client.Session().and_then(|session| session_address(&session)).ok()?;
                Some((address, client))
            })
            .collect())
    }
}

/// Select the provider advertisement's service data: the local name or the
/// caller's data under the served UUID.
/// `validate` rejects specifications that supply both.
/// Windows omits data that cannot fit with a 128-bit UUID
/// (`StartedWithoutAllAdvertisementData`). The GATT advertisement carries it instead.
pub(super) fn provider_service_data(spec: &GattServiceSpec) -> Option<Vec<u8>> {
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

pub(super) async fn start_server(inner: &Arc<Inner>, spec: &GattServiceSpec) -> Result<Server> {
    if !spec.manufacturer_data.is_empty() || spec.service_data.keys().any(|uuid| *uuid != spec.uuid) {
        log::info!(
            "advertise: Windows omits manufacturer data and data for other services; \
             scanners use the GATT advertisement (ADR-0012)"
        );
    }
    let created = GattServiceProvider::CreateAsync(to_guid(spec.uuid.0))
        .map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    let result = created.await.map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    let error = result.Error().map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    if error != BluetoothError::Success {
        return Err(server_error("GattServiceProvider::CreateAsync", error));
    }
    let provider = result.ServiceProvider().map_err(|err| win_err("ServiceProvider", err))?;
    let local_service = provider.Service().map_err(|err| win_err("GattServiceProvider::Service", err))?;

    let (requests_tx, requests_rx) = mpsc::unbounded_channel::<ServerRequest>();
    let values = Arc::new(StdMutex::new(HashMap::new()));
    let generation = inner.next_id();
    *inner.server_generation.lock().unwrap() = generation;
    let mut server = Server {
        inner: Arc::downgrade(inner), generation,
        provider: provider.clone(), characteristics: HashMap::new(), values: values.clone(),
        desired_visibility: true,
        tokens: Vec::new(), request_task: None,
    };
    for characteristic_spec in &spec.characteristics {
        let parameters =
            GattLocalCharacteristicParameters::new().map_err(|err| win_err("CharacteristicParameters", err))?;
        let mut properties = GattCharacteristicProperties::None;
        if characteristic_spec.readable {
            properties |= GattCharacteristicProperties::Read;
        }
        if characteristic_spec.writable {
            properties |= GattCharacteristicProperties::Write | GattCharacteristicProperties::WriteWithoutResponse;
        }
        if characteristic_spec.notifiable {
            properties |= GattCharacteristicProperties::Notify;
        }
        let configure_characteristic = || -> windows::core::Result<()> {
            parameters.SetCharacteristicProperties(properties)?;
            parameters.SetReadProtectionLevel(GattProtectionLevel::Plain)?;
            parameters.SetWriteProtectionLevel(GattProtectionLevel::Plain)?;
            Ok(())
        };
        configure_characteristic().map_err(|err| win_err("CharacteristicParameters", err))?;
        let op = local_service
            .CreateCharacteristicAsync(to_guid(characteristic_spec.uuid.0), &parameters)
            .map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        drop(parameters);
        let created = op.await.map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        let error = created.Error().map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        if error != BluetoothError::Success {
            return Err(server_error("CreateCharacteristicAsync", error));
        }
        let local = created.Characteristic().map_err(|err| win_err("Characteristic", err))?;
        let uuid = characteristic_spec.uuid;
        // Register each token in its owner before the next fallible acquisition.
        let token_index = server.tokens.len();
        server.tokens.push((local.clone(), None, None, None));

        // Answered from `values` rather than a `StaticValue`, so
        // `set_characteristic_value` can change it while serving.
        server.tokens[token_index].1 = if characteristic_spec.readable {
            values.lock().unwrap().insert(uuid, characteristic_spec.initial_value.clone());
            let requests_tx = requests_tx.clone();
            let handler = TypedEventHandler::new(
                move |_: Ref<GattLocalCharacteristic>, args: Ref<GattReadRequestedEventArgs>| {
                    let _ = requests_tx.send(ServerRequest::Read(uuid, args.ok()?.clone()));
                    Ok(())
                },
            );
            Some(local.ReadRequested(&handler).map_err(|err| win_err("ReadRequested", err))?)
        } else {
            None
        };
        server.tokens[token_index].2 = if characteristic_spec.writable {
            let requests_tx = requests_tx.clone();
            let handler = TypedEventHandler::new(
                move |_: Ref<GattLocalCharacteristic>, args: Ref<GattWriteRequestedEventArgs>| {
                    // Queued in arrival order and handled on one task, so
                    // the fragments of a datagram stay in order.
                    let _ = requests_tx.send(ServerRequest::Write(uuid, args.ok()?.clone()));
                    Ok(())
                },
            );
            Some(local.WriteRequested(&handler).map_err(|err| win_err("WriteRequested", err))?)
        } else {
            None
        };
        server.tokens[token_index].3 = if characteristic_spec.notifiable {
            let weak = Arc::downgrade(inner);
            let handler = TypedEventHandler::new(move |sender: Ref<GattLocalCharacteristic>, _: Ref<IInspectable>| {
                let Some(inner) = weak.upgrade() else { return Ok(()) };
                for client in sender.ok()?.SubscribedClients()? {
                    if let Ok(session) = client.Session() {
                        inner.note_server_peer(&session, generation);
                    }
                }
                Ok(())
            });
            Some(local.SubscribedClientsChanged(&handler).map_err(|err| win_err("SubscribedClientsChanged", err))?)
        } else {
            None
        };
        server.characteristics.insert(uuid, local);
    }
    drop(requests_tx);

    server.request_task = Some(tokio::spawn(drain_requests(Arc::downgrade(inner), generation, values.clone(), requests_rx)));
    start_provider_advertising(&provider, spec).await?;
    Ok(server)
}

pub(super) async fn start_provider_advertising(provider: &GattServiceProvider, spec: &GattServiceSpec) -> Result<()> {
    {
        let parameters =
            GattServiceProviderAdvertisingParameters::new().map_err(|err| win_err("AdvertisingParameters", err))?;
        parameters.SetIsConnectable(true).map_err(|err| win_err("SetIsConnectable", err))?;
        parameters.SetIsDiscoverable(true).map_err(|err| win_err("SetIsDiscoverable", err))?;
        if let Some(data) = provider_service_data(spec) {
            parameters.SetServiceData(&to_buffer(&data)?).map_err(|err| win_err("SetServiceData", err))?;
        }
        provider.StartAdvertisingWithParameters(&parameters).map_err(|err| win_err("StartAdvertising", err))?;
    }
    // On the Windows 11 test computer with an Intel adapter, `Aborted` without an error preceded `Started`.
    // Accept a started status immediately. Otherwise, wait until the deadline before reporting failure.
    // Google Nearby's Windows GATT server waits the same way.
    let confirmed = advertising_start::confirm(|| -> Result<bool> {
        let status = provider.AdvertisementStatus().map_err(|err| win_err("AdvertisementStatus", err))?;
        if status == GattServiceProviderAdvertisementStatus::Started {
            return Ok(true);
        }
        if status == GattServiceProviderAdvertisementStatus::StartedWithoutAllAdvertisementData {
            log::info!("advertise: started; service data does not fit. The GATT advertisement carries it");
            return Ok(true);
        }
        Ok(false)
    }, || {
        if let Err(err) = provider.StopAdvertising() {
            log::warn!("advertise: pending start cleanup failed: {err}");
        }
    }, ADVERTISE_START_TIMEOUT, STATUS_POLL_INTERVAL).await?;
    if !confirmed {
        return Err(BleError::Gatt(format!("advertise: not started after {ADVERTISE_START_TIMEOUT:?}")));
    }
    Ok(())
}

/// Answers inbound reads and writes in the order they arrived; writes are
/// published as `CharacteristicWritten`.
async fn drain_requests(
    inner: Weak<Inner>, generation: u64, values: Arc<StdMutex<HashMap<CharacteristicUuid, Vec<u8>>>>,
    mut requests: mpsc::UnboundedReceiver<ServerRequest>,
) {
    while let Some(request) = requests.recv().await {
        let Some(inner) = inner.upgrade() else { return };
        let (characteristic, outcome) = match &request {
            ServerRequest::Write(characteristic, args) => (*characteristic, handle_write(&inner, generation, *characteristic, args).await),
            ServerRequest::Read(characteristic, args) => (*characteristic, handle_read(&values, *characteristic, args).await),
        };
        if let Err(err) = outcome {
            log::warn!("server: request on {} failed: {err}", characteristic.0);
        }
    }
}

/// Answer reads from `values`, including long reads that use `Offset`.
/// Do not emit `Connected` for a read: a scanner can read the GATT advertisement
/// without becoming a datagram peer.
/// Android and Linux use the same rule.
async fn handle_read(
    values: &StdMutex<HashMap<CharacteristicUuid, Vec<u8>>>, characteristic: CharacteristicUuid,
    args: &GattReadRequestedEventArgs,
) -> windows::core::Result<()> {
    let deferral = args.GetDeferral()?;
    let outcome = async {
        let request = args.GetRequestAsync()?.await?;
        let offset = request.Offset()? as usize;
        let value = values.lock().unwrap().get(&characteristic).cloned().unwrap_or_default();
        if offset > value.len() {
            return request.RespondWithProtocolError(ATT_INVALID_OFFSET);
        }
        request.RespondWithValue(&win_buffer(&value[offset..])?)
    }
    .await;
    deferral.Complete()?;
    outcome
}

async fn handle_write(
    inner: &Arc<Inner>, generation: u64, characteristic: CharacteristicUuid, args: &GattWriteRequestedEventArgs,
) -> windows::core::Result<()> {
    let deferral = args.GetDeferral()?;
    let outcome = async {
        let session = args.Session()?;
        let request = args.GetRequestAsync()?.await?;
        let with_response = request.Option()? == GattWriteOption::WriteWithResponse;
        let address = session_address(&session)?;
        if inner.is_excluded(&address) {
            if with_response {
                request.RespondWithProtocolError(ATT_WRITE_NOT_PERMITTED)?;
            }
            return Ok(());
        }
        let Some((address, _)) = inner.note_server_peer(&session, generation) else {
            if with_response {
                request.RespondWithProtocolError(ATT_WRITE_NOT_PERMITTED)?;
            }
            return Ok(());
        };
        // Serialize event publication with server replacement. No native await occurs under this lock.
        let current = inner.server_generation.lock().unwrap();
        if *current != generation || inner.is_excluded(&address) {
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
        let _ = inner.events_tx.send(GattEvent::CharacteristicWritten { peer: PeerAddress(address), characteristic, value });
        Ok(())
    }
    .await;
    deferral.Complete()?;
    outcome
}

pub(super) async fn notify_client(
    local: &GattLocalCharacteristic, value: &[u8], client: &GattSubscribedClient,
) -> Result<()> {
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
