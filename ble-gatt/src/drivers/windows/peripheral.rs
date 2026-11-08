//! Peripheral role: `GattServiceProvider` serves and sends the connectable
//! advertisement; a `BluetoothLEAdvertisementPublisher` sends the
//! manufacturer data the provider cannot (ADR-0011 D4).
//!
//! btleplug has no peripheral role, so this part is this library's own,
//! shaped like the GATT servers of the Linux and Android drivers. The
//! provider-plus-publisher split is the one tinygo-org/bluetooth
//! (`gatts_windows.go`, `gap_windows.go`) uses. Like tinygo's, the
//! publisher carries no service data and no name: Windows refuses to start
//! a publisher with either.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::mpsc;
use windows::core::{IInspectable, Ref};
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementPublisher, BluetoothLEAdvertisementPublisherStatus, BluetoothLEManufacturerData,
};
use windows::Devices::Bluetooth::BluetoothError;
use windows::Devices::Bluetooth::GenericAttributeProfile::{
    GattCharacteristicProperties, GattCommunicationStatus, GattLocalCharacteristic,
    GattLocalCharacteristicParameters, GattProtectionLevel, GattServiceProvider,
    GattServiceProviderAdvertisementStatus, GattServiceProviderAdvertisingParameters, GattSession,
    GattSessionStatus, GattSessionStatusChangedEventArgs, GattSubscribedClient, GattWriteOption,
    GattWriteRequestedEventArgs,
};
use windows::Foundation::TypedEventHandler;

use super::utils::{communication_error, from_buffer, session_address, to_buffer, to_guid, win_err};
use super::{Inner, ADVERTISE_START_TIMEOUT, STATUS_POLL_INTERVAL};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    truncate_name, CharacteristicUuid, GattEvent, GattServiceSpec, PeerAddress, Role, NAME_SERVICE_DATA_MAX_LEN,
};

/// ATT error "Write Not Permitted" (Bluetooth Core, Vol 3, Part F, 3.4.1.1):
/// the reply to a write from a central that `disconnect_peer` excluded.
const ATT_WRITE_NOT_PERMITTED: u8 = 0x03;

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

/// The live GATT server and its advertisements.
pub(super) struct Server {
    pub(super) provider: GattServiceProvider,
    characteristics: HashMap<CharacteristicUuid, GattLocalCharacteristic>,
    /// `(characteristic, WriteRequested token, SubscribedClientsChanged token)`.
    tokens: Vec<(GattLocalCharacteristic, Option<i64>, Option<i64>)>,
    /// Carries the manufacturer data. `None` when the spec has none.
    pub(super) publisher: Option<BluetoothLEAdvertisementPublisher>,
    /// Drains `WriteRequested` in arrival order.
    write_task: tokio::task::JoinHandle<()>,
}

impl Server {
    pub(super) fn shut_down(self) {
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

impl Inner {
    /// Record a central seen through `gatt_session`, minting a session for
    /// it the first time. Emits `Connected { Peripheral }` on every call:
    /// `Driver::events` documents that it is idempotent.
    pub(super) fn note_server_peer(self: &Arc<Self>, gatt_session: &GattSession) -> Option<(String, u64)> {
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

/// The service data the provider's own advertisement carries under the
/// served UUID: the name, sent as service data like the Android driver
/// does, or the caller's service data under that UUID (`validate` forbids
/// both).
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
    let created = GattServiceProvider::CreateAsync(to_guid(spec.uuid.0))
        .map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
    let result = created.await.map_err(|err| win_err("GattServiceProvider::CreateAsync", err))?;
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
        let parameters =
            GattLocalCharacteristicParameters::new().map_err(|err| win_err("CharacteristicParameters", err))?;
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
        let op = local_service
            .CreateCharacteristicAsync(to_guid(spec_char.uuid.0), &parameters)
            .map_err(|err| win_err("CreateCharacteristicAsync", err))?;
        drop(parameters);
        let created = op.await.map_err(|err| win_err("CreateCharacteristicAsync", err))?;
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
    // `Aborted` is not final here. On the test computer (Intel adapter,
    // Windows 11) the provider reports `Aborted` with no error first and
    // `Started` a moment later, so only the status at the deadline counts.
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
        if tokio::time::Instant::now() >= deadline {
            let _ = provider.StopAdvertising();
            let reason = if status == GattServiceProviderAdvertisementStatus::Aborted {
                "Windows aborted the advertisement".to_string()
            } else {
                format!("not started after {ADVERTISE_START_TIMEOUT:?} (status {})", status.0)
            };
            return Err(BleError::Gatt(format!("advertise: {reason}")));
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

/// Build a publisher with the manufacturer data and, when `with_uuid`, the
/// service UUID too, so scanners that filter on it (every driver of this
/// library does) receive it.
fn build_publisher(spec: &GattServiceSpec, with_uuid: bool) -> Result<BluetoothLEAdvertisementPublisher> {
    let publisher = BluetoothLEAdvertisementPublisher::new().map_err(|err| win_err("AdvertisementPublisher", err))?;
    let advertisement = publisher.Advertisement().map_err(|err| win_err("Publisher::Advertisement", err))?;
    if with_uuid {
        advertisement
            .ServiceUuids()
            .and_then(|uuids| uuids.Append(to_guid(spec.uuid.0)))
            .map_err(|err| win_err("Publisher::ServiceUuids", err))?;
    }
    for (company, data) in &spec.manufacturer_data {
        let entry = BluetoothLEManufacturerData::Create(*company, &to_buffer(data)?)
            .map_err(|err| win_err("ManufacturerData", err))?;
        advertisement
            .ManufacturerData()
            .and_then(|list| list.Append(&entry))
            .map_err(|err| win_err("Publisher::ManufacturerData", err))?;
    }
    Ok(publisher)
}

/// Wait until the publisher started (`Ok(true)`), was aborted (`Ok(false)`)
/// or is still waiting for a free advertising slot at the deadline
/// (`Ok(true)`: Windows starts it later by itself).
async fn publisher_started(publisher: &BluetoothLEAdvertisementPublisher) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + ADVERTISE_START_TIMEOUT;
    loop {
        let status = publisher.Status().map_err(|err| win_err("Publisher::Status", err))?;
        if status == BluetoothLEAdvertisementPublisherStatus::Started {
            return Ok(true);
        }
        // Like the provider's, an early `Aborted` may still turn into
        // `Started`; only the status at the deadline counts.
        if tokio::time::Instant::now() >= deadline {
            if status == BluetoothLEAdvertisementPublisherStatus::Aborted {
                return Ok(false);
            }
            log::warn!("advertise: manufacturer data advertisement still waiting (status {})", status.0);
            return Ok(true);
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

/// The non-connectable advertisement with the manufacturer data (ADR-0011
/// D4). `None` when the spec has none.
///
/// Service data under UUIDs other than the served one cannot be advertised
/// on Windows: the provider sends one entry, and the publisher refuses
/// service data (tinygo-org/bluetooth found the same). It is logged and
/// left out.
pub(super) async fn start_publisher(spec: &GattServiceSpec) -> Result<Option<BluetoothLEAdvertisementPublisher>> {
    let other_service_data = spec.service_data.keys().filter(|uuid| **uuid != spec.uuid).count();
    if other_service_data > 0 {
        log::warn!("advertise: {other_service_data} service data entries under other UUIDs are not sent on Windows");
    }
    if spec.manufacturer_data.is_empty() {
        return Ok(None);
    }
    // With the service UUID first; without it if Windows refuses that.
    for with_uuid in [true, false] {
        let publisher = build_publisher(spec, with_uuid)?;
        if let Err(err) = publisher.Start() {
            log::warn!("advertise: publisher refused (service UUID included: {with_uuid}): {err}");
            continue;
        }
        if publisher_started(&publisher).await? {
            if !with_uuid {
                log::warn!("advertise: manufacturer data sent without the service UUID; filtering scanners miss it");
            }
            return Ok(Some(publisher));
        }
        log::warn!("advertise: publisher aborted (service UUID included: {with_uuid})");
        let _ = publisher.Stop();
    }
    Err(BleError::Gatt("advertise: Windows refused the manufacturer data advertisement".into()))
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

async fn handle_write(
    inner: &Arc<Inner>, characteristic: CharacteristicUuid, args: &GattWriteRequestedEventArgs,
) -> windows::core::Result<()> {
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
