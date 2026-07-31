//! `#[tauri::command]` surface calling into `ble-gatt`. Request/response
//! only for now (`ble_scan_once` collects a snapshot over a timeout rather
//! than streaming) — continuous scan-as-events and subscribe-as-events are
//! deferred until Fini's Stage 3 integration defines the exact shape it
//! needs, per the plan's staged-delivery decision.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ble_gatt::{
    Backend, CharacteristicUuid, GattCharacteristicSpec, GattConnection, GattEvent, GattServiceSpec,
    PeerAddress, ServiceUuid, WriteType,
};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use uuid::Uuid;

/// One live GATT connection, individually locked.
type SharedConnection = Arc<Mutex<Box<dyn GattConnection>>>;

pub struct PluginState {
    /// Live event-forwarding tasks, so a JS caller can actually stop one.
    /// Dropping the JS handler alone does not: the `Channel` stays valid and
    /// Rust keeps sending, so repeated subscribe/dispose cycles accumulated
    /// tasks and IPC traffic forever.
    watchers: Mutex<HashMap<u64, tokio::task::JoinHandle<()>>>,
    backend: Arc<dyn Backend>,
    /// Each connection gets its own lock. A single map-wide mutex was held
    /// across `.await`, so one slow or hung GATT operation blocked every
    /// command on every other connection.
    connections: Mutex<HashMap<u64, SharedConnection>>,
    next_handle: AtomicU64,
}

impl PluginState {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            connections: Mutex::new(HashMap::new()),
            watchers: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
        }
    }
}

impl PluginState {
    /// Look up a connection and release the map lock immediately, so a slow
    /// operation on one connection cannot block commands on another.
    async fn connection(&self, handle: u64) -> Result<SharedConnection, String> {
        self.connections
            .lock()
            .await
            .get(&handle)
            .cloned()
            .ok_or_else(|| "unknown connection handle".to_string())
    }
}

fn parse_uuid(raw: &str) -> Result<Uuid, String> {
    Uuid::parse_str(raw).map_err(|err| format!("invalid UUID '{raw}': {err}"))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitiesResponse {
    pub central: bool,
    pub peripheral: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredPeerDto {
    pub address: String,
    pub name: Option<String>,
    /// Manufacturer-specific advertisement data, keyed by company ID as a
    /// decimal string — JSON object keys must be strings, and JS would
    /// silently stringify a numeric key anyway.
    pub manufacturer_data: BTreeMap<String, Vec<u8>>,
    /// Service advertisement data, keyed by service UUID string.
    pub service_data: BTreeMap<String, Vec<u8>>,
    pub rssi: Option<i16>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CharacteristicSpecDto {
    pub uuid: String,
    pub readable: bool,
    pub writable: bool,
    pub notifiable: bool,
    pub initial_value: Vec<u8>,
}

/// Connection lifecycle as delivered to JS. Mirrors `ble_gatt::GattEvent`
/// but flattened into a tagged shape that is natural to `switch` on from
/// TypeScript.
#[derive(Serialize, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum GattEventDto {
    #[serde(rename_all = "camelCase")]
    Connected { address: String, local_role: String },
    #[serde(rename_all = "camelCase")]
    Disconnected { address: String, local_role: String },
    #[serde(rename_all = "camelCase")]
    CharacteristicWritten {
        address: String,
        characteristic_uuid: String,
        value: Vec<u8>,
    },
}

/// Which role *this* device played. JS needs it for the same reason Rust
/// does: an outbound connection and an inbound central are otherwise
/// indistinguishable.
fn role_name(role: ble_gatt::Role) -> String {
    match role {
        ble_gatt::Role::Central => "central".to_string(),
        ble_gatt::Role::Peripheral => "peripheral".to_string(),
    }
}

impl From<GattEvent> for GattEventDto {
    fn from(event: GattEvent) -> Self {
        match event {
            GattEvent::Connected { peer, local_role } => Self::Connected {
                address: peer.0,
                local_role: role_name(local_role),
            },
            GattEvent::Disconnected { peer, local_role } => Self::Disconnected {
                address: peer.0,
                local_role: role_name(local_role),
            },
            GattEvent::CharacteristicWritten {
                peer,
                characteristic,
                value,
            } => Self::CharacteristicWritten {
                address: peer.0,
                characteristic_uuid: characteristic.0.to_string(),
                value,
            },
        }
    }
}

#[tauri::command]
pub async fn ble_capabilities(state: tauri::State<'_, PluginState>) -> Result<CapabilitiesResponse, String> {
    let caps = state.backend.capabilities().await;
    Ok(CapabilitiesResponse {
        central: caps.central,
        peripheral: caps.peripheral,
    })
}

#[tauri::command]
pub async fn ble_advertise(
    state: tauri::State<'_, PluginState>, service_uuid: String, characteristics: Vec<CharacteristicSpecDto>,
) -> Result<(), String> {
    let uuid = parse_uuid(&service_uuid)?;
    let specs = characteristics
        .into_iter()
        .map(|dto| {
            Ok(GattCharacteristicSpec {
                uuid: CharacteristicUuid(parse_uuid(&dto.uuid)?),
                readable: dto.readable,
                writable: dto.writable,
                notifiable: dto.notifiable,
                initial_value: dto.initial_value,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    state
        .backend
        .advertise(GattServiceSpec {
            uuid: ServiceUuid(uuid),
            characteristics: specs,
        })
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn ble_stop_advertising(state: tauri::State<'_, PluginState>) -> Result<(), String> {
    state.backend.stop_advertising().await.map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn ble_notify(
    state: tauri::State<'_, PluginState>, characteristic_uuid: String, value: Vec<u8>,
) -> Result<(), String> {
    let uuid = parse_uuid(&characteristic_uuid)?;
    state
        .backend
        .notify(CharacteristicUuid(uuid), value)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn ble_scan_once(
    state: tauri::State<'_, PluginState>, service_uuid: String, timeout_ms: u64,
) -> Result<Vec<DiscoveredPeerDto>, String> {
    let uuid = parse_uuid(&service_uuid)?;
    let mut stream = state
        .backend
        .scan(ServiceUuid(uuid))
        .await
        .map_err(|err| err.to_string())?;

    let mut found = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            item = stream.next() => match item {
                // A scan failure arriving mid-stream is returned as an error
                // even though some peers may already have been collected:
                // reporting a truncated list as success would tell the user
                // "these are the devices nearby" when the scan was actually
                // cut short by a powered-off adapter or a denied permission.
                Some(Err(err)) => return Err(err.to_string()),
                Some(Ok(peer)) => found.push(DiscoveredPeerDto {
                    address: peer.address.0,
                    name: peer.name,
                    manufacturer_data: peer
                        .manufacturer_data
                        .into_iter()
                        .map(|(id, value)| (id.to_string(), value))
                        .collect(),
                    service_data: peer
                        .service_data
                        .into_iter()
                        .map(|(uuid, value)| (uuid.0.to_string(), value))
                        .collect(),
                    rssi: peer.rssi,
                }),
                None => break,
            }
        }
    }
    Ok(found)
}

#[tauri::command]
pub async fn ble_connect(state: tauri::State<'_, PluginState>, address: String) -> Result<u64, String> {
    let connection = state
        .backend
        .connect(&PeerAddress(address))
        .await
        .map_err(|err| err.to_string())?;
    let handle = state.next_handle.fetch_add(1, Ordering::SeqCst);
    state
        .connections
        .lock()
        .await
        .insert(handle, Arc::new(Mutex::new(connection)));
    Ok(handle)
}

#[tauri::command]
pub async fn ble_read(
    state: tauri::State<'_, PluginState>, handle: u64, characteristic_uuid: String,
) -> Result<Vec<u8>, String> {
    let uuid = parse_uuid(&characteristic_uuid)?;
    let connection = state.connection(handle).await?;
    let mut connection = connection.lock().await;
    connection.read(CharacteristicUuid(uuid)).await.map_err(|err| err.to_string())
}

/// `without_response` opts into ATT Write Command: much faster for bulk
/// transfer, but the peer silently drops what it can't keep up with. Absent
/// or `false` means the acknowledged write, which is the safe default.
#[tauri::command]
pub async fn ble_write(
    state: tauri::State<'_, PluginState>, handle: u64, characteristic_uuid: String, value: Vec<u8>,
    without_response: Option<bool>,
) -> Result<(), String> {
    let uuid = parse_uuid(&characteristic_uuid)?;
    let write_type = if without_response.unwrap_or(false) {
        WriteType::WithoutResponse
    } else {
        WriteType::WithResponse
    };
    let connection = state.connection(handle).await?;
    let mut connection = connection.lock().await;
    connection
        .write_with_type(CharacteristicUuid(uuid), value, write_type)
        .await
        .map_err(|err| err.to_string())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionMtuResponse {
    /// Negotiated ATT MTU for this connection.
    pub att_mtu: u16,
    /// Largest payload that fits in one write. Chunk bulk transfers against
    /// this rather than a hardcoded constant — it is only known after
    /// negotiation and differs per peer and platform.
    pub max_write_len: usize,
}

#[tauri::command]
pub async fn ble_connection_mtu(
    state: tauri::State<'_, PluginState>, handle: u64,
) -> Result<ConnectionMtuResponse, String> {
    let connection = state.connection(handle).await?;
    let connection = connection.lock().await;
    Ok(ConnectionMtuResponse {
        att_mtu: connection.att_mtu(),
        max_write_len: connection.max_write_len(),
    })
}

/// Stream connection lifecycle events to the frontend over a `Channel`
/// supplied by the caller.
///
/// A per-subscriber `Channel` rather than a global emitted event name: it
/// scopes delivery to the caller that asked, needs no agreed-upon event
/// string, and stops cleanly when the JS side drops it.
///
/// This is the only way the frontend learns about a peer disappearing
/// without warning — every other command reports failures of operations you
/// initiated, so a UI mid-transfer would otherwise just appear to stall.
/// Returns a subscription id; pass it to `ble_unwatch_events` to stop.
#[tauri::command]
pub async fn ble_watch_events(
    state: tauri::State<'_, PluginState>, on_event: Channel<GattEventDto>,
) -> Result<u64, String> {
    let mut events = state.backend.events();
    let id = state.next_handle.fetch_add(1, Ordering::SeqCst);
    let task = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            // Send failure means the JS side dropped the channel; stop
            // rather than spin forwarding into nothing.
            if on_event.send(GattEventDto::from(event)).is_err() {
                return;
            }
        }
    });
    state.watchers.lock().await.insert(id, task);
    Ok(id)
}

#[tauri::command]
pub async fn ble_unwatch_events(
    state: tauri::State<'_, PluginState>, subscription: u64,
) -> Result<(), String> {
    if let Some(task) = state.watchers.lock().await.remove(&subscription) {
        task.abort();
    }
    Ok(())
}

#[tauri::command]
pub async fn ble_disconnect(state: tauri::State<'_, PluginState>, handle: u64) -> Result<(), String> {
    let entry = state.connections.lock().await.remove(&handle);
    if let Some(connection) = entry {
        connection
            .lock()
            .await
            .disconnect()
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}
