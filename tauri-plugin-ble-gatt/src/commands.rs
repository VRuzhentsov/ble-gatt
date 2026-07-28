//! `#[tauri::command]` surface calling into `ble-gatt`. Request/response
//! only for now (`ble_scan_once` collects a snapshot over a timeout rather
//! than streaming) — continuous scan-as-events and subscribe-as-events are
//! deferred until Fini's Stage 3 integration defines the exact shape it
//! needs, per the plan's staged-delivery decision.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ble_gatt::{
    Backend, CharacteristicUuid, GattCharacteristicSpec, GattConnection, GattServiceSpec, PeerAddress,
    ServiceUuid,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use uuid::Uuid;

pub struct PluginState {
    backend: Arc<dyn Backend>,
    connections: Mutex<HashMap<u64, Box<dyn GattConnection>>>,
    next_handle: AtomicU64,
}

impl PluginState {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            connections: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
        }
    }
}

fn parse_uuid(raw: &str) -> Result<Uuid, String> {
    Uuid::parse_str(raw).map_err(|err| format!("invalid UUID '{raw}': {err}"))
}

#[derive(Serialize)]
pub struct CapabilitiesResponse {
    pub central: bool,
    pub peripheral: bool,
}

#[derive(Serialize)]
pub struct DiscoveredPeerDto {
    pub address: String,
    pub name: Option<String>,
}

#[derive(Deserialize)]
pub struct CharacteristicSpecDto {
    pub uuid: String,
    pub readable: bool,
    pub writable: bool,
    pub notifiable: bool,
    pub initial_value: Vec<u8>,
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
                Some(peer) => found.push(DiscoveredPeerDto { address: peer.address.0, name: peer.name }),
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
    state.connections.lock().await.insert(handle, connection);
    Ok(handle)
}

#[tauri::command]
pub async fn ble_read(
    state: tauri::State<'_, PluginState>, handle: u64, characteristic_uuid: String,
) -> Result<Vec<u8>, String> {
    let uuid = parse_uuid(&characteristic_uuid)?;
    let mut connections = state.connections.lock().await;
    let connection = connections.get_mut(&handle).ok_or("unknown connection handle")?;
    connection.read(CharacteristicUuid(uuid)).await.map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn ble_write(
    state: tauri::State<'_, PluginState>, handle: u64, characteristic_uuid: String, value: Vec<u8>,
) -> Result<(), String> {
    let uuid = parse_uuid(&characteristic_uuid)?;
    let mut connections = state.connections.lock().await;
    let connection = connections.get_mut(&handle).ok_or("unknown connection handle")?;
    connection
        .write(CharacteristicUuid(uuid), value)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn ble_disconnect(state: tauri::State<'_, PluginState>, handle: u64) -> Result<(), String> {
    let mut connections = state.connections.lock().await;
    if let Some(mut connection) = connections.remove(&handle) {
        connection.disconnect().await.map_err(|err| err.to_string())?;
    }
    Ok(())
}
