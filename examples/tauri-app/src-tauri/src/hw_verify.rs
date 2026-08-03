//! Hardware verification harness — the Android half of a two-device BLE
//! round trip.
//!
//! Mirrors `ble-gatt/examples/hw_peer.rs`, which is the Linux half, and uses
//! the same UUIDs, the same multi-fragment probe payload and the same
//! `HWVERIFY:` log markers, so one driver script can assert on either.
//!
//! Driven by a system property rather than a UI, because the whole point is
//! a machine-checkable result:
//!
//! ```text
//! adb shell setprop debug.blegatt.role peripheral
//! adb shell am start -n dev.blegatt.example/.MainActivity
//! adb logcat -s RustStdoutStderr | grep HWVERIFY
//! ```
//!
//! `debug.*` properties are settable from an adb shell without root, which
//! intent extras through Tauri's activity are not — that would need JNI into
//! `getIntent()`, and this needs no such machinery.
//!
//! With the property unset the app behaves exactly as before, so this costs
//! the normal example nothing.

use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::Backend;
use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::{CharacteristicUuid, PeerAddress, ServiceUuid};
use tokio_stream::StreamExt;
use uuid::Uuid;

/// Must match `ble-gatt/examples/hw_peer.rs` — the two halves are separate
/// processes on separate devices with no channel to negotiate over.
const SERVICE: Uuid = Uuid::from_u128(0x0000_b1e6_0000_1000_8000_0080_5f9b_34fb);
const CHARACTERISTIC: Uuid = Uuid::from_u128(0x0000_b1e7_0000_1000_8000_0080_5f9b_34fb);

const SCAN_TIMEOUT: Duration = Duration::from_secs(45);
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(45);

/// The role property. Read once at startup; absent means "just be the
/// example app".
const ROLE_PROP: &str = "debug.blegatt.role";

fn config() -> DatagramConfig {
    DatagramConfig::new(ServiceUuid(SERVICE), CharacteristicUuid(CHARACTERISTIC))
}

/// Larger than one fragment at any plausible MTU, so a pass proves
/// fragmentation and reassembly over the air rather than a single write.
fn probe_payload() -> Vec<u8> {
    (0..512u16).map(|i| (i % 251) as u8).collect()
}

/// Markers go through `log`, which `tauri_plugin_log` puts on logcat.
fn mark(line: &str) {
    log::info!("HWVERIFY: {line}");
}

/// Read an Android system property via `getprop`.
///
/// Shelling out rather than binding `__system_property_get`: this runs once
/// at startup, and a subprocess is far less to get wrong than an FFI call
/// into libc for a value that is allowed to be missing.
fn role() -> Option<String> {
    let out = std::process::Command::new("getprop").arg(ROLE_PROP).output().ok()?;
    let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Start the harness if a role is set. Returns immediately otherwise.
pub fn spawn_if_configured(backend: Arc<dyn Backend>) {
    let Some(role) = role() else {
        return;
    };
    tauri::async_runtime::spawn(async move {
        let result = match role.as_str() {
            "peripheral" => run_peripheral(backend).await,
            "central" => run_central(backend).await,
            other => Err(format!("unknown role {other:?}")),
        };
        match result {
            Ok(()) => mark("PASS round-trip complete"),
            Err(err) => mark(&format!("FAIL {err}")),
        }
    });
}

async fn run_peripheral(backend: Arc<dyn Backend>) -> Result<(), String> {
    let caps = backend.capabilities().await;
    mark(&format!(
        "INFO capabilities central={} peripheral={}",
        caps.central, caps.peripheral
    ));
    if !caps.peripheral {
        return Err("this device reports no peripheral support".to_string());
    }

    let config = config();
    let mut incoming = datagram::serve(backend, &config)
        .await
        .map_err(|err| format!("serve failed: {err}"))?;
    mark(&format!("INFO advertising service {SERVICE}"));
    mark("READY peripheral");

    let mut channel = tokio::time::timeout(SCAN_TIMEOUT, incoming.next())
        .await
        .map_err(|_| "no central connected within the timeout".to_string())?
        .ok_or_else(|| "serve stream closed before a central arrived".to_string())?;
    mark(&format!("INFO central connected: {}", channel.peer().0));

    let received = tokio::time::timeout(EXCHANGE_TIMEOUT, channel.recv())
        .await
        .map_err(|_| "no message received within the timeout".to_string())?
        .ok_or_else(|| "channel closed before a message arrived".to_string())?
        .map_err(|err| format!("inbound error: {err}"))?;
    mark(&format!("INFO received {} bytes", received.len()));

    // Echoing exercises the peripheral notify path, which is where most of
    // this library's review findings landed and which no mock can confirm.
    channel
        .send(received)
        .await
        .map_err(|err| format!("echo failed: {err}"))?;
    mark("INFO echoed");
    Ok(())
}

async fn run_central(backend: Arc<dyn Backend>) -> Result<(), String> {
    let caps = backend.capabilities().await;
    mark(&format!(
        "INFO capabilities central={} peripheral={}",
        caps.central, caps.peripheral
    ));

    mark(&format!("INFO scanning for {SERVICE}"));
    let mut found = backend
        .scan(ServiceUuid(SERVICE))
        .await
        .map_err(|err| format!("scan failed to start: {err}"))?;
    let discovered = tokio::time::timeout(SCAN_TIMEOUT, found.next())
        .await
        .map_err(|_| "no peer advertising the service within the timeout".to_string())?
        .ok_or_else(|| "scan stream ended without a result".to_string())?
        .map_err(|err| format!("scan failed: {err}"))?;
    mark(&format!(
        "INFO discovered {} rssi={:?} services={}",
        discovered.address.0,
        discovered.rssi,
        discovered.services.len()
    ));
    let peer = PeerAddress(discovered.address.0.clone());
    // The scan stream must be dropped before connecting: Android will not
    // reliably connect while a scan is running.
    drop(found);

    let config = config();
    let mut channel = datagram::connect(backend, &peer, &config)
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    mark(&format!(
        "INFO connected to {} max_message_len={}",
        peer.0,
        channel.max_message_len()
    ));
    mark("READY central");

    let payload = probe_payload();
    channel
        .send(payload.clone())
        .await
        .map_err(|err| format!("send failed: {err}"))?;
    mark(&format!("INFO sent {} bytes", payload.len()));

    let echoed = tokio::time::timeout(EXCHANGE_TIMEOUT, channel.recv())
        .await
        .map_err(|_| "no echo received within the timeout".to_string())?
        .ok_or_else(|| "channel closed before the echo arrived".to_string())?
        .map_err(|err| format!("inbound error: {err}"))?;

    if echoed != payload {
        return Err(format!(
            "echo mismatch: sent {} bytes, got {} bytes",
            payload.len(),
            echoed.len()
        ));
    }
    mark("INFO echo matched byte-for-byte");
    Ok(())
}
