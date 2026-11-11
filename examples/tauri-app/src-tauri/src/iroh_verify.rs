//! Android properties select the same diagnostic used by the Linux example.
#[path = "../../../../ble-gatt-iroh/examples/support/either_peer.rs"]
mod diagnostic;
use std::time::Duration;
use tauri::{AppHandle, Runtime};
use tauri_plugin_ble_gatt::BleGattExt;

fn property(name: &str) -> String {
    std::process::Command::new("getprop")
        .arg(name)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

pub fn spawn_if_configured<R: Runtime>(app: AppHandle<R>) -> bool {
    if !cfg!(debug_assertions) {
        return false;
    }
    let mode = property("debug.blegatt.iroh.mode");
    if mode.is_empty() {
        return false;
    }
    tauri::async_runtime::spawn(async move {
        #[cfg(target_os = "android")]
        let service_dir = {
            use tauri::Manager;
            app.path()
                .app_data_dir()
                .map(|p| p.join("files"))
                .map_err(|e| e.to_string())
        };
        let result = async {
            #[cfg(target_os = "android")]
            {
                let dir = service_dir.as_ref().map_err(Clone::clone)?;
                let pid = std::process::id().to_string();
                tokio::time::timeout(Duration::from_secs(10), async {
                    while std::fs::read_to_string(dir.join("either-peer-foreground")).ok().as_deref() != Some(&pid) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }).await.map_err(|_| "foreground service not ready: inspect EitherPeerService logs and existing permission prerequisites")?;
            }
            let key = property("debug.blegatt.iroh.secret").parse().map_err(|_| "invalid diagnostic secret property")?;
            let peer = property("debug.blegatt.iroh.peer").parse().map_err(|_| "invalid peer public-key property")?;
            let (initiate, expect_incoming) = diagnostic::mode("phone", &mode)?;
            let start = property("debug.blegatt.iroh.start");
            let start_at = if start.is_empty() { 0 } else { start.parse().map_err(|_| "invalid start property")? };
            let adapter = app.ble_gatt().adapter().await;
            diagnostic::run(adapter, diagnostic::Options { key, peer, initiate, expect_incoming, start_at, timeout: Duration::from_secs(180) }).await
        }.await;
        #[cfg(target_os = "android")]
        if let Ok(dir) = service_dir {
            let _ = std::fs::write(dir.join("either-peer-stop"), b"done");
        }
        match result {
            Ok(()) => diagnostic::mark("PASS payload-reuse-survivor-cleanup"),
            Err(e) => diagnostic::mark(format!("FAIL {e}")),
        }
    });
    true
}
