//! Thin Tauri plugin wrapper around the `ble-gatt` crate. All GATT logic
//! lives in `ble-gatt`; this crate only adds `tauri::plugin::Builder`
//! registration and the `#[tauri::command]` IPC surface (`commands`).

pub mod commands;

#[cfg(target_os = "android")]
mod android_lazy;

use std::sync::Arc;

use ble_gatt::Backend;
use tauri::plugin::{Builder, TauriPlugin};
use tauri::{Manager, Runtime};

use commands::PluginState;

pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("ble-gatt")
        .invoke_handler(tauri::generate_handler![
            commands::ble_capabilities,
            commands::ble_advertise,
            commands::ble_stop_advertising,
            commands::ble_notify,
            commands::ble_scan_once,
            commands::ble_connect,
            commands::ble_read,
            commands::ble_write,
            commands::ble_disconnect,
            commands::ble_connection_mtu,
            commands::ble_watch_events,
        ])
        .setup(|app, _api| {
            let backend: Arc<dyn Backend> = build_backend()?;
            app.manage(PluginState::new(backend));
            Ok(())
        })
        .build()
}

/// M1: Linux and Android are the only implemented backends so far (see the
/// workspace README's platform-support matrix). Every other target fails
/// closed with an explicit error at plugin setup rather than silently
/// no-opping — matches this project's convention of honest
/// unimplemented-feature reporting over quiet failure.
#[cfg(target_os = "linux")]
fn build_backend() -> Result<Arc<dyn Backend>, Box<dyn std::error::Error>> {
    let backend = tauri::async_runtime::block_on(ble_gatt::backend::linux::LinuxBackend::new())?;
    Ok(Arc::new(backend))
}

/// Android, unlike Linux, cannot construct the real backend eagerly here:
/// `.setup()` runs synchronously from inside `tao`'s own Android context
/// bring-up (before the JNI/Activity handles this plugin needs are fully
/// populated — confirmed by an actual crash: `ndk_context::android_context()`
/// panicking at exactly this call site). `android_lazy::LazyAndroidBackend`
/// defers the real `AndroidBackend::new()` (and the one-time
/// `tao` → `ndk-context` bridge it needs) to the first command actually
/// invoked from JS, by which point the WebView/Activity is definitely alive.
#[cfg(target_os = "android")]
fn build_backend() -> Result<Arc<dyn Backend>, Box<dyn std::error::Error>> {
    Ok(Arc::new(android_lazy::LazyAndroidBackend::new()))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn build_backend() -> Result<Arc<dyn Backend>, Box<dyn std::error::Error>> {
    Err("ble-gatt: no backend implemented yet for this platform \
         (Linux and Android are the only implemented targets so far — see the ble-gatt README)"
        .into())
}
