//! Tauri integration for the `ble-gatt` crate. All GATT logic lives in
//! `ble-gatt`; this crate adds what a Tauri application needs around it:
//!
//! - the Android Kotlin bridge, shipped with the plugin (`android/`), so an
//!   application never keeps a copy of it;
//! - a backend for the current platform, built on first use (`lazy`), or
//!   one the application injects ([`Builder::backend`]);
//! - Bluetooth runtime permissions through Tauri's own mobile-plugin
//!   permission mechanism ([`BleGatt::request_permissions`]);
//! - access from Rust: `app.ble_gatt().backend()` ([`BleGattExt`]), the same
//!   pattern as `tauri-plugin-store`'s `StoreExt`;
//! - `#[tauri::command]`s for JavaScript callers (`commands`).

pub mod commands;

#[cfg(target_os = "android")]
pub mod android_context;
mod lazy;

use std::sync::Arc;

pub use ble_gatt;
use ble_gatt::Backend;
pub use lazy::LazyBackend;
use tauri::plugin::{PermissionState, PluginApi, TauriPlugin};
use tauri::{AppHandle, Manager, Runtime};

use commands::PluginState;

#[cfg(target_os = "android")]
const PLUGIN_IDENTIFIER: &str = "dev.blegatt";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No backend exists for this platform and none was injected.
    #[error("ble-gatt: no backend for this platform; inject one with Builder::backend")]
    NoBackend,
    #[cfg(target_os = "android")]
    #[error(transparent)]
    PluginInvoke(#[from] tauri::plugin::mobile::PluginInvokeError),
}

/// Registers the plugin with the platform's default backend.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new().build()
}

/// Plugin builder, for applications that supply their own backend (a mock
/// radio in tests, for instance).
#[derive(Default)]
pub struct Builder {
    backend: Option<Arc<dyn Backend>>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Use this backend instead of the platform's default one.
    pub fn backend(mut self, backend: Arc<dyn Backend>) -> Self {
        self.backend = Some(backend);
        self
    }

    pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
        tauri::plugin::Builder::new("ble-gatt")
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
                commands::ble_unwatch_events,
            ])
            .setup(move |app, api| {
                let backend = match self.backend {
                    Some(backend) => backend,
                    None => default_backend().ok_or(Error::NoBackend)?,
                };
                app.manage(BleGatt::new(app, api, backend.clone())?);
                app.manage(PluginState::new(backend));
                Ok(())
            })
            .build()
    }
}

/// The plugin's Rust API, reached with `app.ble_gatt()`.
pub struct BleGatt<R: Runtime> {
    backend: Arc<dyn Backend>,
    #[cfg(target_os = "android")]
    mobile: tauri::plugin::PluginHandle<R>,
    #[cfg(not(target_os = "android"))]
    _runtime: std::marker::PhantomData<fn() -> R>,
}

impl<R: Runtime> BleGatt<R> {
    fn new<C: serde::de::DeserializeOwned>(
        _app: &AppHandle<R>, _api: PluginApi<R, C>, backend: Arc<dyn Backend>,
    ) -> Result<Self, Error> {
        Ok(Self {
            backend,
            #[cfg(target_os = "android")]
            mobile: _api.register_android_plugin(PLUGIN_IDENTIFIER, "BleGattPlugin")?,
            #[cfg(not(target_os = "android"))]
            _runtime: std::marker::PhantomData,
        })
    }

    /// The backend this plugin drives, for using the `ble-gatt` API
    /// directly from Rust.
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    /// Whether the app may use Bluetooth. On Android this covers the
    /// runtime permissions BLE needs on this API level (`BLUETOOTH_SCAN`,
    /// `BLUETOOTH_CONNECT` and `BLUETOOTH_ADVERTISE` on 31+, fine location
    /// before); other platforms have no runtime permission and report
    /// `Granted`.
    pub fn permission_state(&self) -> Result<PermissionState, Error> {
        #[cfg(target_os = "android")]
        {
            Ok(self
                .mobile
                .run_mobile_plugin::<PermissionResponse>("checkPermissions", ())?
                .bluetooth)
        }
        #[cfg(not(target_os = "android"))]
        {
            Ok(PermissionState::Granted)
        }
    }

    /// Shows the system permission dialog if needed and returns the outcome.
    /// On Android this blocks until the user answers, so call it from a
    /// blocking task (`tauri::async_runtime::spawn_blocking`), never from
    /// the main thread.
    pub fn request_permissions(&self) -> Result<PermissionState, Error> {
        #[cfg(target_os = "android")]
        {
            Ok(self
                .mobile
                .run_mobile_plugin::<PermissionResponse>("requestPermissions", ())?
                .bluetooth)
        }
        #[cfg(not(target_os = "android"))]
        {
            Ok(PermissionState::Granted)
        }
    }
}

#[cfg(target_os = "android")]
#[derive(serde::Deserialize)]
struct PermissionResponse {
    bluetooth: PermissionState,
}

/// `app.ble_gatt()` on anything that can reach the app's state.
pub trait BleGattExt<R: Runtime> {
    fn ble_gatt(&self) -> &BleGatt<R>;
}

impl<R: Runtime, T: Manager<R>> BleGattExt<R> for T {
    fn ble_gatt(&self) -> &BleGatt<R> {
        self.state::<BleGatt<R>>().inner()
    }
}

/// Linux and Android are the implemented backends (see the README's
/// platform matrix). Elsewhere the application must inject one.
#[cfg(target_os = "linux")]
fn default_backend() -> Option<Arc<dyn Backend>> {
    Some(Arc::new(LazyBackend::new(|| async {
        let backend = ble_gatt::backend::linux::LinuxBackend::new().await?;
        Ok(Arc::new(backend) as Arc<dyn Backend>)
    })))
}

#[cfg(target_os = "android")]
fn default_backend() -> Option<Arc<dyn Backend>> {
    Some(Arc::new(LazyBackend::new(|| async {
        android_context::ensure().map_err(ble_gatt::BleError::AdapterUnavailable)?;
        let backend = ble_gatt::backend::android::AndroidBackend::new().await?;
        Ok(Arc::new(backend) as Arc<dyn Backend>)
    })))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn default_backend() -> Option<Arc<dyn Backend>> {
    None
}
