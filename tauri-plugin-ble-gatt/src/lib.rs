//! Tauri integration for the `ble-gatt` crate. All GATT logic lives in
//! `ble-gatt`; this crate adds what a Tauri application needs around it:
//!
//! - the Android Kotlin bridge, shipped with the plugin (`android/`), so an
//!   application never keeps a copy of it;
//! - a driver for the current platform, built on first use (`lazy`), or
//!   one the application injects ([`Builder::driver`]);
//! - Bluetooth runtime permissions through Tauri's own mobile-plugin
//!   permission mechanism ([`BleGatt::request_permissions`]);
//! - access from Rust: `app.ble_gatt().adapter()` ([`BleGattExt`]), the same
//!   pattern as `tauri-plugin-store`'s `StoreExt`;
//! - `#[tauri::command]`s for JavaScript callers (`commands`).

pub mod commands;

#[cfg(target_os = "android")]
pub mod android_context;
mod lazy;

use std::sync::Arc;

pub use ble_gatt;
use ble_gatt::{Adapter, Driver};
pub use lazy::LazyDriver;
use tokio::sync::OnceCell;
use tauri::plugin::{PermissionState, PluginApi, TauriPlugin};
use tauri::{AppHandle, Manager, Runtime};

use commands::PluginState;

#[cfg(target_os = "android")]
const PLUGIN_IDENTIFIER: &str = "dev.blegatt";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No driver exists for this platform and none was injected.
    #[error("ble-gatt: no driver for this platform; inject one with Builder::driver")]
    NoDriver,
    #[cfg(target_os = "android")]
    #[error(transparent)]
    PluginInvoke(#[from] tauri::plugin::mobile::PluginInvokeError),
}

/// How long quitting the app waits for Bluetooth to be released. A BlueZ
/// disconnect took about 2s on hardware.
#[cfg(target_os = "linux")]
const EXIT_CLOSE_LIMIT: std::time::Duration = std::time::Duration::from_secs(3);

/// Registers the plugin with the platform's default driver.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new().build()
}

/// Plugin builder, for applications that supply their own driver (a mock
/// radio in tests, for instance).
#[derive(Default)]
pub struct Builder {
    driver: Option<Arc<dyn Driver>>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Use this driver instead of the platform's default one.
    pub fn driver(mut self, driver: Arc<dyn Driver>) -> Self {
        self.driver = Some(driver);
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
                let driver = match self.driver {
                    Some(driver) => driver,
                    None => default_driver().ok_or(Error::NoDriver)?,
                };
                app.manage(BleGatt::new(app, api, driver.clone())?);
                app.manage(PluginState::new(driver));
                Ok(())
            })
            // BlueZ keeps a process's links after it exits; release them on
            // the way out (ADR-0009 D1). Bounded, so a radio that does not
            // answer cannot hold up quitting. Linux only: on Android, Tauri
            // sends Exit when the activity is destroyed while the process
            // keeps running (seen on hardware when the system relaunched it),
            // and Android releases a dead app's links itself.
            .on_event(|_app, _event| {
                #[cfg(target_os = "linux")]
                if let tauri::RunEvent::Exit = _event {
                    let driver = _app.state::<BleGatt<R>>().driver.clone();
                    // The timeout is built inside the runtime: its timer
                    // needs a reactor when it is created.
                    let closed = tauri::async_runtime::block_on(async move {
                        tokio::time::timeout(EXIT_CLOSE_LIMIT, driver.close()).await
                    });
                    match closed {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => log::warn!("closing Bluetooth on exit failed: {err}"),
                        Err(_) => log::warn!("closing Bluetooth on exit timed out after {EXIT_CLOSE_LIMIT:?}"),
                    }
                }
            })
            .build()
    }
}

/// The plugin's Rust API, reached with `app.ble_gatt()`.
pub struct BleGatt<R: Runtime> {
    driver: Arc<dyn Driver>,
    /// Built on the first `adapter()` call: `Adapter::new` is async and
    /// spawns its status watcher, which plugin setup is no place to do.
    adapter: OnceCell<Adapter>,
    #[cfg(target_os = "android")]
    mobile: tauri::plugin::PluginHandle<R>,
    #[cfg(not(target_os = "android"))]
    _runtime: std::marker::PhantomData<fn() -> R>,
}

impl<R: Runtime> BleGatt<R> {
    fn new<C: serde::de::DeserializeOwned>(
        _app: &AppHandle<R>, _api: PluginApi<R, C>, driver: Arc<dyn Driver>,
    ) -> Result<Self, Error> {
        Ok(Self {
            driver,
            adapter: OnceCell::new(),
            #[cfg(target_os = "android")]
            mobile: _api.register_android_plugin(PLUGIN_IDENTIFIER, "BleGattPlugin")?,
            #[cfg(not(target_os = "android"))]
            _runtime: std::marker::PhantomData,
        })
    }

    /// The adapter this plugin drives, for using the `ble-gatt` API
    /// directly from Rust. Every call returns the same adapter.
    pub async fn adapter(&self) -> Adapter {
        self.adapter
            .get_or_init(|| Adapter::new(self.driver.clone()))
            .await
            .clone()
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

/// Linux and Android are the implemented drivers (see the README's
/// platform matrix). Elsewhere the application must inject one.
#[cfg(target_os = "linux")]
fn default_driver() -> Option<Arc<dyn Driver>> {
    Some(Arc::new(LazyDriver::new(|| async {
        let driver = ble_gatt::drivers::linux::LinuxDriver::new().await?;
        Ok(Arc::new(driver) as Arc<dyn Driver>)
    })))
}

#[cfg(target_os = "android")]
fn default_driver() -> Option<Arc<dyn Driver>> {
    Some(Arc::new(LazyDriver::new(|| async {
        android_context::ensure().map_err(ble_gatt::BleError::AdapterUnavailable)?;
        let driver = ble_gatt::drivers::android::AndroidDriver::new().await?;
        Ok(Arc::new(driver) as Arc<dyn Driver>)
    })))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn default_driver() -> Option<Arc<dyn Driver>> {
    None
}
