//! Tauri-specific Android glue. Two things `ble-gatt` itself deliberately
//! does not (and should not) know about, kept here in the Tauri wrapper:
//!
//! 1. **Bridging `tao`'s Android context into `ndk-context`.** `ble-gatt`'s
//!    `AndroidBackend::new()` reads `ndk_context::android_context()` — the
//!    standard, ecosystem-wide interop point most non-Tauri Android Rust
//!    runtimes (`android-activity`, `winit`, `cargo-apk`) populate
//!    automatically. Tauri's own Android runtime (`tao`) does **not**; it
//!    keeps its own separate context in
//!    `tao::platform_impl::android::ndk_glue`. Bridging it once, here,
//!    keeps `ble-gatt`'s public API honestly cross-runtime instead of
//!    quietly special-casing Tauri inside the reusable core crate.
//! 2. **Deferring construction past `.setup()`.** `.setup()` runs
//!    synchronously from inside `tao`'s own context bring-up — confirmed by
//!    an actual crash log (`ndk_context::android_context()` panicking with
//!    "android context was not initialized" at exactly that call site).
//!    `LazyAndroidBackend` defers the real `AndroidBackend::new()` to the
//!    first command actually invoked from JS, by which point the
//!    WebView/Activity is unquestionably alive.

use async_trait::async_trait;
use ble_gatt::backend::android::AndroidBackend;
use ble_gatt::{
    Backend, BleError, BoxStream, CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattConnection,
    GattEvent, GattServiceSpec, PeerAddress, Result, ServiceUuid,
};
use tokio::sync::OnceCell;

/// Populates the `ndk-context` crate's global `AndroidContext` from `tao`'s
/// own, if it hasn't been already. Safe to call more than once — only the
/// first call (which must see `tao`'s context as already available, true by
/// the time any command reaches here) actually initializes anything.
fn bridge_ndk_context_from_tao() -> Result<()> {
    use tao::platform::android::prelude::main_android_context;

    let Some(ctx) = main_android_context() else {
        return Err(BleError::AdapterUnavailable(
            "tao's Android context is not available yet".to_string(),
        ));
    };
    // `ndk_context::android_context()` panics on first read if this was
    // never called — `initialize_android_context` itself asserts it is
    // only ever called once, which `OnceCell::get_or_try_init` in
    // `LazyAndroidBackend::inner` guarantees for us.
    unsafe {
        ndk_context::initialize_android_context(ctx.java_vm, ctx.context_jobject);
    }
    Ok(())
}

pub struct LazyAndroidBackend {
    cell: OnceCell<AndroidBackend>,
}

impl LazyAndroidBackend {
    pub fn new() -> Self {
        Self { cell: OnceCell::new() }
    }

    async fn inner(&self) -> Result<&AndroidBackend> {
        self.cell
            .get_or_try_init(|| async {
                bridge_ndk_context_from_tao()?;
                AndroidBackend::new().await
            })
            .await
    }
}

#[async_trait]
impl Backend for LazyAndroidBackend {
    async fn capabilities(&self) -> CapabilityReport {
        match self.inner().await {
            Ok(backend) => backend.capabilities().await,
            Err(err) => {
                eprintln!("[ble-gatt][android] backend construction failed: {err}");
                CapabilityReport::default()
            }
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<DiscoveredPeer>> {
        self.inner().await?.scan(service).await
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        self.inner().await?.connect(peer).await
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        self.inner().await?.advertise(service).await
    }

    async fn stop_advertising(&self) -> Result<()> {
        self.inner().await?.stop_advertising().await
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.inner().await?.notify(characteristic, value).await
    }

    fn events(&self) -> BoxStream<GattEvent> {
        match self.cell.get() {
            Some(backend) => backend.events(),
            // Nothing has been constructed yet, so there is genuinely
            // nothing to report — honest empty stream, not a fabricated one.
            None => Box::pin(tokio_stream::empty()),
        }
    }
}
