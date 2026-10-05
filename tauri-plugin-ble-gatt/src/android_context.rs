//! The Android context `ble-gatt` needs, set up once for the whole process.
//!
//! `ble-gatt`'s Android driver reads `ndk_context::android_context()`, the
//! interop point most Android Rust runtimes (`android-activity`, `winit`,
//! `cargo-apk`) fill in. Tauri's runtime (`tao`) keeps its own context
//! instead, so it has to be copied across once.
//! `ndk_context::initialize_android_context` asserts it is only ever called
//! once, so an application that also calls Java through `ndk-context` (Fini
//! does, for its own Kotlin classes) must go through [`ensure`] rather than
//! initializing it again.

use std::sync::OnceLock;

/// Copies `tao`'s Android context into `ndk-context`, once per process.
/// Safe to call from anywhere, any number of times, after the Activity is
/// up; before that it returns an error and a later call tries again.
pub fn ensure() -> Result<(), String> {
    static BRIDGED: OnceLock<()> = OnceLock::new();
    if BRIDGED.get().is_some() {
        return Ok(());
    }
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if BRIDGED.get().is_some() {
        return Ok(());
    }
    use tao::platform::android::prelude::main_android_context;
    let ctx = main_android_context()
        .ok_or_else(|| "tao's Android context is not available yet".to_string())?;
    unsafe {
        ndk_context::initialize_android_context(ctx.java_vm, ctx.context_jobject);
    }
    let _ = BRIDGED.set(());
    Ok(())
}
