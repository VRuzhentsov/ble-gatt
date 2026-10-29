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
//!
//! What is copied is the Application context, not `tao`'s Activity. `tao`
//! deletes its reference to the Activity when the Activity is destroyed
//! (the app left in the background, say) while the process keeps running,
//! so a driver built after that read a dead reference and the JVM aborted.
//! The Application lives as long as the process, and this module holds its
//! own reference to it. Precedent: `n0-dns-resolver`'s
//! `install_android_jni_context` takes the application context too, and
//! Android's guidance is to use it for anything that outlives an Activity.

use std::sync::OnceLock;

use jni::objects::{GlobalRef, JObject};
use jni::JavaVM;

/// Copies the Application context into `ndk-context`, once per process.
/// Safe to call from anywhere, any number of times, after the Activity is
/// up; before that it returns an error and a later call tries again.
pub fn ensure() -> Result<(), String> {
    // Owns the reference `ndk-context` points at; never dropped.
    static APPLICATION: OnceLock<GlobalRef> = OnceLock::new();
    if APPLICATION.get().is_some() {
        return Ok(());
    }
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if APPLICATION.get().is_some() {
        return Ok(());
    }
    use tao::platform::android::prelude::main_android_context;
    let ctx = main_android_context()
        .ok_or_else(|| "tao's Android context is not available yet".to_string())?;
    let vm = unsafe { JavaVM::from_raw(ctx.java_vm.cast()) }
        .map_err(|err| format!("JavaVM::from_raw failed: {err}"))?;
    let mut env = vm
        .attach_current_thread_as_daemon()
        .map_err(|err| format!("JNI attach failed: {err}"))?;
    let activity = unsafe { JObject::from_raw(ctx.context_jobject.cast()) };
    let application = env
        .call_method(&activity, "getApplicationContext", "()Landroid/content/Context;", &[])
        .and_then(|value| value.l())
        .and_then(|application| env.new_global_ref(application))
        .map_err(|err| {
            let _ = env.exception_clear();
            format!("getApplicationContext failed: {err}")
        })?;
    unsafe {
        ndk_context::initialize_android_context(ctx.java_vm, application.as_obj().as_raw().cast());
    }
    let _ = APPLICATION.set(application);
    Ok(())
}
