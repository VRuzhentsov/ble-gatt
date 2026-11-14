//! Process-lifetime Application context supplied by Tauri's registered
//! Android plugin, not by a particular version of its windowing runtime.
//!
//! `register_android_plugin` constructs `BleGattPlugin` with the app's
//! Activity. Its constructor passes `activity.applicationContext` through
//! JNI before registration returns. Keeping a global reference makes this
//! independent of Activity destruction/recreation and of Tao versions.
//!
//! The default driver uses [`get`] and never initializes `ndk-context`:
//! recent runtimes already own that global. [`ensure`] is retained only for
//! legacy applications which explicitly delegate ndk-context ownership to
//! this plugin; see its precondition before using it in a newer runtime.

use std::sync::{Mutex, OnceLock};

use jni::objects::{GlobalRef, JObject};
use jni::{JNIEnv, JavaVM};

struct Context {
    vm: JavaVM,
    application: GlobalRef,
}

// Owns the JNI reference for the remainder of the process.
static APPLICATION: OnceLock<Context> = OnceLock::new();
static LOCK: Mutex<()> = Mutex::new(());

/// Returns owned JNI handles to the registered Application context. Safe
/// on any thread; a failure before plugin registration is not cached.
/// This does not read or modify the runtime's ndk-context global.
pub fn get() -> Result<(JavaVM, GlobalRef), String> {
    let ctx = APPLICATION.get().ok_or_else(|| {
        "Android Application context is not initialized; register the ble-gatt plugin first"
            .to_string()
    })?;
    // JavaVM is a non-owning JVM handle. The process JVM outlives this copy.
    let vm = unsafe { JavaVM::from_raw(ctx.vm.get_java_vm_pointer()) }
        .map_err(|err| format!("JavaVM handle unavailable: {err}"))?;
    Ok((vm, ctx.application.clone()))
}

/// Legacy opt-in: installs this Application into `ndk-context` once.
///
/// Only use this when the application/runtime does **not** initialize
/// ndk-context itself (e.g. older Tauri runtimes). Like the previous API,
/// all initializers must cooperate through this function. Newer Tao
/// runtimes already initialize ndk-context: use [`get`] for app JNI instead.
/// There is no non-panicking `try_get` in ndk-context 0.1, so this function
/// cannot discover or safely take over an independently owned global.
/// The plugin's default driver does not call this legacy function.
pub fn ensure() -> Result<(), String> {
    let (vm, application) = get()?;
    static NDK_CONTEXT: OnceLock<()> = OnceLock::new();
    NDK_CONTEXT.get_or_init(|| unsafe {
        ndk_context::initialize_android_context(
            vm.get_java_vm_pointer().cast(),
            application.as_obj().as_raw().cast(),
        );
    });
    Ok(())
}

fn initialize(env: &mut JNIEnv<'_>, context: JObject<'_>) -> Result<(), String> {
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if APPLICATION.get().is_some() {
        return Ok(());
    }
    if context.is_null() {
        return Err("Android Application context is null".into());
    }
    let vm = env
        .get_java_vm()
        .map_err(|err| format!("JNI JavaVM unavailable: {err}"))?;
    let application = env
        .new_global_ref(context)
        .map_err(|err| format!("retaining Android Application context failed: {err}"))?;
    let _ = APPLICATION.set(Context { vm, application });
    Ok(())
}

/// Called synchronously by the app-owned Kotlin plugin's constructor.
/// Errors propagate through JNI to Tauri's register_android_plugin result.
#[no_mangle]
pub extern "system" fn Java_dev_blegatt_BleGattPlugin_initializeContext(
    mut env: JNIEnv<'_>,
    _plugin: JObject<'_>,
    context: JObject<'_>,
) {
    if let Err(err) = initialize(&mut env, context) {
        // Preserve an existing JVM exception (e.g. an allocation failure).
        if !env.exception_check().unwrap_or(true) {
            let _ = env.throw_new("java/lang/IllegalStateException", err);
        }
    }
}
