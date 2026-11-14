//! The Android driver and the Kotlin bridge meet only by name: Rust exports
//! `Java_dev_blegatt_NativeKt_<name>` for each Kotlin `external fun`, and
//! calls `BleGattBridge` methods by name over JNI. Nothing checks either at
//! compile time, and a mismatch shows up only on a device, as
//! `UnsatisfiedLinkError` or `NoSuchMethodError`. This test reads both
//! sides as text and compares the names.

use std::collections::BTreeSet;
use std::path::Path;

const KOTLIN_DIR: &str = "android/src/main/kotlin/dev/blegatt";
const DRIVER: &str = "../ble-gatt/src/drivers/android.rs";
const JNI_EXPORT_PREFIX: &str = "Java_dev_blegatt_NativeKt_";

/// Methods Rust calls on standard Java classes, not on the bridge.
const JAVA_STANDARD_METHODS: &[&str] = &["toString", "getClassLoader", "loadClass"];

fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("reading {}: {err}", path.display()))
}

fn identifier_at(text: &str) -> &str {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    &text[..end]
}

/// Names following `marker` in `source`.
fn names_after(source: &str, marker: &str) -> BTreeSet<String> {
    source
        .match_indices(marker)
        .map(|(at, _)| identifier_at(&source[at + marker.len()..]).to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

/// Method names Rust passes to JNI calls: a string literal holding an
/// identifier, followed by a string literal holding a JNI signature.
fn called_methods(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for (at, _) in source.match_indices('"') {
        let rest = &source[at + 1..];
        let name = identifier_at(rest);
        if name.is_empty() || !rest[name.len()..].starts_with('"') {
            continue;
        }
        let after = rest[name.len() + 1..].trim_start();
        let after = after.strip_prefix(',').map(str::trim_start).unwrap_or("");
        if after.starts_with("\"(") && !JAVA_STANDARD_METHODS.contains(&name) {
            names.insert(name.to_string());
        }
    }
    names
}

#[test]
fn every_kotlin_external_fun_has_a_rust_export_and_back() {
    let kotlin = read(&format!("{KOTLIN_DIR}/Native.kt"));
    let rust = read(DRIVER);
    let declared = names_after(&kotlin, "external fun ");
    let exported = names_after(&rust, &format!("fn {JNI_EXPORT_PREFIX}"));
    assert!(!declared.is_empty(), "no external funs found in Native.kt");
    assert_eq!(declared, exported, "Native.kt externs (left) and Rust JNI exports (right) differ");
}

/// Source-level guard; compiled descriptors can also be checked with `javap`.
#[test]
fn scan_start_status_and_abi_match_on_both_sides() {
    let kotlin = read(&format!("{KOTLIN_DIR}/BleGattBridge.kt"));
    let rust = read(DRIVER);
    assert!(kotlin.contains("fun startScan(serviceUuid: String, generation: Long): Int"));
    assert!(rust.contains("\"(Ljava/lang/String;J)I\""));
    assert!(rust.contains("super::android_scan_start_result(status)?"));
    for (name, value) in [
        ("SCAN_STARTED", 0),
        ("SCAN_BUSY", 1),
        ("SCAN_UNAVAILABLE", 2),
    ] {
        assert!(kotlin.contains(&format!("private const val {name}: Int = {value}")));
    }
    let scan = kotlin
        .split("fun startScan(")
        .nth(1)
        .unwrap()
        .split("fun stopScan(")
        .next()
        .unwrap();
    assert!(scan.contains("return SCAN_BUSY"));
    assert!(scan.contains("adapter?.bluetoothLeScanner ?: return SCAN_UNAVAILABLE"));
    assert!(scan.contains("return SCAN_STARTED"));
    assert!(
        !scan.contains("catch ("),
        "scan permission/platform exceptions must reach JNI"
    );
    assert!(rust.contains("const BRIDGE_ABI_VERSION: i32 = 6;"));
    assert!(kotlin.contains("fun bridgeAbiVersion(): Int = 6"));
}

/// Source-contract regression for duplicate Tao versions. Runtime acceptance
/// additionally requires launching a packaged app with a different Tao version.
#[test]
fn android_context_is_supplied_by_the_registered_plugin_not_tao_statics() {
    let manifest = read("Cargo.toml");
    let context = read("src/android_context.rs");
    let plugin = read(&format!("{KOTLIN_DIR}/BleGattPlugin.kt"));
    assert!(!manifest.lines().any(|line| line.starts_with("tao =")),
        "a plugin-local Tao dependency has Android statics distinct from the app runtime");
    assert!(!context.contains("main_android_context"));
    assert!(plugin.contains("initializeContext(activity.applicationContext)"));
    assert!(plugin.contains("private external fun initializeContext(context: android.content.Context)"));
    assert!(context.contains("fn Java_dev_blegatt_BleGattPlugin_initializeContext("));
    let compact: String = context.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(compact.contains("env.get_java_vm()"));
    assert!(compact.contains("env.new_global_ref(context)"));
    let initialize = context.split("fn initialize(").nth(1).unwrap();
    assert!(!initialize.contains("initialize_android_context"),
        "the runtime may already own ndk-context; plugin registration must not initialize it again");
    let plugin_rust = read("src/lib.rs");
    assert!(plugin_rust.contains("AndroidDriver::with_context("));
    let driver = read(DRIVER);
    assert!(driver.contains("pub async fn with_context("));
}

/// The diagnostic never derives location. Android 12+ otherwise withholds
/// scan results despite a Bluetooth grant when fine location is capped at 30.
#[test]
fn diagnostic_declares_non_location_scanning() {
    let manifest = read("../examples/tauri-app/android-diagnostic/AndroidManifest.xml");
    assert!(manifest.contains("android.permission.BLUETOOTH_SCAN"));
    assert!(manifest.contains("android:usesPermissionFlags=\"neverForLocation\""));
}

#[test]
fn every_bridge_method_rust_calls_exists_in_kotlin() {
    let kotlin = read(&format!("{KOTLIN_DIR}/BleGattBridge.kt"));
    let rust = read(DRIVER);
    let defined = names_after(&kotlin, "fun ");
    let called = called_methods(&rust);
    assert!(!called.is_empty(), "no JNI method calls found in the Android driver");
    let missing: Vec<_> = called.difference(&defined).collect();
    assert!(missing.is_empty(), "Rust calls BleGattBridge methods Kotlin does not define: {missing:?}");
}
