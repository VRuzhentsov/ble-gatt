//! The Android driver and the Kotlin bridge meet only by name: Rust exports
//! `Java_dev_blegatt_NativeKt_<name>` for each Kotlin `external fun`, and
//! calls `BleGattBridge` methods by name over JNI. Nothing checks either at
//! compile time, and a mismatch shows up only on a device, as
//! `UnsatisfiedLinkError` or `NoSuchMethodError`. This test reads both
//! sides as text and compares the names.

use std::collections::BTreeSet;
use std::path::Path;

const KOTLIN_DIR: &str = "android/src/main/kotlin/dev/blegatt";
const DRIVER_DIR: &str = "../ble-gatt/src/drivers/android";
const JNI_EXPORT_PREFIX: &str = "Java_dev_blegatt_NativeKt_";

/// Methods Rust calls on standard Java classes, not on the bridge.
const JAVA_STANDARD_METHODS: &[&str] = &["toString", "getClassLoader", "loadClass"];

fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("reading {}: {err}", path.display()))
}

/// The Android driver's source: every `.rs` file of its module, in name order.
fn read_driver() -> String {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(DRIVER_DIR);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|path| {
            std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()))
        })
        .collect()
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
    let rust = read_driver();
    let declared = names_after(&kotlin, "external fun ");
    let exported = names_after(&rust, &format!("fn {JNI_EXPORT_PREFIX}"));
    assert!(!declared.is_empty(), "no external funs found in Native.kt");
    assert_eq!(
        declared, exported,
        "Native.kt externs (left) and Rust JNI exports (right) differ"
    );
}

/// Source-level guard; compiled descriptors can also be checked with `javap`.
#[test]
fn scan_start_status_and_abi_match_on_both_sides() {
    let kotlin = read(&format!("{KOTLIN_DIR}/BleGattBridge.kt"));
    let rust = read_driver();
    assert!(kotlin.contains("fun startScan(serviceUuid: String, generation: Long): Int"));
    assert!(rust.contains("\"(Ljava/lang/String;J)I\""));
    assert!(rust.contains("BleError::android_scan_start_result(status)?"));
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
    assert!(scan.contains("if (scanCallback === callback) scanCallback = null"));
    assert!(
        scan.contains("throw error"),
        "scan exceptions must reach JNI after ownership rollback"
    );
    assert!(rust.contains("const BRIDGE_ABI_VERSION: i32 = 8;"));
    assert!(kotlin.contains("fun bridgeAbiVersion(): Int = 8"));
}

/// ABI 8: both advertising calls end with the scan response's service data,
/// the fast advertisement (company, data) and the advertise mode, in the
/// same order on both sides.
#[test]
fn advertising_descriptors_match_the_kotlin_parameters() {
    let kotlin = read(&format!("{KOTLIN_DIR}/BleGattBridge.kt"));
    let rust = read_driver();
    let compact = |text: &str| {
        text.chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
    };
    let kotlin = compact(&kotlin);
    let rust = compact(&rust);
    assert!(rust.contains(
        "\"startAdvertising\",\"(Ljava/lang/String;[Ljava/lang/String;[Z[Z[Z[[B[I[[B[Ljava/lang/String;[[B[BI[BI)V\""
    ));
    assert!(rust.contains(
        "\"updateAdvertising\",\"(Ljava/lang/String;[I[[B[Ljava/lang/String;[[B[BI[BI)V\""
    ));
    let tail = "JValue::Object(&ad.scan_response_service_data),JValue::Int(ad.fast_company_id),JValue::Object(&ad.fast_data),JValue::Int(ad.advertise_mode),]";
    assert_eq!(
        rust.matches(tail).count(),
        2,
        "both calls pass the ABI 8 tail"
    );
    // The parameters of startAdvertising, in order (its comments sit between them).
    let start = kotlin
        .split("funstartAdvertising(")
        .nth(1)
        .unwrap()
        .split("stopAdvertising()")
        .next()
        .unwrap();
    let mut at = 0;
    for parameter in [
        "manufacturerIds:IntArray,manufacturerValues:Array<ByteArray>,",
        "serviceDataUuids:Array<String>,serviceDataValues:Array<ByteArray>,",
        "scanResponseServiceData:ByteArray?,",
        "fastCompanyId:Int,fastData:ByteArray?,",
        "advertiseMode:Int,){",
    ] {
        let found = start[at..]
            .find(parameter)
            .unwrap_or_else(|| panic!("startAdvertising lacks {parameter} after byte {at}"));
        at += found + parameter.len();
    }
    assert!(kotlin.contains(
        "funupdateAdvertising(serviceUuid:String,manufacturerIds:IntArray,manufacturerValues:Array<ByteArray>,serviceDataUuids:Array<String>,serviceDataValues:Array<ByteArray>,scanResponseServiceData:ByteArray?,fastCompanyId:Int,fastData:ByteArray?,advertiseMode:Int,)"
    ));
    let placement = read(&format!("{KOTLIN_DIR}/AdvertisePlacement.kt"));
    assert!(placement.contains("const val FAST_ADVERTISEMENT_MAX_LEN: Int = 27"));
    let models = read("../ble-gatt/src/entities/models.rs");
    assert!(models.contains("pub const FAST_ADVERTISEMENT_MAX_LEN: usize = 27;"));
    // The scan filter: the same tag and default company on both sides.
    assert!(placement.contains("const val FAST_ADVERTISEMENT_TAG: Int = 0xFC"));
    assert!(models.contains("pub const TAG: u8 = 0xFC;"));
    assert!(placement.contains("const val DEFAULT_SCAN_COMPANY: Int = 0xFFFF"));
    assert!(models.contains("pub const DEFAULT_SCAN_COMPANY: u16 = 0xFFFF;"));
    assert!(kotlin.contains(
        "AdvertisePlacement.isFastAdvertisement(record?.getManufacturerSpecificData(fastCompany))"
    ));
}

/// Source-contract regression for duplicate Tao versions. Runtime acceptance
/// additionally requires launching a packaged app with a different Tao version.
#[test]
fn android_context_is_supplied_by_the_registered_plugin_not_tao_statics() {
    let manifest = read("Cargo.toml");
    let context = read("src/android_context.rs");
    let plugin = read(&format!("{KOTLIN_DIR}/BleGattPlugin.kt"));
    assert!(
        !manifest.lines().any(|line| line.starts_with("tao =")),
        "a plugin-local Tao dependency has Android statics distinct from the app runtime"
    );
    assert!(!context.contains("main_android_context"));
    assert!(plugin.contains("initializeContext(activity.applicationContext)"));
    assert!(
        plugin.contains("private external fun initializeContext(context: android.content.Context)")
    );
    assert!(context.contains("fn Java_dev_blegatt_BleGattPlugin_initializeContext("));
    let compact: String = context.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(compact.contains("env.get_java_vm()"));
    assert!(compact.contains("env.new_global_ref(context)"));
    let initialize = context.split("fn initialize(").nth(1).unwrap();
    assert!(
        !initialize.contains("initialize_android_context"),
        "the runtime may already own ndk-context; plugin registration must not initialize it again"
    );
    let plugin_rust = read("src/lib.rs");
    assert!(plugin_rust.contains("AndroidDriver::with_context("));
    let driver = read_driver();
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
    let rust = read_driver();
    let defined = names_after(&kotlin, "fun ");
    let called = called_methods(&rust);
    assert!(
        !called.is_empty(),
        "no JNI method calls found in the Android driver"
    );
    let missing: Vec<_> = called.difference(&defined).collect();
    assert!(
        missing.is_empty(),
        "Rust calls BleGattBridge methods Kotlin does not define: {missing:?}"
    );
}
