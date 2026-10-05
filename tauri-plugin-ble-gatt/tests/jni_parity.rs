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
