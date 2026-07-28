# Android plugin scaffolding — deferred to Stage 2

This directory intentionally has no Kotlin/Gradle project yet. The Android
backend (`ble-gatt`'s `backend/android.rs`, hand-written via raw `jni` +
`ndk-context`) is Stage 2 work, in progress. The exact split between what
lives in `ble-gatt`'s Rust/JNI layer versus this `android/` Tauri
mobile-plugin folder is deliberately decided when that work starts, not
pre-committed here — see the plan's explicit deferral note.

`blew`'s repo structure (AGPL-3.0, not reused as code) is the intended
pattern reference for that split.
