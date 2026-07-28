//! M1 (second half), in progress — not implemented yet. Hand-written via
//! raw `jni` + `ndk-context` rather than an existing crate: no permissively
//! licensed Rust crate does BLE peripheral (GATT server) mode on Android —
//! see the plan's library-research table and this crate's README. Verified
//! via two Android emulator instances communicating over Netsim/Root Canal
//! (Android's virtual Bluetooth controller) once implemented.
