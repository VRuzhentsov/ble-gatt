package dev.blegatt

/**
 * Native callbacks implemented in Rust (`ble-gatt/src/backend/android.rs`,
 * `#[no_mangle] extern "system" fn Java_dev_blegatt_NativeKt_<name>`).
 * Declared as top-level functions (not class members) so Kotlin compiles
 * them as static methods on `NativeKt` — a fixed, unmangled JNI symbol name
 * `BleGattBridge`'s own instance methods would not have.
 *
 * Every callback carries `nativeHandle`, the `jlong` a `BleGattBridge` was
 * constructed with: a raw pointer to the Rust-side channel state
 * (`Box::into_raw` on the Rust side; reconstructed with
 * `&*(native_handle as *const _)` in each callback, never taking ownership).
 * This is the standard stateful-JNI-callback pattern — see the module doc
 * comment on `android.rs` for the full contract.
 */
external fun onPeerDiscovered(nativeHandle: Long, address: String, name: String?)

external fun onConnected(nativeHandle: Long, address: String)

external fun onDisconnected(nativeHandle: Long, address: String)

external fun onCharacteristicRead(
    nativeHandle: Long, address: String, characteristicUuid: String, value: ByteArray, success: Boolean
)

external fun onCharacteristicWriteResult(
    nativeHandle: Long, address: String, characteristicUuid: String, success: Boolean
)

external fun onCharacteristicChanged(
    nativeHandle: Long, address: String, characteristicUuid: String, value: ByteArray
)

external fun onServerCharacteristicWritten(
    nativeHandle: Long, address: String, characteristicUuid: String, value: ByteArray
)

external fun onSubscribed(nativeHandle: Long, address: String, characteristicUuid: String)
