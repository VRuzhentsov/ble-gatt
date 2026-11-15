# tauri-plugin-ble-gatt: application access and platform setup

This crate connects a Tauri application to
[`ble-gatt`](../ble-gatt/README.md). It supplies Rust access, JavaScript
commands, Android permission handling and native bridge packaging.
It is not a networking transport and has no iroh dependency.

## Status: current code and agreed target

The [shared design](../docs/adr/0012-discovery-like-nearby.md) keeps BLE peer
behavior in the core and Tauri setup here. The core handshake is not implemented.
No symmetric-peer JavaScript API or Android bridge relocation is implemented here.

## Responsibilities

- Register the Tauri plugin and its Android mobile plugin.
- Supply the default platform driver lazily, or accept an injected driver.
- Return one shared core `Adapter` through `app.ble_gatt().adapter().await`.
- Expose GATT commands and role/session events to JavaScript.
- Check and request Android runtime permissions through Tauri.
- Capture an owned Android Application context for JNI construction.
- Package the Kotlin bridge and callbacks with the plugin.
- Request bounded driver cleanup on Linux application exit.

BLE framing, symmetric connection behavior and collision recovery belong to the
core. QUIC, peer-key routing and iroh path hooks belong to iroh and its adapter.
The frontend must not recreate either implementation with its own retry timers.

## How applications compose the crates

```text
Tauri frontend -- commands/events --> Tauri plugin
                                           |
Tauri Rust application -- shared Adapter ---+
                                           |
                                        ble-gatt

Tauri Rust application --> iroh --> ble-gatt-iroh --> ble-gatt
```

The second path uses the plugin's shared core adapter without sending QUIC
packets through JavaScript IPC. The plugin supplies setup/access, not another
packet layer. Non-Tauri hosts can use the core directly; Android still needs
the bridge/context prerequisites below.

## Current entrypoints

- `init` registers the default plugin.
- `Builder::driver` injects a driver, such as a mock.
- `BleGattExt` exposes `app.ble_gatt()` to Rust application code.
- `adapter` returns the core `Adapter`; `permission_state` and
  `request_permissions` expose permission handling.
- [`guest-js/index.ts`](guest-js/index.ts) provides capabilities, raw scan,
  connect, read, write, notify, advertise, disconnect, MTU and event access.

The JavaScript functions are raw GATT operations. They do not currently expose
a managed symmetric-peer channel or iroh streams. `scanOnce` does not silently
open temporary connections to complete profile metadata.

A connection handle in frontend IPC, a driver connection-generation identifier
and an authenticated application session are different values.

## Android context and bridge ownership

The current default path is:

```text
BleGattPlugin.kt registration
        |
owned JVM / Application reference in android_context
        |
LazyDriver --> AndroidDriver::with_context
        |
Rust JNI calls <--> BleGattBridge.kt / Native.kt callbacks
```

The default driver uses `android_context::get()` after registration. It does not
initialize the runtime's `ndk-context` global or depend on the application's Tao
version. The Application reference is owned; an Activity is not retained.

Legacy `android_context::ensure()` has a different contract: it is only for a
host that explicitly delegates `ndk-context` initialization to this plugin.
It must not initialize a global already owned by the runtime. Use `get()` for
new explicit-context integration.

`request_permissions` blocks while the user answers on Android. Call it from a
blocking task rather than the UI thread. Permission grants do not by themselves
prove that background discovery or a locked-device scenario works.

The Kotlin implementation currently lives in this crate's [`android/`](android)
directory, while the Rust driver lives in the core. Host-neutral packaging
remains open; no new crate or bridge relocation is selected.

## Modules and acceptance

```text
src/lib.rs              Registration, Rust API and platform construction
src/lazy.rs             Lazy shared driver forwarding
src/commands.rs         Tauri GATT commands and IPC event delivery
src/android_context.rs  Android JVM/Application ownership
android/                Kotlin plugin, GATT bridge and native callbacks
guest-js/index.ts       JavaScript access to commands and events
tests/jni_parity.rs     Source-level Rust/Kotlin contract checks
```

Source parity checks, compiled JVM descriptors, packaged app execution and real
Bluetooth behavior are separate checks. None substitutes for the next.
Android diagnostic foreground-service code under the sample application is
example-only; it is not the plugin's production connection manager.

See the [architecture map](../docs/architecture.md),
[Android bridge decision](../docs/adr/0002-android-jni-bridge.md) and
[core interface guide](../docs/interface.md).
