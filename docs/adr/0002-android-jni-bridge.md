# 0002 — Android backend: raw JNI bridge, lazy construction, and what's verified

## Status

Accepted, implemented, and partially verified. The JNI bridge chain (Rust
`Backend` trait → Kotlin `BleGattBridge` → real Android `BluetoothManager`
APIs → back to Rust → Tauri IPC → JS) is proven working end-to-end on a real
Android runtime with zero crashes. A genuine two-device BLE round trip is
**not yet verified** — see "What's verified vs. deferred" below.

## Context

Stage 1 (ADR-0001) built the Linux backend and deferred Android, with the
Kotlin/JNI packaging split deliberately left undecided until Stage 2 began.
Implementing it surfaced two real architectural problems that weren't
visible from the plan alone — this ADR records both the decisions and the
concrete evidence (crash logs, not guesses) that drove them.

## Decision

### The Kotlin bridge is a plain class, not a `@TauriPlugin`

`tauri-plugin-ble-gatt/android/src/main/kotlin/dev/blegatt/BleGattBridge.kt`
does **not** extend Tauri's `Plugin` base class or carry `@TauriPlugin`.
This was tempting to change once real Tauri Android plugin conventions were
in view (`tauri-plugin-notification`'s `NotificationPlugin.kt`, which *does*
extend `Plugin` and dispatches JS `invoke()` calls straight to Kotlin,
bypassing Rust entirely on mobile) — that pattern is lower-risk and better
trodden. It was rejected anyway: routing through Tauri's mobile IPC would
make the entire Android GATT implementation reachable only from a Tauri
app, contradicting the plan's explicit goal (`ble-gatt` usable by
non-Tauri consumers like `keepsmile-lamp`). `BleGattBridge` is called by
raw JNI (`env.call_method`) from `ble-gatt/src/backend/android.rs`, with
its own `Native.kt` callback surface — a hypothetical non-Tauri Android Rust
app would copy this one `.kt` file plus the manifest permissions into their
own project, no Tauri Android runtime required.

`tauri-plugin-ble-gatt/android/` still has to exist and ship real Gradle
files (`build.gradle.kts`, `AndroidManifest.xml`) — Tauri's build tooling
needs a valid Android library module there (wired via
`tauri_plugin::Builder::new(COMMANDS).android_path("android")` in
`build.rs`) to compile the Kotlin and merge the manifest permissions into
the generated app. It just doesn't use Tauri's plugin *class* conventions.

### Gradle: no version pin in the module's `plugins {}` block

First real build attempt against a generated Tauri Android app failed:

```
Error resolving plugin [id: 'com.android.library', version: '8.11.0']
> The request for this plugin could not be satisfied because the plugin is
  already on the classpath with an unknown version, so compatibility cannot
  be checked.
```

`android/build.gradle.kts` had pinned `com.android.library`/
`org.jetbrains.kotlin.android` versions explicitly (needed for this repo's
own standalone `./gradlew build` verification loop, which has no parent
project to supply them). But when Gradle includes this directory as a
*subproject* of a generated Tauri app, the app's own root build already
resolves those plugins — a subproject re-pinning a version conflicts with
it. Every official Tauri plugin (checked directly:
`tauri-plugin-notification`'s `android/build.gradle.kts`) leaves the
`plugins {}` block unversioned for exactly this reason. Fixed by moving the
version pin to `android/settings.gradle.kts`'s `pluginManagement.plugins {}`
block instead — Gradle reads only one settings file per build, so that
block is consulted for this repo's own standalone build and silently
ignored (not even ambiguous — genuinely not read) when a generated app's
own settings.gradle is the real root. Both scenarios now build clean.

### `ndk_context::android_context()` panics inside Tauri — bridged from `tao`

`AndroidBackend::new()` originally read `ndk_context::android_context()`
directly — the standard, documented interop point most non-Tauri Android
Rust runtimes (`android-activity`, `winit`, `cargo-apk`) populate
automatically. First real device run crashed immediately:

```
thread '<unnamed>' (5150) panicked at
  .../ndk-context-0.1.1/src/lib.rs:72:30: android context was not initialized
```

Checked directly (not assumed): neither `tauri`, `tao`, nor `wry`'s source
calls `ndk_context::initialize_android_context()` anywhere. Tauri's Android
runtime keeps its own separate context
(`tao::platform_impl::android::ndk_glue::main_android_context()`, a
different `AndroidContext` type with the same `java_vm`/`context_jobject`
raw-pointer shape). `ble-gatt` itself was not changed — it still reads
`ndk_context::android_context()`, staying honestly cross-runtime.
`tauri-plugin-ble-gatt/src/android_lazy.rs` bridges the two once, at
runtime: `ndk_context::initialize_android_context(ctx.java_vm,
ctx.context_jobject)` using the context `tao` already has. This keeps the
Tauri-specific coupling in the Tauri-specific crate, where it belongs.

### Backend construction is lazy on Android, eager on Linux

Same crash trace showed the panic happening inside
`tao::platform_impl::platform::ndk_glue::create` — i.e. the plugin's
`.setup()` hook runs **synchronously from inside `tao`'s own Android
context bring-up**, before `main_android_context()` can be relied on.
Eager construction (what Linux does, safely, since `LinuxBackend::new()`
just needs a D-Bus connection with no Android-activity-lifecycle
dependency) doesn't work here.

`android_lazy::LazyAndroidBackend` implements `Backend` as a thin
`tokio::sync::OnceCell`-backed proxy: every trait method calls
`get_or_try_init` on the cell, which runs the `tao` → `ndk-context` bridge
and `AndroidBackend::new()` on first real use, not at plugin setup. By the
time any command reaches it, the WebView/Activity is unquestionably alive
(the user is looking at a rendered page). The one non-`async` trait method,
`events()`, returns an empty stream if the cell isn't populated yet instead
of forcing synchronous construction — honest (nothing has been constructed,
so there's genuinely nothing to report yet) rather than blocking.

### Local dev-loop friction (recorded so it isn't re-debugged blind)

Two more real, non-obvious failures hit while getting `tauri android build`
working against the example app, both now fixed in `examples/tauri-app`:

- **`npx tauri` vs. an absolute-path CLI binary.** Running the CLI via an
  absolute path outside the project (borrowing another repo's installed
  `node_modules/.bin/tauri`) leaves the example app's own Gradle build
  unable to resolve a JS module it expects relative to `src-tauri` at
  build time (`Cannot find module '.../src-tauri/tauri'` — a Gradle
  `BuildTask` shells out `node tauri android android-studio-script` with
  `src-tauri` as the working directory, which needs an actual `tauri`/
  `tauri.js` file resolvable from *that* directory, not just the CLI
  callable from PATH). Fixed by installing `@tauri-apps/cli` locally
  (`npm install` in `examples/tauri-app`) and adding a `tauri.js` symlink to
  the CLI's entry script directly in `src-tauri/` so Node's extensionless
  module resolution finds it.

## What's verified vs. deferred

**Verified, with concrete evidence:**
- `tauri-plugin-ble-gatt/android/` builds clean standalone (`./gradlew
  build`, zero errors after disabling the `MissingPermission` lint category
  — deliberate, see `BleGattBridge.kt`'s doc comment) and as a subproject of
  a real generated Tauri Android app.
- `ble-gatt`'s `backend/android.rs` cross-compiles clean (`cargo check`, zero
  warnings after fixes) for both `x86_64-linux-android` (emulators) and
  `aarch64-linux-android` (real devices).
- The full example app (`examples/tauri-app`) builds a real, installable
  APK with the plugin compiled in.
- Installed and launched on a real Android 14 emulator
  (`fini-e2e`, API 34, google_apis, x86_64): the app runs with **no crash**.
- Tapped the `capabilities()` button in the running app: JS `invoke()` →
  Tauri IPC → `LazyAndroidBackend` → lazy `tao`→`ndk-context` bridge → JNI
  attach → `BleGattBridge` construction → real
  `BluetoothManager`/`BluetoothAdapter` queries → JNI return → back through
  the whole chain to the UI, rendering `{"central": false, "peripheral":
  false}`. This is the real bridge executing top to bottom, not a stub.

**Investigated, not a bug:** granting all three runtime Bluetooth
permissions (`BLUETOOTH_CONNECT`/`SCAN`/`ADVERTISE` via `adb shell pm
grant`) and restarting did not change the `false`/`false` result — ruling
out the most likely simple explanation. `dumpsys bluetooth_manager` showed
the emulator's virtual controller enabled and running (Root Canal address
`BB:BB:BB:00:00:02`), but this specific AVD's controller apparently doesn't
expose `bluetoothLeScanner`/`isMultipleAdvertisementSupported` the way a
real device would. Not chased further this session — recorded as a known
gap, not silently assumed fixed.

**Explicitly deferred:** a genuine two-device BLE round trip (central
discovers, connects to, and exchanges data with a real peripheral over the
air) is **not verified**. Given the single-emulator capability query above,
attempting it on this same AVD would not succeed regardless of running two
instances — `scan()`/`advertise()` would silently no-op the same way
`capabilities()` did. Needs either a different AVD/system-image
configuration with confirmed BLE support, or physical hardware. This is the
same honesty-tiered pattern Stage 1 used for `two_adapter_central_to_peripheral_round_trip`
(written, `#[ignore]`d, documented reason) — not faked here either.

## Consequences

- A future session picking up the two-device verification should start by
  checking whether a *different* AVD (newer system image, or explicitly
  configured hardware Bluetooth profile) reports `central`/`peripheral` as
  `true` before assuming the bridge code itself needs changes.
- `examples/tauri-app` is now a real, working verification harness — reusable
  for the eventual macOS/iOS/Windows backends too, not Android-specific.
- The `tao` → `ndk-context` bridge is a hard dependency of
  `tauri-plugin-ble-gatt` on Android (new `tao`/`ndk-context` target
  dependencies in its `Cargo.toml`) but changes nothing about `ble-gatt`
  itself — a non-Tauri Android consumer never needs `tao`.
