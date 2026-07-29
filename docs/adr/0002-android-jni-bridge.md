# 0002 — Android backend: raw JNI bridge, lazy construction, and what's verified

## Status

Accepted and implemented. **Not verified as a working bridge** — that bar is
a real device-to-device BLE round trip, which has not happened yet (physical
Android hardware verification is planned for after the next minor
patch/release, owner-driven). What emulator testing this session *did*
establish, individually and with concrete evidence: the JNI plumbing itself
works with no crashes, a `capabilities()` call genuinely reaches real
Android `BluetoothAdapter` APIs and back (`{"central": true, "peripheral":
true}`, not the `false`/`false` this ADR originally and wrongly reported —
see below), and `advertise()` genuinely starts a real GATT server
(`AdvertiseCallback.onStartSuccess` confirmed). None of that adds up to "the
bridge works" — only real hardware, both roles, exchanging real data, does.
Treat everything below as build-confidence evidence, not a substitute for
that test.

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

### `ClassNotFoundException`: `FindClass` needs the app's classloader, not the bootstrap one

The first real device run *did* complete without crashing — but every
`capabilities()` call returned `{"central": false, "peripheral": false}`.
Root cause was hidden by `android_lazy::LazyAndroidBackend` correctly
catching the construction error and falling back to
`CapabilityReport::default()` (both fields `false`) — meaning that first
`false`/`false` result was **never a real capability query**, it was a
silently-swallowed construction failure. Made visible by adding an
`eprintln!` on the error path (now permanent — see `android_lazy.rs`),
which surfaced:

```
BLE adapter unavailable: BleGattBridge construction failed: Java exception was thrown
```

jni-rs clears the pending exception without describing it, so a second fix
was needed just to see *which* exception: `describe_pending_exception()` in
`android.rs` calls `ExceptionDescribe` (prints the full stack trace to
logcat) and reads the throwable's own message before clearing it. That
revealed the real cause:

```
java.lang.ClassNotFoundException: Didn't find class "dev.blegatt.BleGattBridge"
  on path: DexPathList[[directory "."],...]
```

This is a classic, well-documented Android NDK/JNI gotcha: `FindClass`
(used implicitly by `JNIEnv::new_object` when given a class name string)
only searches the **bootstrap classloader** when called from a thread the
JVM did not create itself — exactly this thread, attached via
`attach_current_thread_as_daemon` rather than JVM-spawned. The bootstrap
classloader only knows core Android framework classes, never app-defined
ones. Fixed with the standard workaround: `load_app_class()` in
`android.rs` resolves the class through the app's own classloader instead
(`context.getClass().getClassLoader().loadClass("dev.blegatt.BleGattBridge")`),
obtained from the `Context` object already on hand. After this fix,
`capabilities()` genuinely returns `{"central": true, "peripheral": true}`
on the same emulator — confirming the earlier `false`/`false` was 100% this
bug, not an AVD hardware limitation as originally (wrongly) concluded here.

**Lesson for any future JNI call site in this file**: never assume a `Err`
from a jni-rs call means what its message says at face value — jni-rs's
generic "Java exception was thrown" hides the actual cause every time
without `describe_pending_exception()`.

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
- `capabilities()` genuinely round-trips the entire chain — JS `invoke()` →
  Tauri IPC → `LazyAndroidBackend` → lazy `tao`→`ndk-context` bridge → JNI
  attach → app-classloader class resolution → `BleGattBridge` construction
  → real `BluetoothManager`/`BluetoothAdapter` queries → JNI return → back
  to the UI — and correctly reports `{"central": true, "peripheral":
  true}`.
- `advertise()` genuinely starts a real GATT server + BLE advertisement:
  `AdvertiseCallback.onStartSuccess` fires (confirmed via logcat, not
  assumed), `dumpsys bluetooth_manager` shows `Le advert started` at the
  HCI level.
- `scan()`'s registration path is genuinely real too: `BluetoothLeScanner`
  registers successfully (`onScannerRegistered status=0`), scan parameters
  are accepted (`onScanParamSetupCompleted: 0`) — confirmed via logcat on a
  clean run (app data cleared, 40s wait to rule out Android's undocumented
  per-app scan-throttling window, single scan attempt).

**Explicitly deferred, with a narrower and more specific reason than before:**
a genuine two-device BLE round trip is not verified. Two real emulator
instances (`fini-e2e` API 34 and a second `google_apis` x86_64 AVD),
confirmed connected to the *same* `netsimd` process (sequential virtual
addresses `BB:BB:BB:00:00:02`/`...03`, assigned by one shared daemon — the
radio-bridging infrastructure is genuinely there) — one advertising
(confirmed `onStartSuccess`), the other scanning unfiltered (confirmed
clean registration) — delivered **zero** `onScanResult` callbacks in either
direction, filtered or not. Historical `dumpsys bluetooth_manager` HCI-level
logs did show non-zero scan `results:N` counts on the scanning instance,
but those are aggregate controller-level counts across every scan client on
the device (including Android's own background service scanning), not
proof specifically of receiving the peer's advertisement — a red herring
chased down and ruled out this session, not left as an open assumption.
Everything on the Android-API side of the bridge is confirmed working
correctly on both ends; what's unconfirmed is whether Root Canal actually
propagates advertisement PDUs between two independently-launched emulator
processes by default, or needs explicit topology/positioning configuration
(the emulator's Netsim tooling has this concept) that wasn't identified in
the time available this session. This is a narrower, better-diagnosed gap
than the original version of this ADR claimed — not the same unknown.

Further emulator-side investigation was deliberately not pursued past this
point: `netsimd`'s device-topology API is gRPC-only and undocumented
(confirmed — a plain HTTP request to its frontend port just hangs), and
chasing it further is open-ended with no guaranteed payoff. **Physical
Android hardware is the actual verification bar for this bridge** and is
planned for after the next minor patch/release (owner-driven, not scheduled
here). Nothing above should be read as "the bridge works" — it's evidence
the pieces are individually sound, not proof of the thing that matters.

## Consequences

- A future session picking up the two-device verification should start from
  Android's Netsim/Root Canal topology configuration (device positioning,
  whether separately-launched emulator processes need explicit pairing to
  be "in range" of each other), not from assuming the bridge or Kotlin GATT
  code needs changes — both are now confirmed correct on each side
  independently.
- Any future JNI call site added to `android.rs` should route errors
  through `describe_pending_exception()`, and any new `Backend`-trait error
  path (in `ble-gatt` or a wrapper like `android_lazy.rs`) should log before
  falling back to a default value — the original false-negative here
  existed specifically because an error was silently absorbed into a
  same-shaped-as-real value (`CapabilityReport::default()` is
  indistinguishable from "genuinely no BLE support" at the call site).
- `examples/tauri-app` is now a real, working verification harness with
  interactive advertise/scan/connect/read test buttons — reusable for the
  eventual macOS/iOS/Windows backends too, not Android-specific.
- The `tao` → `ndk-context` bridge is a hard dependency of
  `tauri-plugin-ble-gatt` on Android (new `tao`/`ndk-context` target
  dependencies in its `Cargo.toml`) but changes nothing about `ble-gatt`
  itself — a non-Tauri Android consumer never needs `tao`.
