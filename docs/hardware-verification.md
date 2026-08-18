# Hardware verification

Everything else in this repository is verified against a mock backend. That
mock has repeatedly certified behaviour both real backends reject — writes to
characteristics no peer declared writable, operations through connections
that had already been closed, and for several revisions a `session: None`
that made an entire class of session checks unreachable from any test. None
of the library's behaviour is confirmed until two radios exchange bytes.

This document is how that is done.

## Status

**No two-device round trip has passed yet.** What is confirmed so far:

| Claim | Evidence |
| --- | --- |
| Linux peripheral genuinely on the air | `LEAdvertisingManager1.ActiveInstances = 1` |
| Android peripheral role end to end | `HWVERIFY: READY peripheral` + `onAdvertisingSetStarted … status=0` |
| Android↔Android discovery | **Blocked** — platform gap, see below |
| Android central → Linux peripheral: connect, MTU, service discovery, subscribe | All succeed — see "First live attempt", below |
| Android central → Linux peripheral: the auth write | **Blocked** — two independent causes found, neither is the platform gap above |
| Android peripheral role (advertise) against a real release build | **Blocked** — `NoSuchMethodError`, see below |

The remaining route to a round trip is Linux ↔ Android or Linux ↔ Linux,
both of which need real radios.

### First live attempt: real fini release build ↔ real Android phone (2026-08-17)

Not the `hw_peer`/datagram-tier harness below — this was fini's actual
production app (`v0.1.45`, pinning this repo at `af4ed1e`) against a real
Pixel 6 Pro, both already paired, diagnosed read-only over `adb logcat` +
`journalctl` + a user-run `btmon` capture (no reinstall, no rebuild). Two
independent defects, plus a third contributing factor, all found from the
same session:

**1. Android peripheral role: `startAdvertising` `NoSuchMethodError`, every
retry, forever.**

```
System.err: java.lang.NoSuchMethodError: no non-static method
  "Ldev/blegatt/BleGattBridge;.startAdvertising(Ljava/lang/String;[Ljava/lang/String;[Z[Z[Z[[B[I[[B[Ljava/lang/String;[[B)V"
[transport][ble] advertise failed, retrying in 60s: ...
```

Current source (`BleGattBridge.kt:623`) has a matching 10-arg
`startAdvertising` and the JNI call site (`android.rs:701`) matches it
exactly — so this isn't a source-level mismatch today. Leading hypothesis:
build/version skew between the Rust JNI signature and the Kotlin bytecode
actually baked into that specific release APK. The `manufacturer_data`/
`service_data` fields (this branch, `feat/advertise-manufacturer-service-
data`) are a recent addition to that exact signature — a release built
before the two sides were rebuilt from the same rev would reproduce exactly
this. Worth confirming against the actual `v0.1.45` build provenance before
assuming this is the whole story; an R8/consumer-rules gap (see below) is a
second plausible contributor and hasn't been ruled out.

The plugin module ships no `consumer-rules.pro` / `consumerProguardFiles`
(`tauri-plugin-ble-gatt/android/build.gradle.kts` has neither), so a
consuming app's release R8 pass has nothing telling it `BleGattBridge`'s
JNI-invoked methods are reachable. Whether or not it's the cause of this
specific error, it's a live gap: add a consumer ProGuard rule keeping
`dev.blegatt.**` (or at least the methods called by name from
`android.rs`) before shipping another release.

**2. Android central role: connects, negotiates MTU, discovers services,
subscribes — then the very first characteristic write is rejected locally,
never reaching the air.**

```
D BleGattBridge: onMtuChanged: 88:D8:2E:BA:72:27 negotiated mtu=517
W BleGattBridge: writeCharacteristic rejected by the stack: 88:D8:2E:BA:72:27/b1e6a001-...
[transport][ble] auth with ... failed: GATT operation failed: characteristic write failed
```

A `btmon` capture across this exact window is conclusive: BlueZ correctly
declares the characteristic as `Properties: 0x1e` (Read | Write | Write
Without Response | Notify) — service discovery reads this straight off the
air — and the CCCD subscribe write completes cleanly (`ATT Write Request` →
`Write Response`, sub-millisecond). No `ATT Write Request` for the
characteristic itself ever appears on the air; the link goes quiet and
times out (`Disconnect Complete … Reason: Connection Timeout`) about 5s
later. So this rules out a declared-property mismatch (the mock's own
blind spot per this doc's opening paragraph) — `gatt.writeCharacteristic()`
is refusing locally, before attempting anything over the air.

**3. Contributing factor: classic-profile radio contention can turn "rejected
by the stack" from rare into routine.**

The same capture shows normal ATT round trips (30-90ms) degrading to
**3.7-3.8 second** per-packet latencies during service discovery, correlated
with `bluetoothd`'s `policy.c:reconnect_timeout()` repeatedly retrying a
*classic* (BR/EDR) Audio Source/Handsfree profile connection to the same
phone address — bonded for an entirely unrelated reason (using the phone as
a Bluetooth audio device with this machine). Each retry re-runs Inquiry/Page
scanning, which shares the same radio as the BLE link. This is not
something fini can prevent — pairing a phone for audio with the same
machine it also uses fini on is an ordinary, unpreventable user scenario,
not a lab artifact.

Android's local single-outstanding-GATT-operation rule (a second op called
before the previous op's callback lands returns `false` immediately) is the
standard explanation for exactly this failure shape, and radio contention
that stretches every op's air time by 100x is exactly what turns a rare
race into a reliable repro. This doesn't need to be the *only* explanation
for defect 2 above, but it's a real, evidenced amplifier and worth fixing
regardless of whether it's the sole cause.

**Proposed fix (not yet implemented):** `BleGattBridge`'s GATT operations
(write, and likely read/subscribe — same failure shape) treat a `false`
return from the underlying `BluetoothGatt` call as terminal today, failing
the whole connection on the very first rejection. Android's own guidance
(and every mature Android BLE library) treats "stack busy" as expected and
transient, not an error — the standard mitigation is a small bounded retry
with backoff (e.g. 3 attempts, ~150-300ms apart) before giving up. Adding
that at the point of rejection would absorb both transient scheduling
jitter and exactly this kind of external radio contention, at a fraction of
the cost of the current behaviour (tear down the whole link, wait for the
outer ~35-45s reconnect loop). Needs a design pass before implementing:
where the retry lives (Kotlin-side in `BleGattBridge`, closest to the
platform quirk, vs. Rust-side in `android.rs`), how many attempts, whether
it's shared plumbing across write/read/subscribe, and how a still-failing
retry should surface (today's `BleError::Gatt` path is probably still
right, just reached later).

## The harness

Two halves that speak the same protocol, use the same UUIDs and the same
probe payload, and emit the same `HWVERIFY:` markers:

| Half | Where | Driven by |
| --- | --- | --- |
| Linux | `ble-gatt/examples/hw_peer.rs` | `--role peripheral\|central`, exits 0/1 |
| Android | `examples/tauri-app` (`src-tauri/src/hw_verify.rs`) | `debug.blegatt.role` system property, logs to logcat |

Both run the *datagram* tier rather than raw GATT, so a pass exercises
advertising, scanning, connection, subscription, MTU negotiation,
fragmentation and reassembly together. The probe payload is 512 bytes —
deliberately larger than one fragment at any plausible MTU, so a passing run
proves fragmentation over the air rather than a single-write happy path.

The peripheral **echoes** what it receives rather than merely accepting it.
That is deliberate: echoing exercises the peripheral notify path, which is
where most of this library's review findings landed and which no mock test
can confirm.

Markers:

- `HWVERIFY: READY <role>` — that half is up and waiting
- `HWVERIFY: INFO …` — progress, including negotiated `max_message_len`
- `HWVERIFY: PASS round-trip complete`
- `HWVERIFY: FAIL <reason>`

## Topologies

### Two Android emulators (RootCanal)

> **This topology cannot complete a round trip.** LE discovery does not work
> between two emulators — see "Why discovery fails" below. It still proves
> the peripheral role, so it is documented rather than deleted.

No physical hardware. Exercises the Android backend, the Kotlin bridge and
the JNI boundary — where the majority of review findings landed.

The emulator log line to look for at startup is:

```
INFO | Activated packet streamer for bluetooth emulation
```

That is RootCanal, the virtual Bluetooth controller. Without it the emulator
has no Bluetooth at all and every run will fail at `capabilities`.

```bash
# two instances, different ports; -no-snapshot so state is clean each run.
# -gpu host is required: see "Emulator crashes on start" below.
emulator -avd ble-test-2 -gpu host -no-window -no-audio -no-boot-anim -no-snapshot -port 5554 &
emulator -avd fini-e2e   -gpu host -no-window -no-audio -no-boot-anim -no-snapshot -port 5556 &

# install on both. Debug builds get a `.debug` package suffix; the activity
# keeps the base name. Using the unsuffixed id makes `pm grant` silently
# grant nothing and `am start` fail with "Activity class does not exist".
for s in emulator-5554 emulator-5556; do
  adb -s $s install -r <apk>
  adb -s $s shell cmd bluetooth_manager enable
  for p in BLUETOOTH_SCAN BLUETOOTH_CONNECT BLUETOOTH_ADVERTISE; do
    adb -s $s shell pm grant dev.blegatt.example.debug android.permission.$p
  done
done

# assign roles, then launch
adb -s emulator-5554 shell setprop debug.blegatt.role peripheral
adb -s emulator-5556 shell setprop debug.blegatt.role central
adb -s emulator-5554 shell am start -n dev.blegatt.example.debug/dev.blegatt.example.MainActivity
```

Start the **peripheral first** and wait for its `READY` marker before
launching the central, or the central's scan window can open and close
before anything is advertising.

#### Emulator crashes on start

The emulator segfaults (exit 139) during boot under the default GPU mode.
The coredump backtrace puts frames 1–5 in `gles_swiftshader/libGLESv2.so`
with frame 0 at an unmapped JIT address — SwiftShader's LLVM JIT. `-gpu off`
does **not** avoid it, because SwiftShader is loaded regardless. `-gpu host`
uses the real DRM node (`/dev/dri/renderD128`) and avoids the JIT entirely.

#### Why discovery fails

The central's scan starts cleanly, reports no error, and returns nothing.
The cause is below this library, and the stack's own counters show it:

```bash
adb -s emulator-5556 shell dumpsys bluetooth_manager | grep -A 30 "GATT Scanner Map"
```

```
dev.blegatt.example.debug
  Scan time in ms (active/suspend/total) : 90036 / 0 / 90036
  Total number of results                : 0
com.google.uid.shared:10126 (Registered)
  Scan time in ms (active/suspend/total) : 1612286 / 0 / 1612286
  Total number of results                : 0
```

**Every** scanner on the device gets zero results, including Google Play
services' — not just ours. Meanwhile RootCanal is bridging correctly at the
controller level; its log shows the exchange in both directions:

```
le_controller.cc:2836  1  Sending LE Scan request to advertising address 4d:…
le_controller.cc:4303  0  Accepting LE Scan request to extended advertiser 0
le_controller.cc:4392  1  Accepting LE Scan response from advertising address 4d:…
```

So the advertisement reaches the scanner's controller and is answered, but
no advertising report is ever delivered up to the host. A plausible reading
is that RootCanal drives its advertiser through the extended state machine
while the emulated controller advertises `extended_scan_support: 0` (visible
in the same `dumpsys` output) — but that is a hypothesis about netsim, not a
finding about this library, and nothing here can work around it.

Related and recorded earlier in `docs/adr/0002-android-jni-bridge.md`: a
*filtered* scan against RootCanal also returned zero results with the target
confirmed advertising and the filter accepted (`status=0`). The Android
backend therefore scans unfiltered and matches in code. Both observations
now look like the same underlying gap.

#### What this topology does prove

The peripheral half runs end to end: `ndk_context` bridging, the classloader
lookup, the Kotlin bridge, the GATT server, and the async advertise
handshake, confirmed by `HWVERIFY: READY peripheral` and the stack's own
`onAdvertisingSetStarted … status=0`. Only discovery is blocked.

### Linux ↔ Android

Two real radios, and the cross-platform pair Fini's plan calls for. The
Linux half runs directly:

```bash
cargo run --example hw_peer -- --role peripheral
```

Confirm it is genuinely on the air rather than merely not erroring:

```bash
busctl get-property org.bluez /org/bluez/hci0 \
  org.bluez.LEAdvertisingManager1 ActiveInstances
# y 1   <- the controller has an active advertising instance
```

That check matters. `serve()` returning `Ok` only means BlueZ accepted the
registration; `ActiveInstances` is the controller confirming it.

### Linux ↔ Linux

Needs a second controller — a USB dongle or a second machine. This is what
`two_adapter_central_to_peripheral_round_trip` in
`ble-gatt/tests/linux_loopback.rs` is `#[ignore]`d for.

## What a pass does and does not prove

A pass proves the two backends interoperate for one connection, one
fragmented message and one echo. It does **not** prove the behaviour that
dominated this library's review findings: reconnection with the same
address, session identity across a stop/restart, cancellation mid-operation,
lock ordering under concurrent callbacks. Those need the churn scenarios —
connect/disconnect/reconnect loops, advertise/stop/advertise cycles — which
belong on top of this harness once a single round trip is reliable.
