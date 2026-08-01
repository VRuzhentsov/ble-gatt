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

The remaining route to a round trip is Linux ↔ Android or Linux ↔ Linux,
both of which need real radios.

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
