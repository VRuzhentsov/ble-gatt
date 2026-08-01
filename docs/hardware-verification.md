# Hardware verification

Everything else in this repository is verified against a mock backend. That
mock has repeatedly certified behaviour both real backends reject — writes to
characteristics no peer declared writable, operations through connections
that had already been closed, and for several revisions a `session: None`
that made an entire class of session checks unreachable from any test. None
of the library's behaviour is confirmed until two radios exchange bytes.

This document is how that is done.

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

No physical hardware. Exercises the Android backend, the Kotlin bridge and
the JNI boundary on both ends — where the majority of review findings landed.

The emulator log line to look for at startup is:

```
INFO | Activated packet streamer for bluetooth emulation
```

That is RootCanal, the virtual Bluetooth controller. Without it the emulator
has no Bluetooth at all and every run will fail at `capabilities`.

```bash
# two instances, different ports; -no-snapshot so state is clean each run
emulator -avd ble-test-2 -no-window -no-audio -no-boot-anim -no-snapshot -port 5554 &
emulator -avd fini-e2e   -no-window -no-audio -no-boot-anim -no-snapshot -port 5556 &

# install on both
adb -s emulator-5554 install -r <apk>
adb -s emulator-5556 install -r <apk>

# assign roles, then launch
adb -s emulator-5554 shell setprop debug.blegatt.role peripheral
adb -s emulator-5556 shell setprop debug.blegatt.role central
```

Start the **peripheral first** and wait for its `READY` marker before
launching the central, or the central's scan window can open and close
before anything is advertising.

Known limitation, recorded in `docs/adr/0002-android-jni-bridge.md`: a
*filtered* scan against RootCanal returned zero results even with the target
confirmed advertising and the filter accepted (`status=0`). The Android
backend therefore scans unfiltered and matches in code. A filtered-scan
regression will not be caught by this topology.

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
