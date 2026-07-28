# 0001 — Repo/crate split, GATT-only scope, and the Backend port

## Status

Accepted. Implemented for the Linux backend (M1, first half). Android (M1,
second half) is in progress; Windows (M2) and macOS/iOS (M3) are reserved,
unimplemented `Backend` targets.

## Context

This repo originates from Fini issue #25 ("Add Bluetooth transport for
DeviceConnection and SpaceSync"). Fini's own transport-neutral peer-protocol
work (see Fini's `docs/adr/0001-transport-neutral-peer-protocol.md`) left a
`Bluetooth` transport as a deliberately deferred stacked follow-up, gated by
a fail-closed `BLUETOOTH_ADAPTER_IMPLEMENTED = false` stub.

During planning for that follow-up, the scope was widened: the owner also
maintains `keepsmile-lamp`, an unrelated Rust CLI that talks to a BLE lamp
over BlueZ, and asked whether the two could share a BLE library rather than
Fini growing a Fini-only Bluetooth module. That reframed the work from "add
a Bluetooth code path to Fini" into "build a standalone, reusable BLE
library, then consume it from Fini."

Library research (checked directly against GitHub/crates.io, not from
memory) found no existing Rust crate covering everything needed:

| Crate | Central | Peripheral | Android | License | Maturity |
|---|---|---|---|---|---|
| `btleplug` / `tauri-plugin-blec` | Yes | **No** (host-side only, by design) | Yes | Apache-2.0 | Active, healthy |
| `bluest` | Yes | **No** (explicit in README) | Not yet ("planned") | Apache-2.0 | Active |
| `ble-peripheral-rust` | — | Yes (by design) | **No** Android backend in source tree | MIT | Reasonably active |
| `blew` / `tauri-plugin-blew` | Yes | Yes | Yes | **AGPL-3.0** | Author-labeled "experimental"; ~3 months stale |

No permissively licensed crate does BLE peripheral (GATT server) mode on
Android. `blew` is the one crate that does everything, but its license is a
hard blocker (see "No AGPL" below) and its own author warns not to rely on
it yet. This repo exists to close that gap, hand-written where necessary.

## Decision

### Repo and crate naming: `ble-gatt`, not `tauri-plugin-ble-gatt`

The repo (and the core crate inside it) is named **`ble-gatt`**, deliberately
without "tauri" in the name — the owner does not want the reusable core
associated with Tauri branding, since `keepsmile-lamp` and any other future
consumer have nothing to do with Tauri.

The **wrapper crate** inside the workspace is still named
`tauri-plugin-ble-gatt`, keeping the `tauri-plugin-*` prefix. This is not
cosmetic: Tauri's own mobile-plugin build tooling (the `tauri-plugin` build-
dependency, permission-schema generation, and eventually the Android/iOS
Gradle/Xcode project conventions) keys off that exact crate-name prefix. Only
the repo and the protocol-agnostic core dropped the Tauri branding; the thin
wrapper keeps it because Tauri's tooling requires it.

### Two-crate workspace, not one

- **`ble-gatt`** — the core library. No Tauri dependency anywhere in its
  dependency graph. Async/Tokio-required (matches `bluer`, which is
  async-only). This is what `keepsmile-lamp` or any other Tokio-based
  consumer would depend on directly.
- **`tauri-plugin-ble-gatt`** — depends on `ble-gatt`, adds only
  `tauri::plugin::Builder` registration, the `#[tauri::command]` IPC
  surface, and (once Stage 2 lands) the Android Kotlin plugin scaffolding.

Splitting them was the entire point of widening this from a Fini-only task —
a single crate with an optional `tauri` feature flag was considered and
rejected: Tauri's mobile-plugin conventions (permission manifests, the
`android/`/`ios/` folder layout, the `tauri-plugin` build-dependency) are
substantial enough that hiding them behind a feature flag on the reusable
core would leak Tauri concerns into `ble-gatt`'s public API surface and
build graph even when the feature is off.

### GATT (central + peripheral), not RFCOMM or L2CAP CoC

BLE GATT was chosen over classic Bluetooth RFCOMM (a lower-risk,
socket-stream alternative that was explicitly on the table) and BLE L2CAP
Connection-oriented Channels (needs Android API 29+; Fini's `minSdk` is 24).
GATT was chosen specifically to support **both central and peripheral
(GATT-server) role**, not just central — most existing crates only do
central, which is exactly the gap this repo exists to close. The one-radio
constraint of peripheral mode (see "What was deliberately not built" below)
was accepted knowingly, not overlooked.

### The `Backend` port: generic GATT primitives, not an opaque byte pipe

`Backend`/`GattConnection` (`ble-gatt/src/backend/mod.rs`) expose scan,
advertise, GATT client read/write/subscribe, and GATT server
characteristics/notify as their own typed operations — not a single
`send(Vec<u8>)`/`recv() -> Vec<u8>` byte pipe (which is what Fini's own
`Transport`/`Link` port looks like, deliberately, per Fini's ADR-0001).

This is the opposite tradeoff from Fini's transport port, and deliberately
so: Fini's `Link` needs to be protocol-agnostic across WS/BLE/LoRa datagrams
carrying an opaque `PeerFrame`. `ble-gatt` has exactly one protocol (GATT)
and needs to stay useful to consumers, like `keepsmile-lamp`, that have no
`PeerFrame` at all and just want to read/write a lamp's actual
characteristics. Fini's eventual `transport::ble.rs` (Stage 3, not yet
built) is expected to be a thin adapter that defines its own
service/characteristic UUIDs and chunks `PeerFrame` bytes across them using
this crate's generic read/write/notify — i.e. Fini's byte-pipe abstraction
sits *on top of* `ble-gatt`, not inside it.

### Honest, runtime-discovered capability, never assumed from the OS

`Backend::capabilities() -> CapabilityReport { central, peripheral }` is
queried at runtime (on Linux: `Adapter::supported_advertising_capabilities()`
against the live BlueZ daemon), not hardcoded per target OS. An Android
device whose chipset/driver can't do peripheral mode reports that honestly
through this call rather than failing opaquely the first time a consumer
tries to advertise. This mirrors Fini's own established pattern of failing
closed and explicitly (`BLUETOOTH_ADAPTER_IMPLEMENTED`,
`bluetooth_address_is_os_paired` returning `false` rather than guessing) —
consistency with that convention was a deliberate choice, since Fini is this
crate's first real consumer.

Deterministic role assignment itself (e.g. Fini's planned "lower
`device_id` = central" rule, with a fallback swap when the assigned
peripheral side reports `peripheral: false`) is **not** implemented in this
crate — it is a policy decision for the caller. `ble-gatt` only supplies the
`Role` enum and the honest `CapabilityReport` the caller needs to make that
call.

### `MockBackend`: in-process, radio-free, built on `tokio::sync::broadcast`

`backend/mock.rs` mirrors Fini's `transport::sim` adapter: a first-class
`Backend` implementation, not a test double that fakes the trait's shape.
Two `MockBackend`s sharing a `MockNetwork` (explicit `Arc`, constructed per
test — no global registry, so unrelated tests never interfere) exercise the
exact same `Backend`/`GattConnection` contract a real backend does: scan,
connect, read/write, server-initiated notify. Built entirely on
`tokio::sync::broadcast` for event fan-out and notification delivery, not
hand-rolled pub-sub plumbing — consistent with the general principle applied
across this repo of reusing established, widely-used crates (`bluer`,
`tokio`, `thiserror`, `async-trait`) over writing new primitives.

This is what lets `cargo test --workspace` prove the protocol contract
(discovery, read, write-then-read-back plus the `CharacteristicWritten`
event, server-initiated notify, connect-to-absent-peer failure,
capability-gated advertise rejection) on any CI runner, with no Bluetooth
hardware required.

### No AGPL dependency, anywhere

Verified directly (not assumed) that no mature, permissively licensed Rust
crate supports BLE peripheral mode on Android — see the table above. `blew`
is the sole exception and is AGPL-3.0, which is incompatible with this
project's MIT license and with Fini being closed/permissively licensed
downstream. The Android backend (Stage 2) will be hand-written via raw
`jni` + `ndk-context`, with `blew`'s repo *structure* studied as a pattern
reference only — never its code.

## What was deliberately not built (Stage 1)

- **True two-adapter loopback verification.** `ble-gatt/tests/linux_loopback.rs`
  contains a `#[ignore]`d `two_adapter_central_to_peripheral_round_trip`
  test that is not yet implemented, because the development machine has a
  single physical Bluetooth adapter and a single radio cannot connect to
  itself as both central and peripheral over the air. What *is* verified on
  real hardware is `peripheral_advertise_and_serve_smoke_test`: it proves
  the `Application`/`Advertisement` D-Bus registration this crate builds is
  accepted by the live `org.bluez` daemon, which is real evidence the
  peripheral-role code is correct, just not evidence of an actual two-device
  connection. A second adapter (USB dongle) or a second machine is needed to
  close this gap — left as a documented follow-up, not faked with a
  same-adapter test.
- **Precise device-level connection lifecycle on the Linux peripheral side.**
  `GattEvent::Connected`/`Disconnected` on `LinuxBackend` are derived from
  GATT activity (first characteristic write from a device address; a notify
  session stopping), not from BlueZ's own `Device.Connected` D-Bus property
  change, which would need an independently driven `Device::events()`
  watcher per connected peer. Acceptable for now because no current consumer
  needs precise link-level connect/disconnect timing; revisit if one does.
- **Continuous scan / subscribe as Tauri events.** `tauri-plugin-ble-gatt`'s
  command surface is request/response — `ble_scan_once` collects a snapshot
  over a caller-given timeout rather than streaming discoveries, and there
  is no event-emission bridge for `GattConnection::subscribe` yet. Deferred
  until Fini's Stage 3 integration defines the exact shape it needs, rather
  than guessing at an IPC event contract with no real consumer yet.
- **`examples/tauri-app/`.** Not scaffolded — parked until there's a real
  cross-platform scenario (Stage 2/3) to demonstrate end-to-end.

## Consequences

- Adding a platform means implementing one `Backend` (Windows via WinRT for
  M2, macOS/iOS via CoreBluetooth for M3) — no change to the port shape,
  the mock backend, or any existing backend.
- `keepsmile-lamp` adopting `ble-gatt` is possible (Linux backend, central
  role only, is all it would need) but requires it to add a Tokio runtime,
  which it doesn't currently have. Not required by this work; the crate is
  just structured so that adoption doesn't require an `ble-gatt` redesign
  later.
- Fini's Stage 3 integration (`src-tauri/src/services/transport/ble.rs`) is
  responsible for: defining Fini's own GATT service/characteristic UUID
  scheme, chunking/reassembling `PeerFrame` bytes across this crate's
  generic characteristic read/write/notify, implementing the actual
  deterministic role-assignment-with-capability-fallback policy using
  `CapabilityReport`, and replacing the existing fail-closed
  `BLUETOOTH_ADAPTER_IMPLEMENTED` stub. None of that exists yet.
