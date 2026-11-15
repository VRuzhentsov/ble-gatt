# ble-gatt

Async, transport-agnostic BLE GATT primitives for Rust — **central and
peripheral role**, no Tauri dependency in the core crate. Built on top of
established, widely-used crates rather than reinventing plumbing:
[`bluer`](https://github.com/bluez/bluer) for BlueZ, [`tokio`](https://github.com/tokio-rs/tokio)
for async/channels, [`thiserror`](https://github.com/dtolnay/thiserror) and
[`async-trait`](https://github.com/dtolnay/async-trait) for the trait
surface.

This repo is a Cargo workspace with three crates:

- **[`ble-gatt`](ble-gatt/README.md)** — the core library. Scan/advertise, GATT client
  read/write/subscribe, GATT server characteristics, connection lifecycle as
  an event stream, and a radio-free `MockDriver` for CI-safe protocol
  tests. Depends on nothing Tauri-specific — usable from any Tokio-based
  Rust program (a CLI, a daemon, another GUI framework).
- **[`tauri-plugin-ble-gatt`](tauri-plugin-ble-gatt/README.md)** — the [Tauri](https://tauri.app) integration:
  ships the Android Kotlin bridge (an application never copies it), builds
  the platform driver on first use, asks for Android's Bluetooth runtime
  permissions through Tauri's permission mechanism, and exposes both a Rust
  API and JavaScript commands.
- **[`ble-gatt-iroh`](ble-gatt-iroh/README.md)** — an [iroh](https://github.com/n0-computer/iroh)
  custom transport that carries QUIC over `ble-gatt`'s GATT datagram
  channel. In the current implementation, the caller schedules scanning and
  advertising; the adapter also contains initiation policies described in
  [ADR-0013](docs/adr/0013-symmetric-iroh-initiation.md).

## Provisional architecture agreement

The target in [ADR-0014](docs/adr/0014-core-peer-ownership.md) makes symmetric
peer communication a core `ble-gatt` capability, independent of iroh. Either
eligible peer can initiate or reconnect; applications do not select an
`EitherPeer`, previous-dialler or identity-ordered mode. Raw central/peripheral
operations remain available for vendor protocols.

`ble-gatt-iroh` adapts generic BLE messages to iroh packets and accounts for
iroh users of a carrier. The Tauri plugin supplies host access and setup; it
is not another packet layer. iroh itself remains an external library.

**This is the agreed direction, not an implemented refactor.** The current
`PeerLink::Symmetric` and adapter initiation APIs do not yet provide that
single core-owned contract. See the [architecture map](docs/architecture.md)
and each crate's README for current behavior, target ownership and open questions.

### Using the plugin from a Rust application

```rust
use tauri_plugin_ble_gatt::BleGattExt;

tauri::Builder::default()
    .plugin(tauri_plugin_ble_gatt::init())
    // or, to supply a driver yourself (a mock radio in tests):
    // .plugin(tauri_plugin_ble_gatt::Builder::new().driver(driver).build())
    ;

// From async code:
let adapter = app.ble_gatt().adapter().await; // ble_gatt::Adapter

// From a blocking task, never the main thread:
let state = app.ble_gatt().request_permissions()?; // PermissionState::Granted, ...
```

On Android, plugin registration captures an owned Application context through
JNI. The default driver never reads or initializes `ndk-context`, so it works
with both older Tauri runtimes and newer runtimes that already own that global,
without matching the app's Tao version or retaining an Activity.

Applications can call `tauri_plugin_ble_gatt::android_context::get()` after
plugin registration for their own JNI: it returns a JVM handle and an owned
Application reference. The legacy `android_context::ensure()` remains available
for applications on older runtimes which explicitly delegate `ndk-context`
initialization to this plugin (such as existing Fini consumers). Do not call
that legacy initializer on a runtime that already initializes `ndk-context`;
`ndk-context` 0.1 has no non-panicking probe for existing ownership. Use `get()`
for new code instead.

This library **carries bytes; it does not encrypt them.** Layering your own
session protocol or end-to-end encryption on top is expected and
supported — and staying out of the way is deliberate, so that consumers
talking to third-party device firmware (which will never speak your
protocol) can use the raw GATT API directly. See ADR-0003.

The layer map (what each layer and module is responsible for, and how
platform modules are designed) is in
[`docs/architecture.md`](docs/architecture.md).

Other BLE libraries, in every language, and what this repo borrows from them:
[docs/alternatives.md](docs/alternatives.md).

Design decisions live in `docs/adr/`:
[0001](docs/adr/0001-ble-gatt-tauri-plugin-split-and-scope.md) — why this
split, why GATT-only, why hand-rolled instead of an existing crate.
[0002](docs/adr/0002-android-jni-bridge.md) — the Android JNI bridge, and
exactly what is and isn't verified about it.
[0003](docs/adr/0003-carrier-not-crypto-and-the-two-api-tiers.md) — carrier
vs. crypto, the two API tiers, and the no-AGPL constraint.
[0004](docs/adr/0004-mock-broker-for-cross-process-e2e.md) — the optional
`mock-broker` feature letting `drivers::mock` bridge two real OS processes
over a socket instead of sharing an in-process `Arc`.

## Platform support

| Platform | Central | Peripheral | Status |
|---|---|---|---|
| Linux (BlueZ) | Yes | Yes | **M1 — implemented**, verified against real BlueZ hardware |
| Android | Yes | Yes (where the chipset/driver allows — see `CapabilityReport`) | **M1 — implemented.** Source checks, packaged JNI execution and physical device acceptance are separate evidence levels. See ADR-0002 and the hardware verification guides; no blanket crossed-role or locked-device guarantee is implied. |
| Windows (WinRT) | Yes | Yes (where the adapter and driver allow — see `CapabilityReport`) | **M2 — first implementation, not yet verified on a radio.** Builds for `x86_64-pc-windows-msvc`; needs Windows 10 2004 or later. Scanners find it through the GATT advertisement (ADR-0012). See ADR-0011. |
| macOS / iOS | — | — | M3 — reserved, not implemented |

Capability is always discovered at runtime via `Driver::capabilities()`,
never assumed from the target OS — an Android device whose driver can't do
peripheral mode reports that honestly instead of failing opaquely later.

## Architecture

```
ble-gatt/src/
├── entities/     Entities: types, events, errors
├── hal/          Use Cases: the Driver / GattConnection ports (async, Tokio)
├── roles/        Use Cases: Adapter, Central and Peripheral
├── connection/   Use Cases: one connection's state machine, PeerLink
├── profile/      Use Cases: the datagram profile (fragmentation, reassembly)
├── power/        Use Cases: power-profile advice
└── drivers/      Frameworks & Drivers: one Driver per platform
    ├── linux.rs    BlueZ via bluer — central + peripheral       (M1)
    ├── android.rs  JNI driver with explicit context support    (M1)
    ├── windows/    WinRT; central adapted from btleplug, own peripheral (M2)
    └── mock/       in-process, radio-free — CI-safe protocol tests;
                    optionally a cross-process broker behind the
                    `mock-broker` feature — see ADR-0004
```

### Roles, published state, handles

```rust
use ble_gatt::{Adapter, RadioStatus};

let adapter = Adapter::platform().await?;      // or Adapter::new(driver) to inject one
adapter.wait_available().await;                 // adapter.status(): watch::Receiver<RadioStatus>

let server = adapter.peripheral().serve(spec).await?; // advertising stops when `server` drops

let mut conn = adapter.central().connect(&peer).await?;
let mut state = conn.state();                   // watch::Receiver<ConnectionState>
tokio::spawn(conn.cancelled().run_until_cancelled_owned(read_loop()));
state.wait_for(|s| s.is_disconnected()).await?;
```

### Power profiles

`power::PowerProfiles` turns the device's situation (foreground, battery,
charging, peers nearby) into advice: scan on/off times, a connection limit
and a connection priority, published on a `watch` channel. It never touches
the radio; the application applies it (ADR-0007 D8).

Layers are named after Clean Architecture and modules after the Bluetooth
specification (ADR-0007). The short paths `ble_gatt::datagram` and
`ble_gatt::peer_link` still resolve; `ble_gatt::backend::…` is gone (use
`ble_gatt::drivers` and `ble_gatt::hal`). Application code takes an
`Adapter` (`datagram::connect`/`serve`, `ble_gatt_iroh::dial_with`);
`Driver` is the driver port underneath it.

Every driver speaks the same generic GATT vocabulary
(`ServiceUuid`/`CharacteristicUuid`/`GattEvent`/`GattServiceSpec`/...) —
callers never see platform types (no `bluer::Device`, no JNI handles)
crossing the `Driver`/`GattConnection` port boundary.

### Three tiers, one carrier

`ble-gatt` carries bytes; it does not interpret them (ADR-0003). A consumer
picks the tier that fits:

| Tier | Use for | Owns the connection? |
|---|---|---|
| **raw GATT** (`Driver`) | a vendor device with its own protocol | you |
| **datagram** (`datagram::connect` / `serve`) | one message pipe, driven by hand | you |
| **`PeerLink`** | app-to-app links to a set of peers | the library |

These are current APIs, not three target peer-initiation behaviors. See
[`docs/interface.md`](docs/interface.md) for their current use and
[ADR-0014](docs/adr/0014-core-peer-ownership.md) for the pending ownership change.

### Load-bearing files

Read the doc next to each before changing it:

| File | What it governs | Doc |
|---|---|---|
| `ble-gatt/src/connection/link_state.rs` | `RadioState` / `CentralLink` / `PeripheralLink` — pure lifecycle transitions | ADR-0005 |
| `ble-gatt/src/connection/peer_link.rs` | current tracked-peer API and dedicated worker | `docs/interface.md`, ADR-0005 |
| `ble-gatt/src/drivers/linux.rs` — `LinuxConnectGuard` / `pending_cleanup` | abandoned-connect cleanup and native ownership | code comments, ADR-0009 |
| `ble-gatt/src/profile/datagram/mod.rs` | complete messages, serving and channel limits | ADR-0003, ADR-0013 |

## Status

Early, under active development. Implemented APIs, the provisional architecture
and device acceptance are different things. The architecture documentation
does not certify all drivers, native roles or background scenarios. See
[hardware verification](docs/hardware-verification.md) and the
[iroh diagnostic checklist](docs/either-peer-hardware.md).
Not yet published to crates.io/npm — consume as a git dependency, pinned to a
release tag:

```toml
ble-gatt = { git = "https://github.com/VRuzhentsov/ble-gatt", tag = "v0.1.0" }
```

## Releasing

1. In a pull request, set `[workspace.package] version` in `Cargo.toml` to
   the new version, and merge it.
2. Actions → **Release Button** → Run workflow, on `main`.

The button refuses a version that is already tagged, and refuses if main
moves while it runs. It runs CI on main's last commit, tags that commit
`vX.Y.Z` (annotated, unsigned) and publishes a GitHub release with generated
notes.

## License

MIT
