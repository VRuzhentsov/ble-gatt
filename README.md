# ble-gatt

Async, transport-agnostic BLE GATT primitives for Rust — **central and
peripheral role**, no Tauri dependency in the core crate. Built on top of
established, widely-used crates rather than reinventing plumbing:
[`bluer`](https://github.com/bluez/bluer) for BlueZ, [`tokio`](https://github.com/tokio-rs/tokio)
for async/channels, [`thiserror`](https://github.com/dtolnay/thiserror) and
[`async-trait`](https://github.com/dtolnay/async-trait) for the trait
surface.

This repo is a Cargo workspace with three crates:

- **`ble-gatt`** — the core library. Scan/advertise, GATT client
  read/write/subscribe, GATT server characteristics, connection lifecycle as
  an event stream, and a radio-free `MockBackend` for CI-safe protocol
  tests. Depends on nothing Tauri-specific — usable from any Tokio-based
  Rust program (a CLI, a daemon, another GUI framework).
- **`tauri-plugin-ble-gatt`** — the [Tauri](https://tauri.app) integration:
  ships the Android Kotlin bridge (an application never copies it), builds
  the platform backend on first use, asks for Android's Bluetooth runtime
  permissions through Tauri's permission mechanism, and exposes both a Rust
  API and JavaScript commands.
- **`ble-gatt-iroh`** — an [iroh](https://github.com/n0-computer/iroh)
  custom transport that carries QUIC over `ble-gatt`'s GATT datagram
  channel. The caller decides when the radio scans and advertises. See
  ADR-0006.

### Using the plugin from a Rust application

```rust
use tauri_plugin_ble_gatt::BleGattExt;

tauri::Builder::default()
    .plugin(tauri_plugin_ble_gatt::init())
    // or, to supply a backend yourself (a mock radio in tests):
    // .plugin(tauri_plugin_ble_gatt::Builder::new().backend(backend).build())
    .setup(|app| {
        let backend = app.ble_gatt().backend(); // Arc<dyn ble_gatt::Backend>
        Ok(())
    });

// From a blocking task, never the main thread:
let state = app.ble_gatt().request_permissions()?; // PermissionState::Granted, ...
```

On Android, an application that also calls Java through `ndk-context`
calls `tauri_plugin_ble_gatt::android_context::ensure()` first instead of
initializing `ndk-context` itself; it may only be initialized once per
process.

This library **carries bytes; it does not encrypt them.** Layering your own
session protocol or end-to-end encryption on top is expected and
supported — and staying out of the way is deliberate, so that consumers
talking to third-party device firmware (which will never speak your
protocol) can use the raw GATT API directly. See ADR-0003.

Design decisions live in `docs/adr/`:
[0001](docs/adr/0001-ble-gatt-tauri-plugin-split-and-scope.md) — why this
split, why GATT-only, why hand-rolled instead of an existing crate.
[0002](docs/adr/0002-android-jni-bridge.md) — the Android JNI bridge, and
exactly what is and isn't verified about it.
[0003](docs/adr/0003-carrier-not-crypto-and-the-two-api-tiers.md) — carrier
vs. crypto, the two API tiers, and the no-AGPL constraint.
[0004](docs/adr/0004-mock-broker-for-cross-process-e2e.md) — the optional
`mock-broker` feature letting `backend::mock` bridge two real OS processes
over a socket instead of sharing an in-process `Arc`.

## Platform support

| Platform | Central | Peripheral | Status |
|---|---|---|---|
| Linux (BlueZ) | Yes | Yes | **M1 — implemented**, verified against real BlueZ hardware |
| Android | Yes | Yes (where the chipset/driver allows — see `CapabilityReport`) | **M1 — implemented, not yet verified as a working bridge.** Individual pieces confirmed on a real emulator (real `capabilities()` returning `{central: true, peripheral: true}`, real GATT-server advertise start), but no device-to-device round trip yet — physical-hardware verification is the real bar, planned for after the next minor patch/release. See ADR-0002. |
| Windows (WinRT) | — | — | M2 — reserved, not implemented |
| macOS / iOS | — | — | M3 — reserved, not implemented |

Capability is always discovered at runtime via `Backend::capabilities()`,
never assumed from the target OS — an Android device whose driver can't do
peripheral mode reports that honestly instead of failing opaquely later.

## Architecture

```
ble-gatt/src/
├── entities/     Entities: types, events, errors
├── hal/          Use Cases: the Backend / GattConnection ports (async, Tokio)
├── connection/   Use Cases: one connection's state machine, PeerLink
├── profile/      Use Cases: the datagram profile (fragmentation, reassembly)
└── drivers/      Frameworks & Drivers: one Backend per platform
    ├── linux.rs    BlueZ via bluer — central + peripheral       (M1)
    ├── android.rs  raw jni + ndk-context JNI bridge              (M1)
    ├── windows.rs  reserved                                      (M2)
    └── mock/       in-process, radio-free — CI-safe protocol tests;
                    optionally a cross-process broker behind the
                    `mock-broker` feature — see ADR-0004
```

### Roles, published state, handles

```rust
use ble_gatt::{Adapter, RadioStatus};

let adapter = Adapter::platform().await?;      // or Adapter::new(backend) to inject one
adapter.wait_available().await;                 // adapter.status(): watch::Receiver<RadioStatus>

let server = adapter.peripheral().serve(spec).await?; // advertising stops when `server` drops

let mut conn = adapter.central().connect(&peer).await?;
let mut state = conn.state();                   // watch::Receiver<ConnectionState>
tokio::spawn(conn.cancelled().run_until_cancelled_owned(read_loop()));
state.wait_for(|s| s.is_disconnected()).await?;
```

### Power profiles

`power::PowerAdvisor` turns the device's situation (foreground, battery,
charging, peers nearby) into advice: scan on/off times, a connection limit
and a connection priority, published on a `watch` channel. It never touches
the radio; the application applies it (ADR-0007 D8).

Layers are named after Clean Architecture and modules after the Bluetooth
specification (ADR-0007). The old paths (`ble_gatt::backend::…`,
`ble_gatt::datagram`, `ble_gatt::peer_link`) still resolve.

Every backend speaks the same generic GATT vocabulary
(`ServiceUuid`/`CharacteristicUuid`/`GattEvent`/`GattServiceSpec`/...) —
callers never see platform types (no `bluer::Device`, no JNI handles)
crossing the `Backend`/`GattConnection` port boundary.

### Three tiers, one carrier

`ble-gatt` carries bytes; it does not interpret them (ADR-0003). A consumer
picks the tier that fits:

| Tier | Use for | Owns the connection? |
|---|---|---|
| **raw GATT** (`Backend`) | a vendor device with its own protocol | you |
| **datagram** (`datagram::connect` / `serve`) | one message pipe, driven by hand | you |
| **`PeerLink`** | app-to-app links to a set of peers | the library |

See **`docs/interface.md`** for how `PeerLink` is meant to be used, and
**`docs/adr/0005`** for the owned link-state machine underneath it.

### Load-bearing files

Read the doc next to each before changing it:

| File | What it governs | Doc |
|---|---|---|
| `src/backend/link_state.rs` | `RadioState` / `CentralLink` / `PeripheralLink` — whether a dial happens, whether a stale record blocks one, whether a consumer is told a link died | `docs/adr/0005` |
| `src/peer_link.rs` | the tier-3 API and its dedicated-thread driver | `docs/interface.md`, `docs/adr/0005` |
| `src/backend/linux.rs` — `LinuxConnectGuard` / `pending_cleanup` | abandoned-connect cleanup, hardware-proven over ~6 review rounds | code comments + `docs/adr/0005` |
| `src/datagram/mod.rs` | fragmentation, the one-central and one-link-per-peer limits | ADR-0003 |

## Status

Early, under active development. `ble-gatt`'s Linux backend is real and
tested against BlueZ hardware. The Android backend is implemented and its
individual pieces run correctly on a real emulator, but it is **not yet
verified as a working bridge** — that requires a real device-to-device round
trip, planned on physical hardware after the next minor patch/release (see
ADR-0002). Not yet published to crates.io/npm — consume as a git
dependency.

## License

MIT
