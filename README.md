# ble-gatt

Async BLE GATT primitives for Rust, with **central and peripheral roles** and
no core Tauri dependency. Uses
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
  custom transport over core `PeerChannel` messages. It translates packets and
  addresses and accounts for authenticated QUIC users of a shared carrier.

## Code style

Use object-oriented code as the default. The rules are in
[docs/code-style.md](docs/code-style.md). Agents apply them through the
[dev skill](.agents/skills/dev/SKILL.md), which follows Spike and Stabilize;
tests are brought up to date with the [testing skill](.agents/skills/testing/SKILL.md).

## Managed communication: working-tree implementation

[ADR-0012](docs/adr/0012-discovery-like-nearby.md) now has a core implementation:
`PeerLink` provides discovery, explicit requests, mutual acceptance, scoped
channels and endpoint/nonce arbitration. Either peer can initiate. An outgoing
loser fails; the incoming winner proceeds separately through acceptance.
The refactor reuses one dedicated core worker, not a second manager.
Bounded application receive pressure pauses only that generation, within the
existing I/O deadline. Native/mock lifecycle streams are isolated from payload
traffic; third-party defaults retain unknown-loss limits. See
[event and receive contracts](docs/interface.md#driver-event-streams-and-receive-loss).

Raw central/peripheral and datagram APIs remain separate for vendor protocols
and caller-owned channels. A standalone CLI can use the
[minimal managed example](docs/interface.md#minimal-outgoing-use) without iroh or Tauri.
The Tauri plugin supplies host setup; external iroh supplies QUIC and security.

**This PR implements the managed refactor. Software checks do not establish
native hardware acceptance.** [ADR-0011](docs/adr/0011-windows-winrt-driver.md) owns native
Windows choices. Neither ADR claims Google Nearby wire interoperability.

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

This library **carries bytes; it does not encrypt them.** Supply your own session
protocol and security layer. Raw GATT remains available for vendor firmware
without this library's message protocol. See ADR-0003.

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
| Windows (WinRT) | Yes | Yes (where the adapter and driver allow — see `CapabilityReport`) | **M2 — first implementation; bidirectional cross-device acceptance pending.** Earlier build/capability/advertising observations are not proof of the new peer protocol. Shared metadata completion is described in ADR-0012; native choices and verification are in ADR-0011. |
| macOS / iOS | — | — | M3 — reserved, not implemented |

Read capabilities at runtime with `Driver::capabilities()`. Do not infer support
from the OS name.

## Architecture

```
ble-gatt/src/
├── entities/     Entities: types, events, errors
├── hal/          Use Cases: the Driver / GattConnection ports (async, Tokio)
├── roles/        Use Cases: Adapter, Central and Peripheral
├── connection/   Use Cases: owned Connection, managed PeerLink worker and codec
├── profile/      Use Cases: the datagram profile (fragmentation, reassembly)
├── power/        Use Cases: power-profile advice
└── drivers/      Frameworks & Drivers: one Driver per platform
    ├── linux/      BlueZ via bluer — central + peripheral       (M1)
    ├── android/    JNI driver with explicit context support    (M1)
    ├── windows/    WinRT via the windows crate — central + peripheral (M2)
    └── mock/       in-process, radio-free — CI-safe protocol tests;
                    optionally a cross-process broker behind the
                    `mock-broker` feature — see ADR-0004
```

### Roles, published state, handles

`Adapter::platform().await` obtains raw role access. `Central::connect` returns
an owned `Connection`; `Peripheral::serve` returns `ServerHandle`.
Their state/cleanup contracts differ from managed scoped channels. See
[API usage](docs/interface.md#raw-apis-and-lifetime-limits).

### Power profiles

`power::PowerProfiles` turns the device's situation (foreground, battery,
charging, peers nearby) into advice: scan on/off times, a connection limit
and a connection priority, published on a `watch` channel. It never touches
the radio; the application applies it (ADR-0007 D8).

Layers are named after Clean Architecture and modules after the Bluetooth
specification (ADR-0007). The short paths `ble_gatt::datagram` and
`ble_gatt::peer_link` still resolve; `ble_gatt::backend::…` is gone (use
`ble_gatt::drivers` and `ble_gatt::hal`). Raw datagram helpers take an `Adapter`;
`ble_gatt_iroh::dial_with` takes `Arc<PeerLink>`. `Driver` is the native port.

Every driver speaks the same generic GATT vocabulary
(`ServiceUuid`/`CharacteristicUuid`/`GattEvent`/`GattServiceSpec`/...) —
callers never see platform types (no `bluer::Device`, no JNI handles)
crossing the `Driver`/`GattConnection` port boundary.

### Three tiers, one carrier

`ble-gatt` carries application bytes without authenticating or encrypting them
(ADR-0003). Managed channels add control records. Choose the required tier:

| Tier | Use for | Owns the connection? |
|---|---|---|
| **raw GATT** (`Driver`) | a vendor device with its own protocol | you |
| **datagram** (`datagram::connect` / `serve`) | one message pipe, driven by hand | you |
| **`PeerLink`** | app-to-app links to a set of peers | the library |

See [`docs/interface.md`](docs/interface.md) for requests, incoming acceptance,
scoped release, disconnect and the adapter's terminal-authorization rules.

### Load-bearing files

Read the doc next to each before changing it:

| File | What it governs | Doc |
|---|---|---|
| `ble-gatt/src/connection/peer_link.rs` | request/accept and scoped channel API | `docs/interface.md`, ADR-0012 |
| `ble-gatt/src/connection/managed_worker.rs` | attempt state, arbitration and native channel ownership | ADR-0012 |
| `ble-gatt/src/connection/managed_codec.rs` | managed metadata and record bytes | ADR-0012 |
| `ble-gatt/src/drivers/linux/mod.rs` — `LinuxConnectGuard` / `pending_cleanup` | abandoned-connect cleanup and native ownership | code comments, ADR-0009 |
| `ble-gatt/src/profile/datagram/mod.rs` | complete messages, serving and channel limits | ADR-0003, ADR-0012 |

The unused crate-private `link_state` implementation is retired. ADR-0005 remains
a historical record, not the current managed state-machine specification.

## Status

Under active development. Implemented APIs, target design and device acceptance
are separate claims. The architecture does not certify native roles or background scenarios. See
[hardware verification and current iroh diagnostic checklist](docs/hardware-verification.md).
Not yet published to crates.io/npm — consume as a git dependency, pinned to a
release tag for released APIs. The example tag does not include this working-tree refactor:

```toml
ble-gatt = { git = "https://github.com/VRuzhentsov/ble-gatt", tag = "v0.1.0" }
```

## Contributing

Turn on the pre-commit hook once per clone:

```sh
git config core.hooksPath .githooks
```

When a commit has staged `.rs` files, the hook (`.githooks/pre-commit`) runs
the same checks as CI: `cargo fmt --all --check` and
`cargo clippy --workspace --all-targets --all-features -- -D warnings`. It
needs the Rust toolchain with the `clippy` and `rustfmt` components. Skip it
one time with `git commit --no-verify`.

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
