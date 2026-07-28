# ble-gatt

Async, transport-agnostic BLE GATT primitives for Rust — **central and
peripheral role**, no Tauri dependency in the core crate. Built on top of
established, widely-used crates rather than reinventing plumbing:
[`bluer`](https://github.com/bluez/bluer) for BlueZ, [`tokio`](https://github.com/tokio-rs/tokio)
for async/channels, [`thiserror`](https://github.com/dtolnay/thiserror) and
[`async-trait`](https://github.com/dtolnay/async-trait) for the trait
surface.

This repo is a Cargo workspace with two published crates:

- **`ble-gatt`** — the core library. Scan/advertise, GATT client
  read/write/subscribe, GATT server characteristics, connection lifecycle as
  an event stream, and a radio-free `MockBackend` for CI-safe protocol
  tests. Depends on nothing Tauri-specific — usable from any Tokio-based
  Rust program (a CLI, a daemon, another GUI framework).
- **`tauri-plugin-ble-gatt`** — a thin [Tauri](https://tauri.app) plugin
  wrapper around `ble-gatt`, following Tauri's own `tauri-plugin-*` naming
  convention for its mobile-plugin tooling.

## Why hand-rolled, not an existing crate

Checked directly (GitHub API + READMEs) before writing a line of this:

| Crate | Central | Peripheral | Android | License | Maturity |
|---|---|---|---|---|---|
| `btleplug` / `tauri-plugin-blec` | Yes | **No** (host-side only, by design) | Yes | Apache-2.0 | Active, healthy |
| `bluest` | Yes | **No** (explicit in README) | Not yet ("planned") | Apache-2.0 | Active |
| `ble-peripheral-rust` | — | Yes (by design) | **No** Android backend in source tree | MIT | Reasonably active |
| `blew` / `tauri-plugin-blew` | Yes | Yes | Yes | **AGPL-3.0** | Author-labeled "experimental"; stale |

No existing crate does BLE peripheral mode on Android under a permissive
license. `ble-gatt` exists to close that gap without taking on AGPL.

## Platform support

| Platform | Central | Peripheral | Status |
|---|---|---|---|
| Linux (BlueZ) | Yes | Yes | **M1 — implemented**, verified against real BlueZ hardware |
| Android | Yes | Yes (where the chipset/driver allows — see `CapabilityReport`) | **M1 — in progress** |
| Windows (WinRT) | — | — | M2 — reserved, not implemented |
| macOS / iOS | — | — | M3 — reserved, not implemented |

Capability is always discovered at runtime via `Backend::capabilities()`,
never assumed from the target OS — an Android device whose driver can't do
peripheral mode reports that honestly instead of failing opaquely later.

## Architecture

```
Backend trait (async, Tokio)
├── linux.rs    BlueZ via bluer — central + peripheral         (M1)
├── android.rs  raw jni + ndk-context JNI bridge                (M1, in progress)
├── windows.rs  reserved                                        (M2)
└── mock.rs     in-process, radio-free — CI-safe protocol tests
```

Every backend speaks the same generic GATT vocabulary
(`ServiceUuid`/`CharacteristicUuid`/`GattEvent`/`GattServiceSpec`/...) —
callers never see platform types (no `bluer::Device`, no JNI handles)
crossing the `Backend`/`GattConnection` port boundary.

## Status

Early, under active development. `ble-gatt`'s Linux backend is real and
tested against BlueZ hardware; the Android backend is the current work in
progress. Not yet published to crates.io/npm — consume as a git dependency.

## License

MIT
