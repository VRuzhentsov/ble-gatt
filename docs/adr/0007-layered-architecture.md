# 0007 — A layered, reusable architecture

## Status

Proposed. The layer map is in [`docs/architecture.md`](../architecture.md).
Restructuring the code to match is planned work (see Plan).

## Context

`ble-gatt` started as Fini's Bluetooth module and grew into a library that
closed-source Rust projects should be able to use as well. Fini is moving its
networking onto iroh (Fini ADR-0009), and `ble-gatt-iroh` connects the two
(ADR-0006). The library therefore needs a structure that:

- serves any Rust application, not only Fini;
- runs on Linux and Android now, then Windows, then Apple platforms;
- works with iroh, without depending on it, so that iroh can be replaced;
- keeps the battery in mind, since BLE is a background radio on phones.

Before deciding, the architecture of other BLE libraries was studied:
Nordic's Kotlin BLE Library, Kable, btleplug, bluest, bluer, `blew` and
`iroh-ble-transport`, and the BLE services of bitchat. Only ideas were
taken from the copyleft or source-available ones (`blew`,
`iroh-ble-transport`, bitchat).

## Decisions

**D1 — The library goes up to the transport layer, plus power policy.**
Roles, links, datagram channels, peer identity and power profiles are in
the library. Routing, store-and-forward, sessions and encryption are
application concerns; iroh provides some of them.

**D2 — Separate crates per role in the stack.**

- `ble-gatt` is the core: layers L0 to L3 and power policy.
- `ble-gatt-iroh` is the iroh adapter. All coupling to iroh lives in that
  crate; if a better alternative to iroh appears, it gets its own adapter
  crate and the core does not change.
- `tauri-plugin-ble-gatt` integrates the library into Tauri apps such as
  Fini.

The core's modules follow the layer map in `docs/architecture.md`, which
also states what each layer is responsible for and how modules on one layer
are designed across platforms.

**D3 — Platform backends are trait objects, supplied by dependency
injection.** `dyn` ports are kept, because they let a test or an e2e lane
swap the radio at runtime (the cross-process mock broker depends on this).
Every layer receives its dependencies as traits from its caller instead of
constructing them.

**D4–D7 — Open.** Role ports, connection scopes, handles and an
`Environment`-style object are still being discussed; the options are
recorded in the pull request and will be written here once chosen.

**D8 — Power policy is advice, as in bitchat.** A resolver turns inputs
(foreground or background, battery level, charging, peers nearby) into a
profile: scan on and off times, and a connection limit. Consumers keep
control of the hardware and apply the profile themselves; they can override
it.

**D9 — Native code ships with the crate.** The Android Kotlin bridge is
delivered by `tauri-plugin-ble-gatt` (`links` metadata and the plugin's
`android_path`), as originally intended. Applications no longer keep a copy
of `BleGattBridge.kt`; Fini's vendored copy is removed when Fini adopts the
plugin.

**D10 — The application chooses its network stack.** `ble-gatt` takes no
position on iroh or any other stack. Which stack an application uses, and
how it hides that choice from its own logic, is that application's decision
(for Fini, Fini ADR-0009).

**D11 — Tests: in-process mock and cross-process broker now; a simulator and
a hardware-in-the-loop rig later.**

**D12 — Tokio only.**

## Plan

1. **Fini uses `tauri-plugin-ble-gatt`** (D9). Make sure the plugin's Android
   bridge, `ndk-context` setup and permission handling cover what Fini does
   today, then drop Fini's vendored Kotlin and its own context bridging.
2. **Restructure the core into the layer modules** (`platform/`, `roles/`,
   `link/`, `transport/`, `power/`), moving code without changing behaviour.
3. **D4–D7**, once decided.
4. **Power profiles** (D8).
5. **Windows backend** (WinRT), then Apple.

Each step is its own change, keeps the existing tests green, and updates
`docs/architecture.md` where the map moves.

## Consequences

- The public API changes in steps 3 and 4; Fini, the main consumer, migrates
  with them.
- New platforms are added by implementing the role ports, guided by the
  "Designing a module" section of `docs/architecture.md`.
