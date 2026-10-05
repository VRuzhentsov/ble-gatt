# 0007 — A layered, reusable architecture

## Status

Accepted. The layer map is in [`docs/architecture.md`](../architecture.md).
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

**D1 — The library goes up to the datagram profile, plus power policy.**
Roles, connections, the datagram profile (message channel, peer identity)
and power profiles are in the library. Routing, store-and-forward, sessions and encryption are
application concerns; iroh provides some of them.

**D2 — Separate crates per role in the stack.**

- `ble-gatt` is the core: Entities, Use Cases (including power policy) and
  the platform drivers.
- `ble-gatt-iroh` is the iroh adapter. All coupling to iroh lives in that
  crate; if a better alternative to iroh appears, it gets its own adapter
  crate and the core does not change.
- `tauri-plugin-ble-gatt` integrates the library into Tauri apps such as
  Fini.

The core's modules follow the layer map in `docs/architecture.md` (D13),
which also states what each layer is responsible for and how modules on one
layer are designed across platforms.

**D3 — Platform backends are trait objects, supplied by dependency
injection.** `dyn` ports are kept, because they let a test or an e2e lane
swap the radio at runtime (the cross-process mock broker depends on this).
Every layer receives its dependencies as traits from its caller instead of
constructing them.

**D4 — One object per role.** The single `Backend` trait is split into
`Central`, `Peripheral` and `Advertiser`, the way every platform splits them
(Android `BluetoothLeScanner` / `BluetoothGattServer` /
`BluetoothLeAdvertiser`; Apple `CBCentralManager` / `CBPeripheralManager`;
Windows `BluetoothLEAdvertisementWatcher` / `GattServiceProvider` /
`BluetoothLEAdvertisementPublisher`; BlueZ `Adapter1` / `GattManager1` /
`LEAdvertisingManager1`), and as `blew` and Nordic's library do. A backend
then maps one to one onto its platform, an application uses and mocks only
the roles it needs, and a device that cannot advertise still scans.

**D5 — A connection publishes its state; cancellation is derived from it.**
Each connection publishes its state (connecting, connected, disconnected) on
a `tokio::sync::watch` channel: any number of subscribers, each seeing the
current state as soon as it subscribes. This is the pub/sub style used across
`ble-gatt` and Fini, and the same model as Kable's and Nordic's
`StateFlow<State>`. As a convenience on top of that subscription, a
connection also hands out a `tokio_util::sync::CancellationToken` that fires
when it disconnects, so a task can be tied to the connection in one line
(`token.run_until_cancelled(task)`).

```rust
let link = central.connect(addr).await?;
let mut state = link.state();                      // watch::Receiver<LinkState>
tokio::spawn(link.cancelled().run_until_cancelled(read_loop()));
state.wait_for(|s| s.is_disconnected()).await;
```

**D6 — Radio resources are handles that stop on drop.** Advertising, a GATT
server and a scan are returned as handles; dropping one stops it, so an
early return cannot leave the radio running. This follows `bluer`, which
already returns `AdvertisementHandle` and `ApplicationHandle` on Linux, and
matches how `ble-gatt`'s scan stream already stops when dropped. Stopping
happens in the background, since `Drop` cannot wait for the platform.

**D7 — An `Adapter` object.** One object stands for the device's Bluetooth
adapter: whether it is on, events when that changes, and what the hardware
can do. Roles are created from it. This follows `bluest`
(`Adapter::default()`, `wait_available()`, `events()`), btleplug, Android's
`BluetoothAdapter` and BlueZ's `Adapter1`. Asking the user for permissions is
Android-specific and stays in `tauri-plugin-ble-gatt`.

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

**D13 — Layers are named after Clean Architecture, modules after the
Bluetooth specification.** No home-grown layer scheme:

- Layers follow Clean Architecture (Robert C. Martin): Entities, Use Cases,
  Interface Adapters, Frameworks & Drivers, with its dependency rule (code
  depends only inwards).
- Modules inside the core are named after the Bluetooth Core Specification
  (`roles/` for the GAP roles, `connection/`, `profile/` for the datagram
  GATT profile) and after Rust's `embedded-hal` (`hal/` traits implemented by
  `drivers/`).
- `L` with a number always means an OSI layer, in this repository and in
  Fini. An earlier draft numbered this library's layers L0 to L5; that
  clashed with OSI (its "L2 Link" and "L3 Transport" were both OSI L2 work,
  and "transport" means OSI L4 in iroh and QUIC), so it was dropped.

## Plan

1. **Fini uses `tauri-plugin-ble-gatt`** (D9). Make sure the plugin's Android
   bridge, `ndk-context` setup and permission handling cover what Fini does
   today, then drop Fini's vendored Kotlin and its own context bridging.
2. **Restructure the core into the layer modules** (`entities/`, `hal/`,
   `roles/`, `connection/`, `profile/`, `power/`, `drivers/`; D13), moving
   code without changing behaviour.
3. **Role objects, the `Adapter`, handles and published connection state**
   (D4–D7).
4. **Power profiles** (D8).
5. **Windows backend** (WinRT), then Apple.

Each step is its own change, keeps the existing tests green, and updates
`docs/architecture.md` where the map moves.

## Consequences

- The public API changes in steps 3 and 4; Fini, the main consumer, migrates
  with them.
- New platforms are added by implementing the role ports, guided by the
  "Designing a module" section of `docs/architecture.md`.
