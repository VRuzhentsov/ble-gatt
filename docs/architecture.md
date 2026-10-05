# Architecture: layers, crates and modules

This file is the map of `ble-gatt`. Every module belongs to exactly one
layer, and every layer has one job. The decisions behind the map are in
[ADR-0007](adr/0007-layered-architecture.md).

The aim is a reusable, cross-platform BLE library for Rust applications:
the same API on Linux, Android, Windows and, later, Apple platforms, with a
network stack such as iroh plugged in from outside, never wired in.

## Naming rule

Nothing here is a home-grown scheme:

- **Layers** are named after [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
  (Robert C. Martin): Entities, Use Cases, Interface Adapters, Frameworks &
  Drivers.
- **Modules inside a layer** are named after the Bluetooth Core
  Specification (GAP roles, connection, GATT profile) and, for the platform
  interface, after Rust's [`embedded-hal`](https://docs.rs/embedded-hal)
  (a HAL of traits, implemented by drivers).
- **`L` followed by a number always means an OSI layer** (L2 = data link).
  It is never used for this library's layers. See
  [How the layers map onto OSI](#how-the-layers-map-onto-osi).

## Layers

Clean Architecture's dependency rule applies: source code depends only
inwards. Entities depend on nothing; Use Cases depend on Entities and declare
the interfaces (ports) they need; Drivers and Interface Adapters depend on
Use Cases and implement or call those interfaces.

| Layer (Clean Architecture) | Job | Modules or crates | Never does |
|---|---|---|---|
| **Entities** | Data types and rules that hold everywhere | `entities/`: `PeerAddress`, peer identity, connection states, errors, the fragment format | Any I/O, any platform call |
| **Use Cases** | What the library does | `hal/` (the port traits drivers implement), `roles/` (`Adapter`, `Central`, `Peripheral`, `Advertiser`), `connection/` (one connection's lifecycle), `profile/` (the datagram profile: message channel, peer identity in the advertisement, finding a known peer), `power/` (power profiles) | Call an operating system directly; know about iroh, Tauri or any application |
| **Interface Adapters** | Connect the library to a specific outside system | Crates `ble-gatt-iroh` (iroh custom transport) and `tauri-plugin-ble-gatt` (Tauri host integration) | Anything the core needs to work |
| **Frameworks & Drivers** | Talk to one operating system's Bluetooth API | `drivers/`: Linux (BlueZ via `bluer`), Android (JNI + Kotlin), Windows (WinRT), Apple (CoreBluetooth), mock radio | Policy, retries, protocol framing |
| *(outside)* Application | Product logic | Which network stack to use, sessions, sync, routing, UI | (lives outside this repository) |

### The Use Cases modules

| Module | Job | Name taken from |
|---|---|---|
| `hal/` | Traits a driver implements: scanning, connecting, GATT client and server operations, advertising | `embedded-hal` (traits) and its drivers |
| `roles/` | The BLE roles as a uniform API, created from the `Adapter` object | GAP roles in the Bluetooth Core Specification; Nordic's `client` / `server` / `advertiser` |
| `connection/` | Keep a connection to one peer healthy: state machine, one operation queue per device with timeouts, connection-scoped tasks, MTU and connection priority | GAP connection procedures; Kable's and Nordic's `connect()` |
| `profile/` | The datagram profile: whole messages over a connection (fragmentation, reassembly), peer identity in the advertisement, finding a known peer, deduplicating crossing connections | GATT-based profile in the Bluetooth Core Specification; Nordic's `profile()` |
| `power/` | Advise how hard the radio should work: inputs (foreground or background, battery level, charging, peers nearby) resolve to a schedule (scan on and off times, connection limit) | bitchat's `PowerManager` |

## How the layers map onto OSI

The two schemes answer different questions. The OSI model says what a
protocol does to the bytes on their way between devices. Clean Architecture
says how code is split and which part depends on which. All of this
library's protocol work falls inside OSI layer 2.

| OSI layer | What it does | Who does it, with `ble-gatt` and iroh (as in Fini) | `ble-gatt` part |
|---|---|---|---|
| L1 Physical | Radio signal | Bluetooth chip | — |
| L2 Data link | Frames between two devices in range | The operating system's Bluetooth stack (BLE link layer, L2CAP, ATT/GATT); on top of it, `ble-gatt` turns GATT into a message channel between two peers | Drivers, `roles/`, `connection/`, `profile/` |
| L3 Network | Addressing and choosing a path | iroh: a peer is addressed by its public key, and iroh picks Bluetooth or IP | `ble-gatt-iroh` connects `profile/` to it |
| L4 Transport | Reliable delivery, streams | QUIC inside iroh | — |
| L5–L7 Session, presentation, application | Sessions, message format, product logic | The application (for Fini: its ALPN, `PeerFrame`, sync) | — |
| — | Not a protocol layer | Power profiles, the Tauri integration | `power/`, `tauri-plugin-ble-gatt` |

Fini's `DataLink` is named after OSI L2 and corresponds to this library's
`profile/` (Fini `docs/glossary.md`).

## Who owns which layer

| Layer | Owner |
|---|---|
| Entities, Use Cases, Frameworks & Drivers | `ble-gatt` (this repository, core crate) |
| Interface Adapter for iroh | `ble-gatt-iroh` (this repository). All iroh coupling lives here and nowhere else. A different network stack gets its own adapter crate next to it. |
| Interface Adapter for Tauri | `tauri-plugin-ble-gatt` (this repository) |
| Application | The application. Fini decides that Fini uses iroh (Fini ADR-0009); `ble-gatt` takes no such decision. |

## Crates

| Crate | Layers | Depends on |
|---|---|---|
| `ble-gatt` | Entities, Use Cases, Frameworks & Drivers | platform SDKs only |
| `ble-gatt-iroh` | Interface Adapters | `ble-gatt`, `iroh` |
| `tauri-plugin-ble-gatt` | Interface Adapters | `ble-gatt`, `tauri` |

## Module map of the core crate

The target layout. Moving today's modules into it is planned work
(ADR-0007, Plan).

```
ble-gatt/src/
  entities/          Entities      types, errors, fragment format
  hal/               Use Cases     port traits the drivers implement
  roles/             Use Cases     public role API; holds injected drivers
    adapter.rs                     the device's Bluetooth adapter: on/off, events, capabilities
    central.rs
    peripheral.rs
    advertiser.rs
  connection/        Use Cases     connection lifecycle, op queue, connection scope
  profile/           Use Cases     datagram profile, peer identity, finding peers
  power/             Use Cases     power profiles and the resolver
  drivers/           Frameworks & Drivers   one sub-module per OS, all implementing hal/
    linux/
    android/
    windows/         (planned)
    apple/           (planned)
    mock/
```

Today's modules map onto it as:

| Today | Target |
|---|---|
| `backend/{linux,android,mock}` | `drivers/` |
| the `Backend` trait | `hal/` |
| `backend/link_state.rs`, `peer_link.rs` | `connection/` |
| `datagram/` | `profile/` |
| `models.rs`, `error.rs` | `entities/` |

## Designing a module

**Dependency injection.** Use Cases declare their dependencies as traits in
`hal/` and receive implementations from the caller. `roles/` holds
`Arc<dyn …>` values supplied at construction; tests supply mocks. A
convenience constructor (`platform()`) builds the real drivers for the
current OS, but nothing in the core creates its own dependencies behind the
caller's back.

**One interface, several platforms (drivers).** Every driver:

- implements the same `hal/` traits, with the same behaviour, so callers
  cannot tell platforms apart;
- reports what it cannot do through `Adapter` capabilities or a typed
  `Unsupported` error, never by silently doing nothing;
- keeps platform quirks (Android's one-operation-at-a-time GATT, BlueZ
  caching, Windows permission prompts) inside the module, documented next to
  the code that handles them;
- has its native code (Kotlin, etc.) shipped by the crate itself, never
  copied into an application;
- is covered by the shared behaviour tests that run against the mock, plus
  hardware checks listed in `docs/hardware-verification.md`.

**Lifetimes.** Anything that keeps the radio busy (advertising, a GATT
server, a scan) is returned as a handle that stops it when dropped, as
`bluer` does (ADR-0007 D6).

**Errors and events: pub/sub.** Errors are typed per layer. State is
published, never polled: a value that changes (adapter on or off, a
connection's state) is a `tokio::sync::watch` channel, and a sequence of
events is a broadcast stream; any number of subscribers can follow either
(ADR-0007 D5).

**Runtime.** Async, Tokio only.
