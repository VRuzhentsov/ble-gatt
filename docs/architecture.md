# Architecture: layers, crates and modules

This file is the map of `ble-gatt`. Every module belongs to exactly one
layer, and every layer has one job. The decisions behind the map are in
[ADR-0007](adr/0007-layered-architecture.md).

The aim is a reusable, cross-platform BLE library for Rust applications:
the same API on Linux, Android, Windows and, later, Apple platforms, with a
network stack such as iroh plugged in from outside, never wired in.

## Layers

| Layer | Job | Owns | Never does |
|---|---|---|---|
| **L0 Platform** | Talk to one operating system's Bluetooth API | BlueZ (via `bluer`), Android (JNI + Kotlin), Windows (WinRT), Apple (CoreBluetooth), mock radio | Policy, retries, protocol framing |
| **L1 Roles** | The BLE roles as a uniform API | `Central` (scan, connect, read, write, subscribe), `Peripheral` (GATT server, requests, notify), `Advertiser`, `Environment` (adapter power, permissions, capabilities) | Deciding when to scan or connect |
| **L2 Link** | Keep a connection to one peer healthy | Connection state machine, one operation queue per device with timeouts, connection-scoped tasks, MTU and connection priority | Knowing who the peer is |
| **L3 Transport** | Move messages between peers | Datagram channel (fragmentation, reassembly), peer identity in the advertisement, finding a known peer, deduplicating crossing connections | Encryption, routing, any specific network stack |
| **Policy** | Advise how hard the radio should work | Power profiles: inputs (foreground or background, battery level, charging, peers nearby) resolve to a schedule (scan on and off times, connection limit) | Switching hardware on or off itself |
| **L4 Adapters** | Connect the library to the outside world | `ble-gatt-iroh` (iroh custom transport), `tauri-plugin-ble-gatt` (Tauri host integration) | Anything the core needs to work |
| **L5 Application** | Product logic | Which network stack to use, sessions, sync, routing, store-and-forward, UI | (lives outside this repository) |

Dependencies point downwards only: a layer uses the layer below through its
public interface and knows nothing about the layers above. L4 adapters
depend on the core; the core never depends on an adapter.

## Who owns which layer

| Layer | Owner |
|---|---|
| L0–L3 and Policy | `ble-gatt` (this repository, core crate) |
| L4 iroh adapter | `ble-gatt-iroh` (this repository). All iroh coupling lives here and nowhere else. A different network stack gets its own adapter crate next to it. |
| L4 Tauri adapter | `tauri-plugin-ble-gatt` (this repository) |
| L5 | The application. Fini decides that Fini uses iroh (Fini ADR-0009); `ble-gatt` takes no such decision. |

## Crates

| Crate | Layers | Depends on |
|---|---|---|
| `ble-gatt` | L0, L1, L2, L3, Policy | platform SDKs only |
| `ble-gatt-iroh` | L4 | `ble-gatt`, `iroh` |
| `tauri-plugin-ble-gatt` | L4 | `ble-gatt`, `tauri` |

## Module map of the core crate

The target layout. Moving today's modules into it is planned work
(ADR-0007, Plan).

```
ble-gatt/src/
  platform/          L0  one sub-module per OS, all implementing the same ports
    linux/
    android/
    windows/         (planned)
    apple/           (planned)
    mock/
  roles/             L1  public role API; holds injected platform ports
    central.rs
    peripheral.rs
    advertiser.rs
    environment.rs
  link/              L2  connection lifecycle, op queue, connection scope
  transport/         L3  datagram channel, peer identity, peer finding
  power/             Policy  profiles and the resolver
  models.rs, error.rs
```

Today's modules map onto it as: `backend/{linux,android,mock}` → `platform/`,
`backend/link_state.rs` and `peer_link.rs` → `link/`, `datagram/` →
`transport/`.

## Designing a module

**Dependency injection.** Each layer defines its dependencies as traits
(ports) and receives implementations from the caller. L1 holds
`Arc<dyn …Port>` values supplied at construction; tests supply mocks. A
convenience constructor (`platform()`) builds the real ports for the current
OS, but nothing below L4 creates its own dependencies behind the caller's
back.

**One layer, several platforms (L0).** Every platform module:

- implements the same port traits, with the same behaviour, so callers
  cannot tell platforms apart;
- reports what it cannot do through `Environment` capabilities or a typed
  `Unsupported` error, never by silently doing nothing;
- keeps platform quirks (Android's one-operation-at-a-time GATT, BlueZ
  caching, Windows permission prompts) inside the module, documented next to
  the code that handles them;
- has its native code (Kotlin, etc.) shipped by the crate itself, never
  copied into an application;
- is covered by the shared behaviour tests that run against the mock, plus
  hardware checks listed in `docs/hardware-verification.md`.

**Lifetimes.** Anything that keeps the radio busy (advertising, a GATT
server, a scan, a connection) is returned as a handle that stops it when
dropped.

**Errors and events.** Errors are typed per layer. State changes are
streams that any number of subscribers can follow.

**Runtime.** Async, Tokio only.
