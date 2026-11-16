# Architecture: crates, roles and ownership

The shared design is [ADR-0012](adr/0012-discovery-like-nearby.md).
[ADR-0011](adr/0011-windows-winrt-driver.md) owns Windows-native choices.
The managed core and iroh migration are implemented in this PR. Software checks
do not establish native hardware acceptance.

## Workspace boundaries

- [`ble-gatt`](../ble-gatt/README.md): generic GATT operations, complete messages,
  core peer coordination, resource ownership and native/mock drivers.
- [`ble-gatt-iroh`](../ble-gatt-iroh/README.md): custom packet/address adaptation,
  authenticated iroh key mapping and accounting of iroh users of a BLE carrier.
- [`tauri-plugin-ble-gatt`](../tauri-plugin-ble-gatt/README.md): host access,
  permissions, lazy construction and Android bridge/context setup.
- **External iroh:** authenticated QUIC connections, encryption, streams and paths.
- **Applications:** eligibility, product pairing, acceptance and application data.

The core works without iroh or Tauri. A Tauri app can obtain a shared core
`Adapter` from the plugin while using the Rust data path directly; packet traffic
need not cross frontend IPC. Android still needs a compatible native bridge and
Application context, even in a non-Tauri host.

## Object-oriented code

Use object-oriented programming (OOP) as the default for new and changed code.
In Rust, use structs or enums with `impl` methods and associated functions.
Put behavior on the type that owns its state, rules or protocol.

- Put state access on the owning object. A helper that takes `inner` or a
  context object belongs on that object or its existing service.
- Put protocol parsing, validation and conversion on the protocol type or its
  codec. A pure operation still has an owner.
- Reuse existing types and traits. Give a new type a real responsibility,
  rather than making one wrapper type for each function.
- Keep language and framework entry points as thin calls to owning objects.
  Keep a generic free helper only when no domain type owns the operation;
  explain that exception in review.
- Check this rule in each code review. Keep current interfaces and wire bytes
  until a separate implementation change updates them.

Load [the code-style skill](../.agents/skills/ble-gatt-code-style/SKILL.md)
before writing or reviewing code. This is a design rule, not a claim that all
existing free functions have been moved.

## Ownership map

Boxes group responsibilities; arrows show selected caller/provider dependencies,
not every call. One `PeerLink` worker owns managed communication.

```mermaid
flowchart TB
    subgraph apps["Apps: product policy outside this workspace"]
        direct["Rust / non-iroh apps"]
        tauriApp["Tauri apps / frontend"]
    end
    subgraph external["External iroh"]
        endpoint["Endpoint / QUIC / TLS / streams / paths"]
    end
    subgraph integrations["Our adapter crates"]
        irohAdapter["ble-gatt-iroh: packet/address adapter"]
        tauriAdapter["tauri-plugin-ble-gatt: host access/setup"]
    end
    subgraph core["ble-gatt: reusable core and native implementations"]
        peer["PeerLink worker: discovery / handshake / tie-break"]
        adapter["Adapter: radio / capabilities / role access"]
        central["Central: scan / connect / GATT client"]
        peripheral["Peripheral: advertise / serve / notify"]
        owned["Connection ownership / generation-safe cleanup"]
        messages["Datagram profile: fragmentation / reassembly"]
        discovery["Advertisement data / profile completion"]
        power["Power-profile advice"]
        hal["HAL: Driver / GattConnection ports"]
        entities["Entities: types / errors / role-session events"]
        subgraph drivers["Drivers: native OS effects"]
            linux["Linux / BlueZ"]
            android["Android / JNI"]
            windows["Windows / WinRT"]
            mock["Mock / optional broker"]
        end
    end
    direct --> peer
    direct -. "raw GATT when needed" .-> adapter
    tauriApp --> tauriAdapter
    tauriApp --> endpoint
    endpoint --> irohAdapter
    irohAdapter --> peer
    tauriAdapter --> adapter
    peer --> central
    peer --> peripheral
    peer --> owned
    peer --> messages
    peer --> discovery
    adapter --> central
    adapter --> peripheral
    central --> hal
    peripheral --> hal
    owned --> hal
    messages --> hal
    discovery --> hal
    hal --> entities
    power --> entities
    linux -. "implements" .-> hal
    android -. "implements" .-> hal
    windows -. "implements" .-> hal
    mock -. "implements" .-> hal
```

Drivers keep native objects/callbacks and session-scoped effects behind the
ports. Core policy does not replace their ownership guards or serialization.
Power profiles provide advice rather than mandatory radio control.

## Roles and lifecycle

A central initiates a BLE connection; a peripheral serves GATT. Writes in one
direction and notifications in the other already provide bidirectional traffic.
Either eligible application peer can initiate the managed protocol. No permanent
identity ranking or caller-selected initiation mode controls the initiator.
Raw role operations remain available for vendor devices without our handshake.

```text
advertise / discover -> peer information -> request connection
    -> acceptance on both sides -> messages -> disconnected
```

Metadata completion is an internal discovery procedure, not an accepted message
connection. Stopping discovery or advertising must preserve active communication
and the native resources it needs. Finishing a message does not release a healthy
connection. Explicit disconnect/cancel must not be undone by an independent loop.

The new core tie-break compares per-attempt nonces for the same endpoint. The
larger wins; equal nonces fail both. The losing outgoing request reports failure;
the winning incoming request is processed separately. Do not transfer its success
to the outgoing caller. Closing a loser is not global peer disconnection.
Endpoint correlation, native session generation and authenticated identity are
different facts. See ADR-0012 for selected bytes, typed outcomes and acceptance gates.

## Core coordinator and independent raw paths

The managed iroh path is:

```text
Application -> external iroh -> BleGattTransport
            -> PeerLink worker -> datagram profile -> HAL -> native driver
```

Non-iroh applications use the same worker directly, including standalone CLIs.
Raw `Central`, `Peripheral` and datagram APIs remain independent for vendor or
caller-owned protocols. They do not run the managed handshake.

The refactor replaces the existing worker's tracking/ranking contract. It does
not add a second manager. The unused crate-private `link_state` implementation
is retired; historical ADR-0005 remains unchanged.

Managed discovery completes missing metadata through bounded GATT reads. Raw
scan completion remains opt-in. Neither path implements Nearby's exact wire protocol.
The datagram server still admits one inbound peer; `MaxLinks` does not establish
Cluster/M-to-N or universal crossed-attempt support.

Each generation's existing transport task waits for bounded application capacity.
It holds at most one pending record and pauses raw reads, not the shared worker.
Native send and control paths remain responsive. Admission keeps the five-second
I/O bound; native end and retirement interrupt the wait. Queue pressure alone
does not create a receive gap.

Drivers publish bounded lifecycle events separately from characteristic writes.
Native/mock drivers, the broker and wrappers preserve this separation through
`Driver::lifecycle_events()`. The third-party default filters the legacy stream;
it cannot isolate loss. Critical lag remains terminal, unlike known payload gaps.
See [event-stream limits](interface.md#driver-event-streams-and-receive-loss).

Core handles separate scoped release from generation-wide disconnect. The iroh
adapter accounts for current users/paths separately from address history.
It uses core close reasons to block automatic reacquisition after terminal
disconnect. Old packets cannot authorize replacement; unexpected loss remains
recoverable. See [API usage](interface.md) and the [adapter](../ble-gatt-iroh/README.md).

## Layers and validation

The layer vocabulary remains that of [ADR-0007](adr/0007-layered-architecture.md):

- **Entities:** reusable data and pure rules; no I/O.
- **Use Cases:** `hal`, `roles`, `connection`, `profile` and `power`.
- **Interface Adapters:** the iroh and Tauri crates.
- **Frameworks & Drivers:** native and mock implementations of the ports.

### How the layers map onto OSI

OSI is an explanatory protocol analogy, not a crate or ownership rule. The
Bluetooth radio/OS stack supplies physical/link mechanisms; the GATT profile
adds complete messages. In the iroh composition, QUIC supplies transport streams
and security above that carrier, and the application supplies its data protocol.
Tauri setup and power advice are not packet layers. A type named `DataLink`
does not establish its actual protocol layer.

Follow [ADR-0012's migration gates](adr/0012-discovery-like-nearby.md#migration-and-verification-gates-for-this-pr).
Mock checks, JNI bytecode/packaging checks, observed native sessions and physical
OS teardown are separate evidence levels. Old candidate-model results do not
certify the new handshake. Procedures and limits live in
[hardware-verification.md](hardware-verification.md).
