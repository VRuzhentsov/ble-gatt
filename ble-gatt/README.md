# ble-gatt: reusable BLE communication

`ble-gatt` is the core Rust library in the [workspace](../README.md). It
provides GATT operations, complete-message channels, connection ownership and
platform drivers. It has no iroh or Tauri dependency.

## Status: current code and agreed target

The [shared design under review](../docs/adr/0012-discovery-like-nearby.md)
sets the target below. The source has not yet been refactored to meet it.
Current API names in this document describe implemented code; the target
contract is not a new API that callers can use today.

## Target responsibility

Symmetric peer communication belongs here, independently of a network stack:

- Either peer can initiate communication or reconnect.
- An established BLE connection carries messages in both directions.
- The library coordinates competing attempts and protects replacement
  connections from stale callbacks or cleanup.
- The core performs the new endpoint-ID/nonce tie-break. A losing outgoing
  request reports failure; the incoming winner is processed separately.
- Stopping discovery/advertising or completing a message does not close healthy
  communication. Explicit disconnect/cancellation is respected.
- Platform backoff, native resource ownership and collision recovery stay
  inside the BLE implementation.
- The high-level peer API has one symmetric initiation behavior. Applications
  do not select a previous dialler or an identity-ordered initiator.
- A platform limitation is reported explicitly. It is not hidden behind a
  different peer-to-peer contract.

Central and peripheral are roles of a particular BLE connection, not permanent
application roles. A central initiates and performs GATT client operations;
a peripheral accepts the connection and exposes the GATT service. One connection
is sufficient for bidirectional messages. Both roles being available separately
does not prove that a device supports simultaneous crossed attempts.

Raw role operations remain public for third-party devices such as sensors and
lamps. Those devices need not implement this library's message profile or the
symmetric peer contract.

## Current public surfaces

- `Adapter`: a shared driver, radio state, capabilities and role access.
- `Central`: raw scanning and an owned `Connection` for GATT operations.
- `Peripheral`: GATT serving, notifications and per-peer disconnect operations.
- `datagram`: complete messages with bounded fragmentation and reassembly.
- `advertisement`: readable GATT metadata and opt-in scan completion.
- `PeerLink`: an existing tracked-peer controller with its own worker runtime.
- `power`: advice about scan schedules, connection limits and priority. It
  does not operate the radio itself.

The current `PeerLink::LinkRole::Symmetric` chooses a lower-`LinkId` dialler.
It does **not** implement the target symmetric-initiation contract. A refactor
must evaluate its existing state tables and ownership mechanisms; the target
is not permission to add another controller beside it.

Current iroh callers use `DatagramChannel` directly through `ble-gatt-iroh`.
Running the existing `PeerLink` and that adapter as independent owners of the
same channels is not a supported way to reach the target.

## Modules and drivers

```text
src/
  entities/       Types, errors, addresses and role/session events
  hal/            Driver and GattConnection ports
  roles/          Adapter, Central and Peripheral
  connection/     Owned Connection, pure state tables and PeerLink
  profile/        Datagram framing/reassembly and advertisement completion
  power/          Power-profile advice
  drivers/
    linux.rs      BlueZ through bluer and D-Bus
    android.rs    Rust/JNI driver
    windows/      WinRT device, server, watcher and byte helpers
    mock/         Local simulation and optional cross-process broker
```

Apple drivers are not implemented in this source tree.

The Android Kotlin bridge currently ships in
[`tauri-plugin-ble-gatt/android`](../tauri-plugin-ble-gatt/android).
An Android host must provide compatible bridge packaging and context; lack of a
Rust Tauri dependency does not mean those native prerequisites disappear.
Whether host-neutral packaging needs a later change remains open.

## Resource and error contracts

Native operations use the `hal` ports. Drivers keep OS objects, callback tokens,
JNI references and connection-generation checks local. Shared policy must not
remove the ownership checks around native effects.

Current public lifetimes differ:

- Dropping a raw scan stream releases that scan.
- `Central::connect` returns an owned `Connection`; its Drop schedules cleanup
  when a Tokio runtime is available. Explicit disconnect can be awaited.
- `Peripheral::serve` returns `ServerHandle`; `stop` awaits shutdown, whereas
  Drop requests it in the background.
- Dropping the stream returned by `datagram::serve` stops handing out new
  channels. Existing channels continue receiving, and the served service
  does **not** stop. Use `datagram::stop_serving` to stop it.

These are existing contracts, not a blanket synchronous teardown guarantee.
Report typed errors and event loss. A driver connection-generation identifier
is not an authenticated application session.

## What stays outside the core

- iroh `EndpointId`, custom addresses, QUIC packets and path hooks.
- Tauri commands, frontend IPC and application permission dialogs.
- Product pairing, permitted peers, sync settings and application data formats.
- End-to-end authentication and encryption of the messages it carries.

The application decides who is eligible and when communication is wanted.
The request/event/release API, protocol encoding, discovery cache policy and
retry/liveness timing remain review gates in ADR-0012. The Nearby lifecycle and
attempt-level outcomes are the baseline, not new caller-selected modes.

## Validation boundary

Mock contracts, host builds, packaged JNI execution, native-role observations
and physical device behavior are separate evidence levels. The target requires
non-iroh tests as well as integration tests. It is not satisfied solely by a
passing iroh diagnostic or a central/peripheral capability report.

See the [architecture map](../docs/architecture.md),
[current usage guide](../docs/interface.md), [glossary](../docs/glossary.md)
and [hardware verification guide](../docs/hardware-verification.md).
