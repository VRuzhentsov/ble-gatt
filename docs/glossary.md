# Glossary

This file separates writing rules from the project's technical vocabulary.
Use one term for each concept. Report conflicts between definitions and code;
a definition does not change implemented behavior.

**Synonyms.** Where another library or platform uses a different word for the
same thing, it is listed as *Also called*. Use the first word in code and
documents.

The working-tree API follows [ADR-0012](adr/0012-discovery-like-nearby.md).
Implementation, integrated verification and hardware acceptance are separate claims.

## Writing and naming rules

Apply these STE-based rules to new or touched names, comments, logs, documents
and pull request text. The technical terms below define meaning, not sentence structure.

- Use active voice, American English and complete words. Do not use contractions.
- Give one instruction per sentence. Aim for at most 20 words in procedures
  and 25 words in descriptions. Use lists for complex conditions.
- Remove repetition. State the decision, owner, contract and evidence limit.
- Split a proposed name into words. Use established English and glossary terms
  with one meaning. Explain new technical terms before adding them below.
- Keep existing Rust/module naming conventions. Do not import another application's
  role suffixes or directory layout.
- Preserve public APIs, protocol fields, foreign names, quoted errors and source
  identifiers. A writing pass does not authorize compatibility changes.
- Separate current behavior, target requirements and verification. Preserve
  accepted historical ADRs and third-party attribution.
- Refer to the manifest (`Cargo.toml`) for versions. Keep versions, tags and
  commit hashes out of docs.
- Keep glossary entries to a short definition of the term. Put examples and
  usage in the doc that covers the feature.

These are local writing rules, not verified ASD-STE100 dictionary compliance or
certification. Do not copy the copyrighted ASD dictionary into this repository.

## Code design terms

### Object-oriented programming (OOP)

Code organized around objects that own state and behavior. A class defines
such objects. Rust uses structs or enums with `impl` blocks for this purpose.
See [the code rule](code-style.md#object-oriented-code).

## Bluetooth basics

### BLE (Bluetooth Low Energy)
The low-power part of Bluetooth that this library uses. Classic Bluetooth
(audio, file transfer) is not used.

### Radio
The device's Bluetooth hardware as a physical resource. It uses battery while
it scans or advertises.

### Bluetooth adapter
The Bluetooth hardware of one device as the operating system exposes it: it
can be on or off, present or missing. *Also called:* controller, local
adapter, `BluetoothAdapter` (Android), `org.bluez.Adapter1` (Linux).
Not to be confused with an [adapter crate](#adapter-crate).

### Role
What a device does in a BLE exchange. One device can hold several roles at
once.

### Central
The role that looks for other devices and connects to them. *Also called:*
client, GATT client, scanner. Android: `BluetoothLeScanner` and
`BluetoothGatt`; Apple: `CBCentralManager`.

### Peripheral
The role that is connected to: it holds data (a GATT server) that a central
reads and writes. *Also called:* server, GATT server. Android:
`BluetoothGattServer`; Apple: `CBPeripheralManager`.

### Advertiser
The role that broadcasts short packets saying "I am here" so a central can
find the device. *Also called:* broadcaster. Android:
`BluetoothLeAdvertiser`; Linux: `LEAdvertisingManager1`.

### Advertisement
One of those broadcast packets: up to about 31 bytes that carry service UUIDs
and small data. *Also called:* advertising data, adv.

### Advertisement update
A change to what a peripheral advertises while its GATT server and the
centrals connected to it stay. See
[managed advertisement updates](interface.md#managed-advertisement-updates).

### Stop serving
The end of a peripheral's advertisement and GATT server together, which drops
the centrals connected to it. See
[raw APIs](interface.md#raw-apis-and-lifetime-limits).

### Radio recovery
The radio coming back on after it was off or the Bluetooth service restarted.
Serving then registers again what it last served (ADR-0010).

### Scan response
A second packet of up to 31 bytes that an advertiser sends only when a
scanner asks for it, during an *active* scan. ADR-0012 §2 gives what this
library puts in it. *Also called:* SCAN_RSP.

### Local name
A short, human-readable name a peripheral advertises so a scanner can show
it without connecting. Not the OS device name (the phone's model name).

### Scan
A central listening for advertisements for a while. Scanning is the most
battery-expensive thing BLE does on a phone.

### Connection
A link between one central and one peripheral after the central has
connected, over which GATT operations run. Ends when either side
disconnects or the devices move out of range.

### GATT
The way data is organised and exchanged over a connection: the peripheral
exposes services, each with characteristics; the central reads, writes and
subscribes to them.

### Service
A group of characteristics, identified by a UUID.

### Characteristic
One value inside a service, identified by a UUID, that can be read, written,
or watched for changes.

### Notify, indicate
The peripheral pushing a characteristic's new value to a central that
subscribed. *Notify* is not acknowledged; *indicate* is.

### Subscribe
A central asking to receive a characteristic's notifications or indications.

### MTU
The largest single message a connection carries at once, agreed by both sides
after connecting (at least 23 bytes). Anything larger is split into fragments.

### Attribute value
The bytes carried by one read, write or notification of a characteristic.
Bluetooth caps it at 512 bytes, whatever the MTU.

### Connection priority
A request to the phone's Bluetooth stack to trade battery for speed on one
connection (shorter connection interval). Android only.

### OS pairing (bonding)
The operating system's own Bluetooth pairing, with its system dialog and
stored keys. This library does not use it. *Not the same as* an
application's own pairing, which happens over this library's channels.

### Private address
A Bluetooth address a phone makes up and changes from time to time so it
cannot be tracked. A peer therefore cannot be recognized by its address alone.

### Connectable advertisement
An advertisement a central can answer by connecting. A non-connectable
advertisement only carries data.

### Peripheral role support
Whether a Bluetooth adapter and its driver can run a GATT server and
advertise it. Not every adapter can. The driver reports it as
`CapabilityReport::peripheral`.

### Excluded central
A central that the server refused but the platform cannot disconnect. The
server ignores it until it leaves by itself (Windows; ADR-0011).

### Request deferral
A Windows promise to answer an inbound GATT read or write after the event
handler has returned (ADR-0011).

## This library's concepts

### Fast advertisement
A radio record of at most 27 bytes that carries a managed peer's endpoint,
its metadata hash and some of the caller's manufacturer data, so a scanner
lists the peer without connecting. The term comes from Google Nearby
(ADR-0012 §2, §5).

### Advertisement header
A 9-byte radio record that carries the metadata hash and the endpoint tag, so
a scanner can report a peer from its cache (ADR-0012 §2, §5).

### Metadata hash
A hash of the complete readable advertisement value (the GATT advertisement).
It changes when any advertised field changes. A cache key, not a security check.

### Endpoint tag
The first four bytes of an endpoint identifier, as the advertisement header
carries them.

### Completion read
The one read a scanner makes when a peer's radio records do not carry the
complete advertised value (ADR-0012 §2).

### Advertising mode
How often a peripheral advertises (`AdvertisingMode`). More often is found
sooner and costs more battery.

### Scan parameters
How long and how often a scanner listens (scan window, scan interval).

### GATT advertisement
The advertisement data, served in a readable characteristic of every datagram
service, so a scanner can read what the radio advertisement does not carry
(ADR-0012).

### Peer
Another device this library talks to.

### Peer address
The value a driver uses to reach a peer right now (`PeerAddress`). It is
opaque and may change (see [private address](#private-address)), so it is not
a peer's identity.

### Peer identity
What recognises a peer across address changes: a fingerprint the peer puts in
its advertisement.

### Link
A managed communication relationship with a peer, coordinated by `PeerLink`.
*Not the same as* OSI L2 or the BLE link layer.

### Driver session
An identifier that distinguishes successive GATT connections to the same
address, so a replacement ignores stale events. Not an authenticated QUIC
connection or an application conversation.

### GAP (Generic Access Profile)
The part of the Bluetooth Core Specification that defines the roles
(central, peripheral, broadcaster, observer) and how devices find and
connect to each other. The `roles/` module is named after it.

### Profile
In the Bluetooth specification, a set of GATT services and the rules for
using them for one purpose. The `profile/` module is named after it.
*Not the same as* a [power profile](#power-profile).

### Datagram channel
A message channel over a connection: whole messages in, whole messages out,
however small the MTU.

### Fragment, reassembly
A *fragment* is one MTU-sized piece of a message; *reassembly* is joining the
pieces back into the message.

## Code structure

### Layer
One level of the library with one job, named after
[Clean Architecture](#clean-architecture); see
[`architecture.md`](architecture.md).

### Clean Architecture
Robert C. Martin's way of splitting code into rings, each depending only on
the rings inside it: **Entities** (data types and rules that hold
everywhere), **Use Cases** (what the software does), **Interface Adapters**
(connecting to a specific outside system) and **Frameworks & Drivers**
(external APIs). This library's layers use these names.

### Dependency rule
Clean Architecture's rule that code depends only on the rings inside it.

### HAL (hardware abstraction layer)
A set of traits that hide the differences between platforms; each platform
supplies a driver that implements them. Named after Rust's `embedded-hal`.
In this library: the `hal/` module.

### Driver
The code that implements the `hal/` traits for one platform, or the mock
(`drivers/`). Layer: Frameworks & Drivers. *Also called:* backend (the code's
old word, and SimpleBLE's and Qt's), platform implementation, executor
(Nordic). Not the operating system's kernel driver for the Bluetooth chip,
which sits several levels lower.

### Platform
One operating system's Bluetooth API: Linux (BlueZ), Android, Windows, Apple.

### Port
An interface (a Rust trait) a layer defines for what it needs from outside,
so that any implementation, real or mock, can be plugged in.

### Dependency injection
Passing a component the implementations it needs (its ports) from outside,
instead of the component creating them itself.

### Handle
A value that represents something running (a scan, an advertisement, a
connection) and stops it when the value is dropped. *Also called:* guard,
RAII handle.

### RAII
"Resource acquisition is initialization": a Rust (and C++) idiom where
owning a value means owning a resource, and dropping the value releases it.

### Adapter
This library's object for the device's [Bluetooth adapter](#bluetooth-adapter):
whether it is on, events when that changes, and what the hardware can do.
Roles are created from it (ADR-0007 D7). *Also called:* `Adapter` in bluest
and btleplug, `BluetoothAdapter` on Android.

### Mock radio
A software stand-in for Bluetooth used in tests, in one process or shared by
several processes through the *mock broker* (ADR-0004).

### Power profile
Advice on how hard to work the radio (how long to scan, how long to pause,
how many connections), worked out from the device's situation: battery,
charging, foreground or background.

### OSI model
The standard seven-layer model of network protocols: 1 physical, 2 data link,
3 network, 4 transport, 5 session, 6 presentation, 7 application. Each layer
says what a protocol does to the bytes. **`L` with a number always means an
OSI layer** (L2 = data link), never one of this library's layers. Where this
library sits on it: [`architecture.md`](architecture.md#how-the-layers-map-onto-osi).

### Pub/sub (publish/subscribe)
One side *publishes* events or state; any number of *subscribers* receive
them and react on their own, without the publisher knowing who they are.

### Watch channel
Pub/sub for a *value that changes* (`tokio::sync::watch`). It keeps the latest
value, so a new subscriber sees the current state at once. *Also called:*
`StateFlow` (Kotlin).

### Broadcast stream
Pub/sub for a *sequence of events* (`tokio::sync::broadcast` behind a
stream), where every subscriber receives every event published after it
subscribed, unless its bounded buffer overflows.

### Lifecycle event stream
A driver's stream of connection and radio events, without characteristic
writes. See [driver event streams](interface.md#driver-event-streams-and-receive-loss).

### Receive gap
Known loss of messages on a payload receive path (`BleError::ReceiveGap`).
See [driver event streams](interface.md#driver-event-streams-and-receive-loss).

### Cancellation token
A one-shot signal that tasks can wait on or be stopped by
(`tokio_util::sync::CancellationToken`).

### Leftover link
A link BlueZ still holds for a process that exited or crashed. On Linux a
link belongs to `bluetoothd`, not to the app that dialled it (ADR-0009).

### Endpoint hooks
Callbacks an application installs on its iroh endpoint. They run once TLS has
proved the peer's key (ADR-0008).

### Path
One transport address an iroh connection can use to reach the peer.
*Not the same as* a filesystem path.

### Crossed dials
Both devices dialling each other at the same moment, so two links or two
connections come up where one was wanted.

### Symmetric initiation
Either peer can request communication, with no permanent ordering of
identities. Both sides must accept.

### Initiation policy
A retired caller-selected initiator mode. The managed API has no such mode.

### Candidate link
One possible BLE attempt before core arbitration selects the survivor.

### Redial yield
A retired adapter delay that gave the previous initiator time to redial.

### Adapter crate
A crate that connects `ble-gatt` to something outside it, such as iroh
(`ble-gatt-iroh`) or Tauri (`tauri-plugin-ble-gatt`). Layer: Interface
Adapters. Not to be
confused with a [Bluetooth adapter](#bluetooth-adapter).

### Core endpoint identifier
A fresh 128-bit identifier for one worker lifetime (`ble_gatt::EndpointId`).
It correlates discovery and attempts. It is not an iroh `EndpointId`, native
address or authenticated identity.

### Attempt nonce
A fresh signed 32-bit value for one outgoing request, which core arbitration
compares. It is not an encryption key, endpoint identifier or native session
generation.

### Attempt outcome
The result of one establishment attempt, distinct from an established peer's
connection state.

### Control record
A tagged managed record for establishment, acceptance, disconnect or
liveness, apart from application data (ADR-0012).

### Scoped channel use
One `PeerChannel` handle: one use of an established channel generation. See
[messages and closure](interface.md#messages-and-closure).

### Channel close reason
Why a channel ended, set before receive ends. See
[messages and closure](interface.md#messages-and-closure).
