# Glossary

This file separates writing rules from the project's technical vocabulary.
Use one term for each concept. Report conflicts between definitions and code;
a definition does not change implemented behavior.

**Synonyms.** Where another library or platform uses a different word for the
same thing, it is listed as *Also called*. Use the first word in code and
documents.

Current API descriptions and provisional target requirements are distinct.
The target in [ADR-0012](adr/0012-discovery-like-nearby.md) does not rename or
change an existing API by documenting a new responsibility boundary.

## Writing and naming rules

Apply these STE-based rules to new or touched names, comments, logs, documents
and pull request text. The technical terms below form the company glossary;
they define meaning, not sentence structure.

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

These are local writing rules, not verified ASD-STE100 dictionary compliance or
certification. Do not copy the copyrighted ASD dictionary into this repository.

## Bluetooth basics

### BLE (Bluetooth Low Energy)
The low-power part of Bluetooth that this library uses. Classic Bluetooth
(audio, file transfer) is not used.

### Radio
The device's Bluetooth hardware as a physical resource: while it scans or
advertises it draws battery, so every layer treats it as something to hold
briefly.

### Bluetooth adapter
The Bluetooth hardware of one device as the operating system exposes it: it
can be on or off, present or missing. *Also called:* controller, local
adapter, `BluetoothAdapter` (Android), `org.bluez.Adapter1` (Linux).
Not to be confused with an [adapter crate](#adapter-crate).

### Role
What a device does in a BLE exchange. One device can hold several roles at
once. The four roles below are the ones BLE defines; each platform exposes
them as separate APIs.

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
One of those broadcast packets: up to about 31 bytes carrying service UUIDs
and small data (for example the manufacturer data where this library puts a
peer's identity fingerprint). *Also called:* advertising data, adv.

### Advertisement update
Changing what a peripheral advertises while it keeps serving
(`datagram::update_advertisement`). Only the advertisement is replaced; the
GATT server, and every central connected to it with its subscription, stay.
Advertising again with `serve` replaces the server and drops those centrals.
An update survives a [radio recovery](#radio-recovery).

### Stop serving
Ending a peripheral's advertisement and GATT server together
(`datagram::stop_serving`), which drops the centrals connected to it.
Dropping the stream `serve` returned is not stopping: it only ends handing
out new channels, and the server keeps advertising and accepting. A stopped
server stays stopped across a [radio recovery](#radio-recovery).

### Radio recovery
The radio coming back on (`RadioChanged { On }`) after it was off or the
Bluetooth service restarted. The platform has forgotten the advertisement
and GATT server by then, so serving registers again what the driver last
served (`Driver::served_service`), and nothing if serving was stopped
(ADR-0010).

### Scan response
A second packet of up to 31 bytes that an advertiser sends only when a
scanner asks for it. Scanners ask during an *active* scan, which is how both
Android and BlueZ scan. This library puts a peer's local name there, because
the advertisement itself is full. *Also called:* SCAN_RSP.

### Local name
A short, human-readable name a peripheral advertises so a scanner can show
it without connecting (`GattServiceSpec::local_name` → `DiscoveredPeer::name`).
Linux sends it as BlueZ's `LocalName`; Android, which cannot set a name per
advertisement, sends it as service data under the advertised service UUID.
Both go in the scan response. Not the OS device name (the phone's model name).

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
A group of characteristics, identified by a UUID. This library uses one
service per purpose (for example its datagram channel).

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
after connecting (23 bytes at minimum, often a few hundred). Anything larger
is split into fragments.

### Attribute value
The bytes carried by one read, write or notification of a characteristic.
Bluetooth caps it at 512 bytes, whatever the MTU. With an MTU of 517 the
largest message would be 514 bytes, so the 512-byte cap applies first, and
Android refuses a longer write outright (`MAX_ATT_ATTRIBUTE_LEN`).

### Connection priority
A request to the phone's Bluetooth stack to trade battery for speed on one
connection (shorter connection interval). Android only.

### OS pairing (bonding)
The operating system's own Bluetooth pairing, with its system dialog and
stored keys. This library does not use it. *Not the same as* an
application's own pairing, which happens over this library's channels.

### Private address
A Bluetooth address a phone makes up and changes from time to time so it
cannot be tracked. Android does this, which is why a peer cannot be
recognised by its address alone.

### Connectable advertisement
An advertisement a central can answer by connecting. A non-connectable
advertisement only carries data. The current Windows driver uses its GATT
provider advertisement and a readable GATT advertisement characteristic; it
does not publish a second manufacturer-data radio advertisement. ADR-0012
defines shared discovery; ADR-0011 defines Windows-native behavior.

### Peripheral role support
Whether a Bluetooth adapter and its driver can run a GATT server and
advertise it. Not every adapter can. Windows reports it as
`BluetoothAdapter.IsPeripheralRoleSupported`, Android as
`isMultipleAdvertisementSupported`; the driver reports it as
`CapabilityReport::peripheral`.

### Excluded central
A central that `disconnect_peer` refused but the platform cannot drop.
Windows has no call that disconnects a central from a GATT server, so the
Windows driver stops notifying it and refuses its writes until it leaves by
itself (ADR-0011, native events and cleanup limits).

## This library's concepts

### Fast advertisement
Advertisement data that travels in the radio advertisement itself, so a
scanner reads it without connecting. Android and Linux peers can send it.
The term comes from Google Nearby (ADR-0012).

### GATT advertisement
The same advertisement data, served in a readable characteristic
(`advertisement::GATT_ADVERTISEMENT_CHARACTERISTIC`) of every datagram
service. A scanner reads it when a peer's radio advertisement carries no
profile data (`advertisement::complete`, ADR-0012). Select completion from received
information, not the OS name. Nearby also retrieves full metadata through GATT;
this library does not implement its exact header/hash protocol.

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
This library's handle on one connection, which tracks whether it is
connecting, connected or gone (`PeerLink`, ADR-0005). The code still uses
the word; its current module is `connection/peer_link.rs`. *Not the
same as* OSI L2 (data link) or the BLE link layer.

### Driver session
The optional connection-generation identifier in `GattEvent` and
`GattConnection::session()`. It distinguishes successive GATT connections to
the same address and protects a replacement from stale events. It does not
mean an authenticated QUIC connection or an application conversation.

### GAP (Generic Access Profile)
The part of the Bluetooth Core Specification that defines the roles
(central, peripheral, broadcaster, observer) and how devices find and
connect to each other. The `roles/` module is named after it.

### Profile
In the Bluetooth specification, a set of GATT services and the rules for
using them for one purpose. This library's datagram channel is such a
profile; it lives in `profile/`. *Not the same as* a
[power profile](#power-profile).

### Datagram channel
A message channel over a connection: whole messages in, whole messages out,
however small the MTU (`datagram`). Each message is split into fragments and
put back together on the other side.

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
Clean Architecture's rule that code depends only inwards: Use Cases never
call a driver directly, they call a trait the driver implements.

### HAL (hardware abstraction layer)
A set of traits that hide the differences between platforms; each platform
supplies a driver that implements them. Named after Rust's `embedded-hal`.
In this library: the `hal/` module.

### Driver
The code that implements the `hal/` traits for one platform, or the mock
(`drivers/`): the `hal::Driver` trait, implemented by `LinuxDriver`,
`AndroidDriver`, `WindowsDriver` and `MockDriver`. Layer: Frameworks & Drivers. *Also
called:* backend (the code's old word, and SimpleBLE's and Qt's), platform
implementation, executor (Nordic). Not the operating system's kernel driver
for the Bluetooth chip, which sits several levels lower.

### Platform
One operating system's Bluetooth API: Linux (BlueZ), Android, Windows, Apple.

### Port
An interface (a Rust trait) a layer defines for what it needs from outside,
so that any implementation, real or mock, can be plugged in. The ports
drivers implement live in `hal/`.

### Dependency injection
Passing a component the implementations it needs (its ports) from outside,
instead of the component creating them itself. This is what lets a test give
the library a mock radio.

### Handle
A value that represents something running (a scan, an advertisement, a
connection) and stops it when the value is dropped. *Also called:* guard,
RAII handle.

### RAII
"Resource acquisition is initialization": a Rust (and C++) idiom where
owning a value means owning a resource, and dropping the value releases it.
A `MutexGuard` unlocking when it goes out of scope is the everyday example.

### Adapter
This library's object for the device's [Bluetooth adapter](#bluetooth-adapter):
whether it is on, events when that changes, and what the hardware can do.
Roles are created from it (ADR-0007 D7). *Also called:* `Adapter` in bluest
and btleplug, `BluetoothAdapter` on Android.

### Mock radio
A software stand-in for Bluetooth used in tests: in one process
(`MockNetwork`), or shared by several processes through the *mock broker*
(ADR-0004).

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
Fini's `DataLink` is named after OSI L2.

### Pub/sub (publish/subscribe)
One side *publishes* events or state; any number of *subscribers* receive
them and react on their own, without the publisher knowing who they are.
The style used across `ble-gatt` and Fini for state and events.

### Watch channel
`tokio::sync::watch`: pub/sub for a *value that changes*, such as whether the
adapter is on or a connection's state. It keeps the latest value, so a new
subscriber sees the current state at once. Kotlin's `StateFlow` is the same
idea.

### Broadcast stream
Pub/sub for a *sequence of events* (`tokio::sync::broadcast` behind a
stream), where every subscriber receives every event published after it
subscribed.

### Cancellation token
`tokio_util::sync::CancellationToken`: a one-shot signal that tasks can wait
on or be stopped by. A connection hands one out that fires when it
disconnects.

### Leftover link
A link BlueZ still holds for a process that exited or crashed: on Linux a
link belongs to `bluetoothd`, not to the app that dialled it. Android, Apple
and Windows release a dead app's links themselves. The Linux driver
disconnects leftover links that offer the app's service before its first
scan or advertisement, and `Adapter::close` releases the app's own links on
the way out (ADR-0009).

### Endpoint hooks
Callbacks an application installs on its iroh endpoint
(`iroh::endpoint::EndpointHooks`). `after_handshake` runs for every
connection, incoming and outgoing, once TLS has proved the peer's key
(ADR-0008).

### Path
One transport address an iroh connection can use to reach the peer
(`Connection::paths`). For `ble-gatt-iroh` it is the peer's BLE address.
*Not the same as* a filesystem path.

### Crossed dials
Both devices dialling each other at the same moment, so two links or two
connections come up where one was wanted.

### Symmetric initiation
Either peer may start or reconnect a link, regardless of permanent identity
ordering. This is the high-level core contract under review in ADR-0012,
independent of iroh and without a caller-selected initiation mode.
In current code, `ble-gatt-iroh::InitiationPolicy::EitherPeer` selects the
no-yield behavior, whereas core `PeerLink::LinkRole::Symmetric` selects a
lower-identity dialler. The names are not synonymous, and neither capability
declaration proves simultaneous-role hardware support. The target refactor
has not happened yet.

### Initiation policy
The current adapter's explicit choice of who may start an outgoing link when no usable queue
exists. In `ble-gatt-iroh`, `PreviousDialer` preserves the historical redial
yield, `EitherPeer` removes that yield and `Ordered` is the smaller-endpoint-key
fallback. It is not peer authentication or an application's session policy.
ADR-0012 removes this public choice from the target design; it does not merely
change the default. Existing enum variants remain implemented until refactored.

### Candidate link
A BLE channel attached to the iroh transport, potentially one of several
crossed candidates. Older candidates can still receive while the newest queue
sends. An address with live iroh users remains protected from idle cleanup;
last-user release closes it when those users end (current adapter behavior;
see ADR-0012's implementation gap).

### Redial yield
After a link the peer dialled drops, the time (`REDIAL_YIELD`, 30 s) for
which `ble-gatt-iroh` does not dial that peer, so that the peer, which re-dials
for its still-open iroh connection, does not cross with it. It starts only
when such a connection may still be open: not after this side closed the
link, nor after every connection on it had started to close. It applies to
`InitiationPolicy::PreviousDialer` (the compatibility default), not to
`EitherPeer` or `Ordered` (current implementation, not ADR-0012's target).

### Adapter crate
A crate that connects `ble-gatt` to something outside it, such as iroh
(`ble-gatt-iroh`) or Tauri (`tauri-plugin-ble-gatt`). Layer: Interface
Adapters. Not to be
confused with a [Bluetooth adapter](#bluetooth-adapter).

### Core endpoint identifier
In the ADR-0012 target, the protocol identifier that correlates discovery and
incoming/outgoing attempts for one peer context. Not an iroh `EndpointId`, a
native address or proof of authenticated identity. Its representation and exact
lifetime remain review gates; the handshake is not implemented yet.

### Attempt nonce
A fresh random value associated with an outgoing connection attempt. Nearby's
reference uses a signed 32-bit value; the target follows its larger-wins,
equal-values-fail-both rule. It is not an encryption key or a native generation.

### Attempt outcome
The result of one establishment attempt, distinct from an established peer's
connection state. In the ADR-0012 target, an outgoing tie-break loser reports
failure/rejection while the incoming winner proceeds separately. It is not a
silent success handoff and does not mean a healthy peer was globally disconnected.

### Control record
A protocol record for establishment, acceptance or liveness, explicitly distinct
from application payload. ADR-0012 requires bounded, versioned handling without
sniffing QUIC or arbitrary payload prefixes; its encoding is not selected yet.
