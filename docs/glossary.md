# Glossary

What the words in `ble-gatt` mean. This library is meant for many projects and
developers, and Bluetooth words are used loosely across the industry, so this
file fixes one meaning for each. When the code or a document disagrees with
this file, this file wins.

**Synonyms.** Where another library or platform uses a different word for the
same thing, it is listed as *Also called*. Use the first word in code and
documents.

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

## This library's concepts

### Peer
Another device this library talks to.

### Peer address
The value a backend uses to reach a peer right now (`PeerAddress`). It is
opaque and may change (see [private address](#private-address)), so it is not
a peer's identity.

### Peer identity
What recognises a peer across address changes: a fingerprint the peer puts in
its advertisement.

### Link
This library's handle on one connection, which tracks whether it is
connecting, connected or gone (`PeerLink`, ADR-0005).

### Datagram channel
A message channel over a connection: whole messages in, whole messages out,
however small the MTU (`datagram`). Each message is split into fragments and
put back together on the other side.

### Fragment, reassembly
A *fragment* is one MTU-sized piece of a message; *reassembly* is joining the
pieces back into the message.

## Code structure

### Layer
One level of the library with one job, L0 to L5; see
[`architecture.md`](architecture.md).

### Platform
One operating system's Bluetooth API: Linux (BlueZ), Android, Windows, Apple.

### Backend
The code that implements this library's interfaces for one platform, or the
mock. *Also called:* platform implementation, driver, executor (Nordic).

### Port
An interface (a Rust trait) a layer defines for what it needs from the layer
below, so that any implementation, real or mock, can be plugged in.

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

### Layer number
L0 to L5 in [`architecture.md`](architecture.md). These are this library's
own layers, **not** the OSI model's: here L2 is one BLE connection, while
OSI's layer 2 (data link) is what this library's L3 datagram channel does.
Fini's `DataLink` is named after the OSI layer.

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

### Adapter crate
A crate that connects `ble-gatt` to something outside it, such as iroh
(`ble-gatt-iroh`) or Tauri (`tauri-plugin-ble-gatt`). Layer L4. Not to be
confused with a [Bluetooth adapter](#bluetooth-adapter).
