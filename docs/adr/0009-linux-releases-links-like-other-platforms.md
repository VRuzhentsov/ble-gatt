# 0009 — On Linux, the driver releases links the way other platforms do

## Status

Accepted. Implemented in `drivers/linux.rs` and `tauri-plugin-ble-gatt`.

## Context

A desktop app using `ble-gatt` was killed while it held a link to a phone.
BlueZ kept that link (`Device1.Connected = true`, at the phone's old
address, offering the app's service). Every later dial to the phone's new
address failed with `le-connection-abort-by-local` until the adapter was
power-cycled.

Who owns a link differs by platform:

| Platform | Owner of a link | When the app dies |
|---|---|---|
| Android | each app's `BluetoothGatt`; the stack counts users per link | the system closes the dead app's clients ("appDied"); the link drops if nobody else holds it |
| iOS / macOS | each app's `CBCentralManager`; apps can share one link | the system ends the dead app's connections |
| Windows | each app's `BluetoothLEDevice` handles | "disposing of all references … (and if no other app on the system has a reference to the device) will trigger an automatic disconnect" (Microsoft, GATT client docs) |
| Linux (BlueZ) | `bluetoothd`, system-wide; `Device1.Connect` has no per-client owner | nothing: the link stays until something calls `Disconnect` or the adapter goes down |

BlueZ does drop a client's GATT application and advertisement when the
client leaves D-Bus, so only links the app dialled survive it.

What libraries that run on BlueZ do:

- **bleak** (Python, MIT): reuses a device BlueZ already reports connected
  instead of calling `Connect` ("The BlueZ backend does not disconnect
  devices when the application closes or crashes"), and sends `Disconnect`
  as a best effort when its event loop dies.
- **SimpleBLE** (C++): reuses a device that is already connected with its
  services resolved; its destructor does not disconnect.
- **btleplug** (Rust, through `bluez-async`): calls `Connect` regardless and
  cleans up nothing.

None of them sweeps up links at startup. Reusing the link would not have
helped here: the leftover link was to the phone's old address and the dial
went to its new one.

## Decisions

**D1 — Release on exit.** New `Adapter::close()`, backed by
`Driver::close()`: stop serving and disconnect every link this process
dialled, waiting a bounded time for each. On Linux it does that work; the
other drivers keep the default, which does nothing, because their platforms
release a dead app's links themselves. `tauri-plugin-ble-gatt` calls it on
Linux when the app exits (`RunEvent::Exit`). It cannot run on `kill -9` or a
crash. It does not run on Android: there Tauri sends `Exit` whenever the
last activity is destroyed, also while the process keeps running and the
system starts a new activity (seen on hardware about 3 s after launch).

**D2 — Sweep at startup.** The first time the Linux driver scans for or
advertises a service, before it starts, it disconnects every device BlueZ
reports connected that offers that service and that this process neither
dialled nor serves. That is what Android does for a dead app, done by hand.
It covers `kill -9` and crashes, which D1 cannot. Running it before the first
advertisement means no central can have connected to this process yet.

*Risk.* Another program on the same machine using the same service UUID
loses its link once, when this one starts. Service UUIDs of this library's
users are private (Fini's is), so that should not happen in practice. It is
logged as a warning naming the address.

*Alternatives.* Reusing a connected device, as bleak and SimpleBLE do, fixes
only a dial to the address that is still connected. Doing nothing and
documenting `bluetoothctl disconnect` leaves every user of a crashed app
with the dead end.

## Consequences

- Linux behaves like the other platforms for a crash and a normal exit.
- An application not built on the Tauri plugin calls `Adapter::close()`
  itself before it exits.
- The first scan or advertisement for a service after a crash waits for the
  sweep's disconnects (BlueZ took about 2 s per disconnect on hardware).
- Not covered by CI: the mock radio has no BlueZ. Hardware decides.
