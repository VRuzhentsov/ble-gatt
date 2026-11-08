# 0012 — Discovery like Google Nearby: fast advertisement and GATT advertisement

## Status

Accepted. Implemented in `hal/` (`Driver::set_characteristic_value`),
`profile/advertisement.rs`, `profile/datagram/` and every driver. Updates
the discovery design of ADR-0001 and ADR-0007 and replaces ADR-0011 D4.

This is the direction for discovery from now on: when a platform limits
what an advertisement can carry, this library does what Google Nearby
Connections does on that platform, and records here where it differs.

## Context

Fini, and the datagram profile it uses, finds a peer from its
advertisement alone: the service UUID, manufacturer data (company `0xFFFF`:
an Add Device flag and a 4-byte identity fingerprint) and a short name.
Android's `BluetoothLeAdvertiser` and BlueZ send all of that in one
connectable advertisement. The other platforms cannot:

| Platform | Connectable advertisement can carry | Source |
|---|---|---|
| Android, Linux | Anything that fits: UUIDs, manufacturer data, service data, name | Today's drivers |
| Windows | The service UUID and one service data entry (`GattServiceProvider`). With a 128-bit UUID even 9 bytes of service data do not fit. The `BluetoothLEAdvertisementPublisher` carries manufacturer data but is not connectable and refuses a service UUID list (`E_INVALIDARG`) | Measured on the test computer, 2026-11-08; Microsoft API reference |
| Apple | Only the service UUIDs and the local name | `CBPeripheralManager` documentation; bitchat `docs/SERVICE-UUID-DETECTABILITY.md` |

So a phone cannot discover a Windows computer running Fini, and later an
Apple device, from the advertisement alone.

Research into how others solve this on Windows (`docs/alternatives.md`
lists the projects):

- **Google Nearby Connections** (`google/nearby`, Apache-2.0), which Quick
  Share for Windows is built on. Its BLE advertisement carries a small
  header in service data under Google's 16-bit UUID `0xFEF3`: version,
  number of slots, a bloom filter of service IDs, a hash of the
  advertisement, an L2CAP PSM. The full advertisement is in GATT
  characteristics, which a scanner reads after it connects and caches by
  the hash (`ble_advertisement_header.h`, `discovered_peripheral_tracker.h`).
  Version 1 put everything in service data; version 2 moved it to GATT.
  Small payloads still go out whole as a "fast advertisement".
- **FlyingCarpet** (Rust, Windows↔Android/iOS): advertises only its
  service UUID on Windows; everything else is read from characteristics.
- **qaul** on iOS, **bitchat** on iOS: UUID (and name) only; the identity is
  read from a characteristic or exchanged after connecting.
- **universal_ble** and **ble_peripheral** (Flutter): document or show that
  Windows cannot combine service UUIDs with manufacturer data.

## Decisions

### D1 — Two paths, as in Nearby

1. **Fast advertisement.** Where the platform allows it, the advertisement
   data travels in the radio advertisement as before. A scanner needs no
   connection. Android and Linux keep doing this; nothing changes between
   them.
2. **GATT advertisement.** Every service `datagram::serve` serves also has
   a readable characteristic, `GATT_ADVERTISEMENT_CHARACTERISTIC`, whose
   value is the same advertisement data. A scanner that finds a peer whose
   radio advertisement has no manufacturer data and no service data
   connects, reads it, disconnects, and merges it into the `DiscoveredPeer`
   (`advertisement::complete`). A consumer sees the same fields either way.

Options considered:

| Option | Example | For | Against | Who does it |
|---|---|---|---|---|
| A. GATT advertisement only | every scanner connects to every peer | one path | every Android↔Android discovery gets slower | FlyingCarpet, qaul on iOS |
| B. All in service data under a 16-bit UUID | `0xFE2C` with an account-key filter | no connection | the UUID must be bought from the Bluetooth SIG; the name still does not fit on Windows; Apple cannot send service data | Fast Pair, Exposure Notifications, Berty (unassigned alias) |
| C. Two advertisements, merged by address | provider + non-connectable publisher | no connection | depends on both using one address (not documented); Apple cannot | tinygo-org/bluetooth |
| **D. Fast advertisement + GATT advertisement** | this ADR | no change where the platform allows it; works on Windows and Apple | two paths | **Google Nearby** |

**Decided: D**, because it is what Nearby ships on the same four
platforms.

### D2 — The GATT advertisement format

One version byte (`1`), then advertising data structures as the Bluetooth
Core Specification Supplement, Part A defines them: `[length][AD type][data]`.
Written types: manufacturer specific data (`0xFF`), service data with a
128-bit UUID (`0x21`), complete local name (`0x09`). Read types add 16- and
32-bit service data. Unknown types are skipped, so a later version can add
fields (for example Nearby's advertisement hash) without breaking readers.
The value stays within the 512 bytes of one ATT attribute; an entry that
does not fit is left out and logged.

Reusing the AD format means one parser for both paths, and the name is not
cut to the 13 or 29 bytes a radio advertisement holds.

### D3 — `Driver::set_characteristic_value`

The GATT advertisement changes when the advertisement changes (Fini's Add
Device flag). A new port method replaces the value reads return, without
touching the server, its centrals or their subscriptions:

| Driver | How |
|---|---|
| Linux | The read handler already answers from a value map; the entry is replaced |
| Android | Kotlin `setCharacteristicValue` sets `characteristic.value`, which `onCharacteristicReadRequest` returns. Bridge ABI v5 |
| Windows | `ReadRequested`, answered from a value map, replaces `StaticValue` |
| Mock | The value map of the mock peripheral; the broker wire has a request for it |

It has no default, for the reason ADR-0010 gave `served_service` none: a
wrapping driver that forgot it would serve a stale value without an error.
`datagram::update_advertisement` calls it after the driver's
`update_advertisement`. `GattServiceSpec::same_service` no longer compares
`initial_value`, since values now change while serving.

A read is not a sign of a datagram peer, so no driver emits
`Connected { Peripheral }` for one.

### D4 — When and how often a scanner reads

`advertisement::complete` reads a peer when its radio advertisement has
neither manufacturer data nor service data, which is how a Windows or Apple
peer looks. Reads run one at a time, each bounded by a timeout, so a crowd
of peers cannot use up the platform's connection slots. A peer is read
again every `refresh` (default 10 s) while the scan runs, and reported
again only when the data changed; after `max_failures` failed reads in a
row it is no longer read.

Here the design differs from Nearby. Nearby learns that an advertisement
changed from the hash in its header and reads only then. A header needs
service data, which does not fit next to a 128-bit UUID on Windows, so
this library reads again on a timer. Fini sees an Add Device flag change on
a Windows peer up to `refresh` late.

### D5 — The Windows driver advertises only what the provider can

The Windows driver no longer runs the separate manufacturer data publisher
of ADR-0011 D4: the GATT advertisement carries that data, and the risk of
the two advertisements using different addresses goes away. The provider
still sends the name as service data when it fits (a 16-bit service UUID);
with a 128-bit UUID Windows leaves it out
(`StartedWithoutAllAdvertisementData`) and the GATT advertisement carries
it.

### D6 — The future

- **Apple (M3)** uses the GATT advertisement from the start: its
  advertisement can only carry UUIDs and a name.
- **A header with a hash**, as in Nearby, once a service uses a 16-bit
  UUID: the scanner then reads only when the hash changes, and D4's timer
  goes. The version byte (D2) leaves room for it.
- **Bonded or known peers** can skip the read: an application that already
  knows a peer's address can map it to its identity without reading.

## Consequences

- A Windows computer serving the datagram profile can be found by Android,
  Linux and Windows scanners that use `advertisement::complete`.
- Fini must scan through `advertisement::complete` (or read the GATT
  advertisement itself) and must forward `set_characteristic_value` in its
  `SwitchedDriver`; it does not compile against this version otherwise.
  That change is in Fini, not here.
- An application vendoring `BleGattBridge.kt` must take the v5 file; the
  Android driver logs a loud error on an ABI mismatch.
- Every datagram service has one more characteristic. Older peers ignore
  it.
- Anyone who connects can read the advertisement data, as anyone in range
  can already receive it over the radio.
- Tests: `tests/gatt_advertisement.rs` (mock radio) and the format tests in
  `profile/advertisement.rs`. Not verified on hardware: a phone reading a
  Windows peer's GATT advertisement, and the Android and Linux read path of
  `set_characteristic_value`.
