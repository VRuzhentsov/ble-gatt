# 0011 — Windows driver: WinRT central and peripheral

## Status

Proposed. The implementation is in `ble-gatt/src/drivers/windows/`.
**D4 is replaced by ADR-0012**: the Windows driver no longer runs a second
advertisement; manufacturer data and the name reach scanners through the
GATT advertisement. D5 still holds where the name fits.

It builds for `x86_64-pc-windows-msvc`, its tests pass, and clippy
reports no warnings. On the test computer (Intel Wireless Bluetooth,
driver 23.90.0.8, Windows 11) `capabilities()` reports
`central: true, peripheral: true` and `radio_status()` reports `On`
(hardware check 1), and the GATT server advertises once the driver waits
out the early `Aborted` status the provider reports first.
**No link is verified yet.** The bar is the same
as for Android (ADR-0002): a Fini pairing and a sync between a Windows
computer and an Android phone, in both roles. The checklist is in
"Hardware verification" below.

## Context

Fini runs on Windows. Its `src-tauri/Cargo.toml` pulls `ble-gatt` only for
Linux and Android, and uses a stub on Windows, so a Windows computer cannot
pair with a phone over Bluetooth. The Windows driver was reserved as M2.

Fini needs both roles on every platform:

- **Central.** Scan for Fini's service UUID, read the manufacturer data
  (company `0xFFFF`: one flags byte for Add Device mode, then the identity
  fingerprint) and the name, then connect, write and subscribe.
- **Peripheral.** Serve the datagram service, advertise it with that
  manufacturer data and the short name, receive writes and send
  notifications to one central at a time.

WinRT (`Windows.Devices.Bluetooth`) has both roles. `docs/alternatives.md`
says to read btleplug's and bluest's WinRT code before writing ours. Both are
central only.

## Decisions

### D1 — Use Microsoft's `windows` crate directly

Options:

1. **btleplug** (`btleplug::platform::Manager`). Mature Windows central.
   Example: `adapter.start_scan(ScanFilter { services: vec![uuid] })`, then
   `peripheral.connect()`. It has no peripheral role, so the peripheral half
   still needs WinRT directly, and the driver would mix two event models
   (btleplug's `CentralEvent` stream and WinRT handlers). It also adds its
   own dependency tree.
2. **bluest** (`bluest::Adapter::default()`). Same shape, same gap: central
   only.
3. **The `windows` crate** (MIT OR Apache-2.0), the WinRT projection that
   btleplug and bluest are built on. Example:
   `GattServiceProvider::CreateAsync(guid)?.await?`. One event model for
   both roles, and the driver maps WinRT types to `crate::entities` itself,
   like the Linux driver does with `bluer`.

**Decided: option 3, with the central half adapted from btleplug's
code.** btleplug's `src/winrtble/` is BSD-3-Clause, so its code may be
copied with its notice (`LICENSES/btleplug-BSD-3-Clause.txt`). The driver
takes btleplug's module split and its central-role fixes, and keeps this
library's own `Driver` contract and peripheral role on top:

| Module | Job | Source |
|---|---|---|
| `drivers/windows/watcher.rs` | Scanning | btleplug `ble/watcher.rs`: active scanning, extended advertisements, scan responses only from addresses that matched, a bounded set of 1024 matched addresses |
| `drivers/windows/device.rs` | One central link | btleplug `ble/device.rs`, `ble/characteristic.rs`: uncached discovery falls back to the Windows cache after 5 s (btleplug #325: some drivers never answer), link loss from `ConnectionStatusChanged`, Notify or Indicate from the characteristic's properties, one `ValueChanged` handler per characteristic, the status of `RequestPreferredConnectionParameters` checked |
| `drivers/windows/peripheral.rs` | GATT server, advertising | Own. btleplug has no peripheral role; the provider-plus-publisher split matches tinygo-org/bluetooth (D4) |
| `drivers/windows/utils.rs` | Conversions | Partly btleplug (`parse_service_data`) |
| `drivers/windows/mod.rs` | The `Driver` port: sessions, events, radio | Own |

Two choices differ from btleplug and follow bluest instead: the dial uses
the address type the scan saw (`FromBluetoothAddressWithBluetoothAddressTypeAsync`),
and the link keeps `GattSession.MaintainConnection = true`.

Using btleplug as a dependency (option 1) stays possible later, once the
`hal::Driver` port is split by role; that is a separate decision.

Version: `windows = "0.62"`, the version btleplug 0.13 uses. Its async
operations implement `IntoFuture`, so they `.await` on Tokio with no
executor bridge.

### D2 — What the computer must support, and how `CapabilityReport` says it

The driver reads the default `BluetoothAdapter`:

| Field | WinRT source |
|---|---|
| `central` | `IsLowEnergySupported && IsCentralRoleSupported` |
| `peripheral` | `IsLowEnergySupported && IsPeripheralRoleSupported` |

Both are `false` when there is no adapter. `WindowsDriver::new` fails with
`AdapterUnavailable` when there is no adapter or it has no Bluetooth LE, so
`PeerLink` reports the radio as unavailable.

Requirements:

- **Radio:** a Bluetooth 4.0 or later adapter with Bluetooth LE.
- **Driver:** for the peripheral role, a driver that reports
  `IsPeripheralRoleSupported`. Many older or generic adapters report
  `false`. Current adapters from the large vendors usually report `true`,
  but only the adapter can say. The test computer has an Intel Wireless
  Bluetooth adapter, driver 23.90.0.8; `capabilities()` on it is the first
  hardware check.
- **Two advertisements at once** (D4). The controller must run the
  connectable and the non-connectable advertisement together. Windows
  interleaves them when the controller has one advertising set; a publisher
  that stays `Waiting` means it could not. The driver logs that and does not
  fail.
- **Windows:** Windows 10 version 2004 (build 19041) or later, for
  `GattServiceProviderAdvertisingParameters.ServiceData`, the address type in
  scan results, and extended advertisement scanning. On Windows 11,
  `request_connection_priority` uses
  `BluetoothLEDevice.RequestPreferredConnectionParameters`; on Windows 10 it
  returns `Unsupported`, which callers already treat as a no-op.
- **Settings:** Bluetooth must be on, and the Windows privacy setting for
  radios must let apps use the radio. A scan that Windows refuses
  ends with `AdapterUnavailable`.

The `RadioChanged` event comes from `Radio.StateChanged`; `radio_status()`
reads `Radio.State`.

### D3 — Peer addresses

`PeerAddress` is `"AA:BB:CC:DD:EE:FF"`, the same form as the other drivers.
The driver remembers each address's `BluetoothAddressType` from the scan
and dials with `FromBluetoothAddressWithBluetoothAddressTypeAsync`. Android
advertises from a random [private address](../glossary.md#private-address);
dialled as public, it is not found.

A GATT server sees a central only through its `GattSession`. The driver
takes the remote address from the session's device id
(`BluetoothLE#BluetoothLE<local>-<remote>`). When the id has another form,
the whole id is the address, which still routes `notify_peer` correctly.

### D4 — Manufacturer data goes in a second advertisement

*Replaced by ADR-0012.* Measured on the test computer: the publisher
refuses a service UUID list (`E_INVALIDARG`), so scanners that filter on
the UUID never received it, and with a 128-bit UUID the name did not fit
in the provider's advertisement either. The text below records the first
design.

`GattServiceProvider.StartAdvertising` sends a connectable advertisement
with the service UUID and, from build 19041, one service data entry under
that UUID. It cannot send manufacturer data. Fini's scanner ignores a peer
with no fingerprint in manufacturer data, so Windows would never be found.

Options:

1. **Provider only.** Simple, but Fini cannot discover Windows. Changing
   Fini to read the fingerprint from service data would also move the name
   (D5) and change every platform.
2. **Publisher only** (`BluetoothLEAdvertisementPublisher`). Carries any
   data, but its advertisement is not connectable, so no central can reach
   the GATT server.
3. **Both.** The provider sends the connectable advertisement (UUID and the
   name). A publisher sends a non-connectable advertisement with the UUID and
   the manufacturer data. Example:
   `advertisement.ManufacturerData().Append(BluetoothLEManufacturerData::Create(0xFFFF, &buffer)?)`.
   The two are independent WinRT objects, so each can be replaced
   without touching the other. tinygo-org/bluetooth (`gatts_windows.go`,
   `gap_windows.go`, BSD-3-Clause) does the same.

**Decided: option 3.** The publisher carries no service data and no name:
tinygo records that Windows refuses to start a publisher with either.
Service data under UUIDs other than the served one is therefore not sent
on Windows (logged). If Windows refuses the publisher with the service
UUID, the driver starts it with the manufacturer data alone and logs that
filtering scanners will miss it.

Open risk: a scanner merges the two advertisements only if both come from
the same address. FlyingCarpet's field guide (GPL-3.0, read for facts only)
records that `GattServiceProvider` advertises from a static random address;
which address the publisher uses is not documented. If the two differ, a
phone sees two devices: one with the fingerprint that cannot be connected,
and one that can be connected without it. Hardware check 4 settles this.
The fallback, if needed, is to send Fini's flags and fingerprint in the
provider's own service data on Windows; that changes the advertisement
format on every platform and is a separate decision.

`update_advertisement` replaces only the publisher when only the
manufacturer data changed, which is Fini's Add Device toggle. When the name
changes, it also restarts the provider's advertisement. Whether that keeps
the connected centrals is hardware check 6.

### D5 — The name travels as service data, like on Android

The provider's advertisement cannot set a local name per advertisement.
The driver sends `local_name`, cut to `NAME_SERVICE_DATA_MAX_LEN`, as service
data under the served UUID: the same encoding as the Android driver.
`advertised_name` already reads it there, so Linux and Android scanners
show the name with no change. As a scanner, the Windows driver reads both
forms.

### D6 — `disconnect_peer` excludes a central instead of dropping it

WinRT has no call that disconnects a central from a GATT server. Options:

1. Return `Unsupported`. Honest, but `datagram::serve` then cannot refuse a
   second central at all.
2. **Exclude it.** Mark the central as excluded until its `GattSession`
   closes: `notify` skips it, `notify_peer` refuses it, and its writes get
   ATT error `0x03` (Write Not Permitted) and are not published.

**Decided: option 2.** The refused central stays connected until it gives
up, but it receives none of the served peer's data. This keeps the
guarantee that `disconnect_peer` exists for. It is a glossary
term: [excluded central](../glossary.md#excluded-central).

### D7 — Server-side order and identity

WinRT raises `WriteRequested` on its thread pool. The handler only queues
the request; one Tokio task answers the requests in arrival order and
publishes `CharacteristicWritten`, so the fragments of a datagram stay in
order. `Connected { Peripheral }` is emitted when a central subscribes
(`SubscribedClientsChanged`) and before each write, with a session minted at
first sight, as the Android driver does. `Disconnected { Peripheral }` comes
from `GattSession.SessionStatusChanged` = `Closed`.

Reads of a readable characteristic are answered by Windows from
`StaticValue` (`initial_value`). `notify_max_len` is
`GattSubscribedClient.MaxNotificationSize`.

### D8 — Central links

`connect` opens a `GattSession` with `MaintainConnection = true` and
discovers every service and characteristic uncached, falling back to the
Windows cache after 5 s (D1); discovery is where WinRT really connects.
Link loss is `BluetoothLEDevice.ConnectionStatusChanged` to `Disconnected`
(as in btleplug) or `GattSession.SessionStatusChanged` to `Closed`,
whichever comes first, reported once as `Disconnected { Central }`. `att_mtu` is
`GattSession.MaxPduSize`. A second `connect` to an address with a live or
pending link is refused, as on Android.

### D9 — Threads and `Send`

WinRT handlers run on WinRT threads, so they capture a `Weak<Inner>` and do
no blocking work. Some `windows` types are not `Send` (`IBuffer`, event
handler objects); the driver never holds them across an `.await`, because
`async_trait` futures must be `Send`.

## Hardware verification

Run on the Windows computer with an Android phone running Fini:

1. `capabilities()` reports `central: true, peripheral: true` on the Intel
   adapter.
2. Windows scans and finds the phone with its fingerprint and name.
3. Windows connects to the phone, writes, subscribes and receives
   notifications (central role).
4. The phone finds Windows through `advertisement::complete`: one
   `DiscoveredPeer` with the manufacturer data and the name read from the
   GATT advertisement (ADR-0012).
5. The phone connects to Windows, writes, and receives notifications
   (peripheral role).
6. Add Device toggles (`update_advertisement`) while the phone stays
   connected.
7. Bluetooth off and on: `RadioChanged` both ways, serving recovers
   (ADR-0010).
8. A second central is excluded (D6) and gets none of the first one's data.

## Consequences

- Fini can depend on `ble-gatt` on Windows: add the `ble-gatt` dependencies
  for `cfg(target_os = "windows")` in `src-tauri/Cargo.toml` and remove the
  Windows stub in `src-tauri/src/services/mod.rs` (separate Fini change).
- `drivers::platform()`, `PeerLink::new`, `Adapter::platform` and
  `tauri-plugin-ble-gatt`'s default driver now include Windows.
- The driver builds natively with the MSVC toolchain, or from Linux with
  `cargo xwin build --target x86_64-pc-windows-msvc`.
