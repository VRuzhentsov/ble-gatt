# 0011: Windows driver, WinRT central and peripheral

## Status

PR #38 contains the Windows implementation and native ownership fixes.
Native cross-device acceptance remains pending. Software checks are not physical proof.
This ADR covers Windows-native choices; [ADR-0012](0012-discovery-like-nearby.md)
covers the implemented core protocol. The PR remains draft.

## Decision question

How does Windows implement the same central/peripheral contracts as Linux,
Android and mocks without introducing a separate peer protocol or lifecycle?

## Context and constraints

A reusable BLE carrier needs GATT client and server operations, platform
capabilities, events and correctly scoped cleanup. Platform role support is a
runtime fact; an OS name does not prove peripheral or crossed-attempt support.
Applications such as Fini consume this contract but do not define the driver.

Use the GATT provider, not the earlier unmerged manufacturer-publisher proposal.
Shared metadata completion handles information omitted from radio advertisements.
A publisher now carries only the managed fast advertisement next to the
provider (§3, amended with hardware evidence).

## Decision

### 1. One driver, the existing HAL contracts

Use the `windows` crate's WinRT APIs and implement `Driver` / `GattConnection`.
Keep WinRT types, callback tokens, address conversion and native session state
behind that boundary. Windows joins the existing platform factory and wrappers.

```text
Shared core operations / Adapter role access
                    |
          Driver / GattConnection
                    |
       WindowsDriver / WinRT objects
            /                 \
central: watcher, device     peripheral: GATT provider
```

The current module responsibilities are:

- `drivers/windows/watcher.rs`: advertisements and scan filtering/cache.
- `drivers/windows/device.rs`: outgoing GATT client/session operations.
- `drivers/windows/peripheral.rs`: server requests, values and advertising.
- `drivers/windows/utils.rs`: conversions and advertisement byte helpers.
- `drivers/windows/mod.rs`: HAL implementation, session/event coordination.

The central implementation adapts btleplug code under BSD-3-Clause. Preserve
its source notices and [license](../../LICENSES/btleplug-BSD-3-Clause.txt).
The peripheral implementation uses WinRT directly.

### 2. Capabilities and addresses

Read the adapter's BLE, central-role and peripheral-role capabilities. Report
missing radio/support through the existing error and capability vocabulary.
Keep radio-state observation in the driver. The existing Windows implementation
targets Windows 10 version 2004 or later; check the actual APIs and device driver
when validating a supported machine.

Preserve the address type seen during scanning when opening a client connection.
A native address locates a device; it is not an authenticated identity or the
core endpoint identifier used for the new tie-break.

Central/peripheral support reported separately does not certify simultaneous
roles, crossed establishment, background execution or every adapter model.

### 3. Provider advertising and readable metadata

Use `GattServiceProvider` for connectable service advertising and GATT.
`ServiceData` is optional when capacity permits; Windows can report
`StartedWithoutAllAdvertisementData`. This is not a universal prohibition on
service data or a fixed payload limit for every Windows configuration.[1]

**Amended (ADR-0012 §2): a publisher for the fast advertisement.** This section
first said not to add an independent manufacturer-data publisher. That stays true
for repairing discovery: the publisher does not replace the provider, its
advertisement is not connectable, and a scanner never dials its address. It now
carries the managed fast advertisement next to the provider, as Google Nearby's
Windows medium does (`internal/platform/implementation/windows/ble.cc`, a
`BluetoothLEAdvertisementPublisher` for the fast advertisement next to the GATT
server). Evidence that led to it, and that it works, below.

Advertise the fields the chosen API can supply. For `PeerLink` that is the
9-byte advertisement header (ADR-0012 §2, §5) as `ServiceData` (Windows 10 1903
and later). Scanners would then read the readable value once per header hash
instead of on a timer. In a legacy advertisement it does not fit: Windows writes
the service UUID again in the service-data section (2 + 16 bytes before the
data) and keeps the flags and UUID lists (on the test PC also `0x180A`). The
Windows 11 build 26200 test PC reported `StartedWithoutAllAdvertisementData`
(logged with the data size) on every start, so phones read the PC as before.
The extended provider advertisement of build 26100
(`UseLowEnergyUncoded1MPhyAsSecondaryPhy`) has room, but legacy scanners such
as Android's current scan cannot see an extended-only advertisement: ADR-0012
phase 2.

So the driver starts a `BluetoothLEAdvertisementPublisher` (`publisher.rs`)
while the provider is visible: the fast advertisement as manufacturer data under
its company (Fini: `0xFFFF`, no SIG 16-bit UUID needed), 30 bytes, a legacy
advertisement. Like Nearby, it would use an extended advertisement with
`IsAnonymous = false` only when the record does not fit a legacy one and
`BluetoothAdapter.IsExtendedAdvertisingSupported`; the record always fits, and
legacy scanners cannot see extended-only advertisements. The publisher follows
show, hide and advertisement updates, and stops with the server.

Evidence on the test PC (Windows 11 build 26200) and the Android test phone:

- The publisher started (`Status` Waiting, then Started, error 0).
- The phone saw it from another address than the provider: `1B:D5:4E:4C:44:6B`,
  non-connectable, manufacturer `ffff:26`, against the provider's
  `7F:98:EF:FD:52:61`, connectable, with the service UUID and the name.
- The phone connected to the provider's address and read the GATT advertisement
  (one read), which gave the endpoint.

Because the addresses differ, the shared managed worker (ADR-0012 §2) joins them
by endpoint: a read of the provider's address records where the endpoint is;
the publisher's record (the whole endpoint) is then reported at that address, and
a later sighting of the provider's address is answered from the publisher's
record key, one read per hash. A publisher's record is never reported at its
own address, which a central cannot dial. Scanners let an advertisement through
when it carries only a fast advertisement (`carries_fast_advertisement`), and
the Android driver merges the advertisements of one address, as the watcher does.

GATT discovery of a new link ends as soon as that link drops
(`BluetoothLEDevice.ConnectionStatus` back to `Disconnected`), instead of
waiting out the 5 s uncached-discovery timeout and the caller's deadline. On
hardware an Android phone that was reading the PC at the same time dropped the
PC's link to it with "Instant Passed" (`0x28`) about 2 s in. Shared profile
discovery, not the Windows driver, determines whether it must fetch readable
GATT metadata.

The watcher scans actively with extended advertisements allowed, and asks for
`BluetoothLEAdvertisementScanParameters.LowLatency()` (Windows 11 24H2, build
26100) through `SetScanParameters`. Managed scans run only while discovery is
wanted, so the duty cycle is short. Where the class or interface is missing the
call fails, the watcher keeps the system parameters and logs "scan parameters:
not available"; that failure is the feature check, with no build-time switch.
The `windows` crate is at least 0.62.2, the first with that class. If end-to-end
runs show LE-connection or Wi-Fi regressions, `CoexistenceOptimized` is the
fallback. A result matches the service by its UUID list, by service data under
the service UUID, or by a fast advertisement in its manufacturer data.
The driver implements reads and `set_characteristic_value` without replacing a
live server merely to update its data. See ADR-0012 for format/cache policy.

`set_advertising_enabled(false)` stops provider visibility without destroying the
server, sessions or subscriptions. Re-enabling uses the retained provider.
`stop_advertising` remains destructive. Startup ownership guards stop an issued
native start if confirmation fails or its future is cancelled.

### 4. Native events and cleanup limits

Keep client/session objects alive for the operations that need them. Observe
native link loss and translate it to role/session-aware events. Serialize
server request handling so callback concurrency does not reorder fragments.
Serve readable values from the current value map, not an immutable initial value.

Publish `Driver::lifecycle_events()` separately from characteristic writes.
Its bounded storage can still report critical `Lagged`. The legacy `events()`
stream remains available. Known notification loss uses `BleError::ReceiveGap`;
it does not establish that a native session ended.

The server can exclude a central by rejecting writes and omitting notifications
until its session ends. Exclusion does not prove physical disconnection.
Keep diagnostics and cleanup assertions explicit about this limit.

Core arbitration selects an attempt; the driver must apply effects to the
correct owned session. A stale completion must not tear down a replacement.
Native crossed-attempt behavior still needs acceptance. Two role booleans do not prove it.

## Alternatives and consequences

- A central-only abstraction leaves the peripheral role to another native path
  and introduces another event model. The selected direct WinRT implementation
  covers both halves of the existing HAL.
- A separate non-connectable metadata advertisement depends on scanners
  correlating advertisements correctly. Not selected; discovery completion is
  the shared mechanism in ADR-0012.
- Returning success as if exclusion were physical disconnection would overstate
  the native operation. Record the real limit instead.

The target adds no Windows-only initiation mode. Driver integration alone
proves neither the new handshake nor hardware interoperability and changes no consumer dependencies.

## Verification and known limits

Working-tree source now guards client acquisition, subscription registration and
server startup. `close` also drains owned central cleanup, rather than only
stopping serving. Session/generation checks protect replacement objects.
Logical exclusion remains distinct from physical peripheral disconnect.

Linux and Android also separate visibility from server ownership. Android's
Rust/JNI and Kotlin bridge require ABI 8. Scan-start exceptions roll back only
the failed callback ownership. Source guards do not prove compiled descriptors,
APK packaging, callback timing or native execution.

Review watcher cache capacity, expiry and overflow delivery, callback queue bounds,
deferral timing, cancellation at each acquisition boundary and metadata partial
failure. Keep these review/native gates separate from implemented ownership fixes.

Earlier branch records report Windows builds/tests, capabilities and advertising.
This revision reruns no native checks. Those observations do not prove bidirectional acceptance.

Before accepting Windows support, verify on an authorized Windows/Android pair:

1. Runtime capabilities and radio status; unsupported configurations fail clearly.
2. Windows scans and completes metadata for the remote peer.
3. Windows initiates and exchanges complete messages in both directions.
4. The remote peer discovers Windows, fetches missing metadata and initiates.
5. Metadata changes while communication/subscriptions remain active.
6. Disconnect, cancellation, stale events and radio recovery preserve ownership.
7. An excluded additional central receives no other peer's data; native teardown
   is reported separately from logical exclusion.
8. The new core protocol's crossed-attempt outcomes match ADR-0012 on this path.

Builds, mocks, observed native sessions and physical radio/OS teardown are
separate claims. Use [hardware verification](../hardware-verification.md) for
procedures; retain pending checks rather than marking them passed from CI.

## Sources

[1] https://learn.microsoft.com/en-us/uwp/api/windows.devices.bluetooth.genericattributeprofile.gattserviceprovideradvertisingparameters.servicedata
