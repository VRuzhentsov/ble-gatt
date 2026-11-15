# 0011: Windows driver, WinRT central and peripheral

## Status

PR #38 contains the first Windows implementation. Contract gaps and cross-device
acceptance remain pending. This ADR covers Windows-native choices;
[ADR-0012](0012-discovery-like-nearby.md) covers shared discovery and the unimplemented
core tie-break. The PR remains draft.

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

Use `GattServiceProvider` for connectable service advertising. Do not add an
independent manufacturer-data publisher to repair profile discovery.
`ServiceData` is optional when capacity permits; Windows can report
`StartedWithoutAllAdvertisementData`. This is not a universal prohibition on
service data or a fixed payload limit for every Windows configuration.[1]

Advertise the fields the chosen API can supply. Shared profile discovery, not
the Windows driver, determines whether it must fetch readable GATT metadata.
The driver implements reads and `set_characteristic_value` without replacing a
live server merely to update its data. See ADR-0012 for format/cache policy.

### 4. Native events and cleanup limits

Keep client/session objects alive for the operations that need them. Observe
native link loss and translate it to role/session-aware events. Serialize
server request handling so callback concurrency does not reorder fragments.
Serve readable values from the current value map, not an immutable initial value.

The server can exclude a central by rejecting writes and omitting notifications
until its session ends. Exclusion does not prove physical disconnection.
Keep diagnostics and cleanup assertions explicit about this limit.

Core arbitration selects an attempt; the driver must apply effects to the
correct owned session. A stale completion must not tear down a replacement.
The exact contract needed for crossed attempts is a gate in ADR-0012, not a
capability inferred from two role booleans.

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

Source review identifies partial-acquisition cleanup gaps: maintained client
sessions and notification callback tokens are acquired before awaits without
complete cancellation guards (`device.rs`); server construction also needs
ownership protection across advertising startup (`peripheral.rs`). Current
`close` only stops serving, and peer exclusion does not itself report a terminal
physical disconnect (`mod.rs`). Callback queue bounds/deferral timing and
watcher cache/delivery behavior require contract review and native validation.
These are static findings, not reproduced Windows failures or completed fixes.

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
