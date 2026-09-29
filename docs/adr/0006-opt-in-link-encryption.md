# 0006 — Opt-in link encryption, as one flag per characteristic

## Status

Accepted. Delivered with its implementation in one change.

## Context

ADR-0003 settles that `ble-gatt` is a carrier, not a crypto layer: a consumer
that needs confidentiality or authentication protects its own messages (Fini
does, with its own `Auth` frame, and moved away from OS bonding after a
dual-bond problem). That stays true.

A different, generic consumer may instead want the *OS* to do it: require
BLE link-layer encryption and let the platform handle pairing. Both platforms
support this natively, and on the side that decides it — the GATT server —
it is one permission per characteristic:

| Platform | Mechanism |
|---|---|
| Linux (BlueZ via `bluer`) | `CharacteristicRead::encrypt_read`, `CharacteristicWrite::encrypt_write` |
| Android | `PERMISSION_READ_ENCRYPTED` / `PERMISSION_WRITE_ENCRYPTED`, and the same on the CCCD descriptor |

The connecting central needs nothing: its OS sees the insufficient-encryption
error and pairs on demand, usually with a system prompt, before the access
succeeds.

## Decision

- **One `bool`, `GattCharacteristicSpec::encrypted`**, default `false`.
  `DatagramConfig::encrypted` sets it on the single characteristic Tiers 2–3
  serve, and on the dialling side makes `datagram::connect` read the
  characteristic once after subscribing (see Consequences). Set it on both
  sides.
- **No strength levels.** Both platforms also offer MITM-protected
  ("authenticated") encryption, but it only works when both devices have a
  display or keyboard and brings its own failure modes. Nobody needs it yet;
  adding it later would be a new field, not a change to this one.
- **No new error variant.** An access refused for lack of encryption fails on
  the existing error paths. Android reduces GATT status to success/failure
  before it reaches Rust, so distinguishing it would widen the JNI surface for
  no current consumer.
- **Peripheral-side only.** There is no "request encryption" call for the
  central.

## Consequences

- BlueZ can gate the subscription (`encrypt-notify`), but `bluer` does not
  expose that flag, so an unpaired central can subscribe on Linux. Sending to
  it then would cross an unencrypted link. The Linux backend therefore does
  not announce a central on its subscription to an encrypted characteristic,
  only on its first read or write (BlueZ delivers those only once the link is
  encrypted), and skips its notify session until then. For a
  server-speaks-first protocol the central has nothing to write, so
  `datagram::connect` with `encrypted` set reads once after subscribing: that
  makes its OS pair and is the proof the server waits for. A *notify-only*
  encrypted characteristic on Linux is only delivered to centrals that have
  read or written another encrypted characteristic of the service. Android
  gates the subscription itself, through the CCCD permission.
- The Android bridge ABI goes to v4 (`startAdvertising` gained an
  `encrypted` array).
- Windows (WinRT `GattProtectionLevel`) and Apple
  (`CBAttributePermissions.readEncryptionRequired`) map the same way when
  those backends exist.
