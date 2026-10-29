# 0010 — Serving recovers what it last served

## Status

Accepted. Implemented in `hal/`, `profile/datagram/`, `roles/peripheral.rs`
and both drivers.

## Context

When the radio goes off or the Bluetooth service restarts, the platform
forgets the advertisement and the GATT server. `datagram::serve` and
`Peripheral::serve` therefore register again on `RadioChanged { On }` (see
the glossary's *radio recovery*). They used the spec `serve` started with,
which caused two bugs:

- An advertisement update was undone. Fini changes its advertisement when it
  leaves Add Device (`update_advertisement`); after Bluetooth off and on, the
  old advertisement came back.
- A stopped server came back. `stop_serving` stops the platform's
  registration but not `serve`'s task, which still holds the stream. After a
  power cycle a device stopped to be unreachable advertised again.

Two related driver bugs, found in the same review:

- Linux: `update_advertisement` registered the new advertisement before
  dropping the old one, so a controller with one advertising instance
  refused it, and the error path left nothing advertised.
- Android: switching Bluetooth off while `update_advertisement` awaited its
  start result left the wait pending forever, holding `advertise_lock`, so
  every later advertise blocked.

## Decisions

**D1 — The driver reports what it serves.** `Driver::served_service()`
returns the served spec with its latest advertisement data, kept across a
radio loss and cleared by `stop_advertising`. Both drivers already kept it
(`served_service`) to check `update_advertisement`'s spec. It has no
default, so a wrapping driver (Fini's `SwitchedDriver`) must forward it:
with a default of `None`, a wrapper that forgot would silently never
recover.

**D2 — Recovery registers what the driver serves, or nothing.** On
`RadioChanged { On }` both servers advertise `served_service()` if it is
the same service, and nothing if it is `None`. The driver is the one place
that sees every `advertise`, `update_advertisement` and `stop_advertising`,
whoever called them, so the recovery cannot miss one.

*Alternative.* Keep the current spec in the serving task, updated by
`update_advertisement` and cleared by `stop_serving`. Both functions take
only an `Adapter`, not the task, so this needs shared state keyed by
adapter, a second copy of what the driver already holds.

**D3 — Linux frees an instance first only when none is spare.** When BlueZ
reports no free advertising instance (`LEAdvertisingManager1.SupportedInstances`
is 0), `update_advertisement` drops the old advertisement, waits up to 2 s
for BlueZ to report its instance free, and then registers the new one.
Otherwise it keeps registering first, so advertising never pauses.

**D4 — Android ends a pending advertise wait on radio loss.** The radio-off
handler completes the waiting start result with the stopped error, as
`stop_advertising` already did; Kotlin ignores the late callback.

## Consequences

- Fini's `SwitchedDriver` must implement `served_service`; it no longer
  compiles otherwise.
- A third-party driver must report `served_service` honestly, or recovery
  re-advertises nothing.
- Tests: `a_radio_recovery_keeps_an_advertisement_update`,
  `a_radio_recovery_keeps_a_datagram_advertisement_update`,
  `a_radio_recovery_does_not_restart_a_stopped_server`. D3 and D4 have
  none: the mock models neither a one-instance controller nor the Android
  callback.
