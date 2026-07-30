# 0003 — `ble-gatt` is a carrier, not a crypto library; and the two API tiers

## Status

Accepted. Supersedes the open question in ADR-0001 about where an
encryption seam belongs, and records the constraints that shape everything
built after it.

## Context

Two real consumers drove this, and they want opposite things:

- **App-to-app.** Two devices the same person owns, syncing peer-to-peer
  (Fini; Bitchat is the same shape). Both ends run software we control, so
  a framed, encrypted session protocol is possible and wanted.
- **App-to-device.** A phone/desktop app talking to vendor sensor hardware
  over Nordic UART, with the vendor's own command framing, CRC-32, and an
  AES challenge/response. The firmware is third-party and unchangeable.

The second case is the constraint that settles the design: **that device
will never implement our session protocol or our encryption.** Any layer
this library imposes on the wire is a layer that use case cannot use.

## Decision

### Encryption is the consumer's job, not this library's

`ble-gatt` contains no cryptography and defines no session protocol. It
carries bytes.

The alternative — building an encrypted session layer into the library —
was considered and rejected. Fini already owns this concern in its own
`secure_channel` seam, deliberately covering *every* transport it speaks
(HTTP/WebSocket today, BLE next, LoRa reserved). Encryption belongs there,
where one implementation protects all transports uniformly, rather than
here, where it would only protect the BLE path and would be dead weight —
or an outright obstacle — for the app-to-device case.

What "ready for encryption" actually requires of a carrier, and what this
library therefore guarantees:

- **No payload assumptions.** Ciphertext is indistinguishable from random.
  Nothing may assume UTF-8, inspect content, or impose framing.
- **Boundaries preserved.** A consumer that hands over one encrypted
  message gets exactly that message back on the far side — not a stream to
  re-delimit, which would force every consumer to reinvent framing beneath
  their crypto.
- **Fragmentation below the crypto layer.** Splitting to fit the MTU is the
  carrier's problem; it must be invisible to whatever sits above.

### Two API tiers

- **Raw GATT** — scan/connect/read/write/subscribe/advertise against
  specific characteristics. The app-to-device path lives here, speaking
  someone else's protocol. This tier imposes nothing.
- **Datagram** — an ordered, opaque-bytes channel to a peer, with
  fragmentation and reassembly handled internally against the negotiated
  MTU. The app-to-app path lives here. This is exactly the shape Fini's
  existing `Link` port already specifies: *"moves opaque byte datagrams
  (whole payloads, boundaries preserved); each adapter owns its own
  chunking."*

Fragmentation sits in this library rather than in each consumer because
doing it correctly needs real machinery — bounded concurrent reassembly,
timeouts, cumulative size caps, metadata consistency checks — that is easy
to get subtly wrong and pointless to reimplement per consumer. Bitchat's
`FragmentManager` is a useful reference for the hardening required.

### No AGPL, including transitively

Already established in ADR-0001 (why `blew` was rejected). Restated because
it now binds a second decision: the official Signal Protocol
implementation, `libsignal`, is **AGPL-3.0**. Using it anywhere in this
dependency graph would force `ble-gatt` off MIT, and would force every
consumer — including closed commercial ones — to publish their source.
That is disqualifying regardless of the protocol's merits.

Permissive alternatives were surveyed for the Fini-side work:

| Crate | License | Notes |
|---|---|---|
| `libsignal` | **AGPL-3.0** | Disqualified on license alone |
| `snow` (Noise) | Apache-2.0 | Widely used; Noise XX suits live interactive links |
| `vodozemac` (Olm) | Apache-2.0 | See caveat below |
| `double-ratchet` | BSD-3 | Unmaintained since 2021 |

`vodozemac` was the initial recommendation and was **withdrawn**. It is
Matrix's official implementation and passed a 2022 public audit, but a
February 2026 analysis reported that its 3DH key agreement accepts an
all-zero public key — yielding an all-zero shared secret — and the finding
was disputed rather than patched. It applies directly to new consumers
using it as a Double Ratchet implementation, who must add contributory
checks the library does not enforce. Recommending it while aware of an
unfixed finding matching our exact usage was not defensible.

The current recommendation for Fini's `secure_channel` is therefore
`snow`/Noise. That choice lives in Fini and does not constrain this
library, which stays crypto-free either way.

### Mesh relay is out of scope

Bitchat's multi-hop flooding (TTL, dedup, store-and-forward) is a genuinely
useful reference but is not a requirement for either consumer. It layers
*above* the datagram tier and can be added later without disturbing
anything here. Not building it now.

## Consequences

- The app-to-device consumer depends only on the raw GATT tier and never
  pays for session or crypto machinery it cannot use.
- Fini gets encryption once, covering all its transports, rather than a
  BLE-only implementation duplicated against its existing one.
- This library stays MIT and usable in closed commercial products —
  which is the entire reason it exists as a separate crate.
- If an encrypted session layer is ever extracted for sharing between
  projects, the AGPL question returns, because it would then bind every
  consumer of that shared layer. Revisit this ADR at that point.
