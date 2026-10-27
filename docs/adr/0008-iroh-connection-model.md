# 0008 — `ble-gatt-iroh` follows iroh's connection model

## Status

Accepted. D1–D3 are implemented in `ble-gatt-iroh`. D4 and D5 needed no
transport code: on the mock radio iroh already does both (see their notes).
What is left is Fini's side and hardware.

## Context

`ble-gatt-iroh` (ADR-0006) carries iroh's QUIC packets over datagram
channels. Today it decides which BLE link belongs to which peer from one
source only: the address the application records with `set_peer_address`
when it dials. A link the peer dialled into this device arrives from an
address nobody recorded, so the transport cannot tell whose it is.

Hardware runs in Fini (Android phone and Linux desktop, Bluetooth switched
off and on) showed what that costs:

- `close_links(peer_key)` on the phone closed nothing. The only link was the
  one the desktop had dialled in, and its address was not recorded for the
  desktop's key. It stayed up about 3 s, until the desktop's BlueZ dropped
  it, and the desktop did not re-advertise meanwhile.
- A link outlives the iroh connection on it until it carries nothing for
  `link_idle_timeout` (20 s). Fini added a `close_links` call after each
  place where that was too long: a session ending, a rejected session, a
  pairing link left unused.
- Fini lets only the device with the smaller id dial over Bluetooth. Two
  dials crossing made BlueZ abort one (`le-connection-abort-by-local`), and
  Fini's crossing rule refused the other. With that rule, the device with
  the larger id only ever accepts, so it can never close its session link
  itself.

Terms used below:

- **Endpoint hooks** (`iroh::endpoint::EndpointHooks`): callbacks an
  application installs on its iroh endpoint. `after_handshake` runs for
  every connection, incoming and outgoing, once TLS has proved the peer's
  key.
- **Path**: one transport address a QUIC connection can use to reach the
  peer. iroh lists them (`Connection::paths`) and reports them opening and
  closing (`Connection::path_events`). For this transport a path's address is
  the peer's BLE address.
- **Crossed dials**: both devices dialling each other at the same moment.

On the mock radio, `after_handshake` on the accepting side lists a path whose
address is the dialling device's BLE address, and on the dialling side the
address it dialled. Both sides can therefore learn key ↔ link from iroh
itself.

## Decisions

**D1 — The transport learns which key each link belongs to from iroh.**
`ble-gatt-iroh` provides endpoint hooks (`BleGattTransport::hooks()`), which
the application installs once with `Endpoint::builder().hooks(..)`. After
each handshake they record the connection's peer key against the BLE
addresses of its paths, and follow `path_events` for later paths. This covers
links the peer dialled in. `set_peer_address` stays for what iroh cannot know
yet: where to dial a peer before any connection exists.

*Alternatives.* The application could keep its own key → inbound address
map (every application would repeat it), or tell the transport with a new
call when it accepts a connection (easy to forget). Both duplicate what iroh
already knows. Precedent: iroh documents hooks as the place to inspect a
connection once its key is known, and to keep a `WeakConnectionHandle`
rather than the connection.

**D2 — `close_links(peer_key)` reaches every link to that key.** With D1 it
closes links in either role, using `DatagramChannel::disconnect` (the
peripheral side drops the central). Its signature does not change.

**D3 — A link closes when the last iroh connection on it has drained.** The
hooks keep a weak handle per connection. "Ended" means drained, not closed:
`on_closed` fires when the close starts (at `close()`, before its
CONNECTION_CLOSE is sent, or when the peer's arrives), and cutting the link
then lost the peer's copy on hardware. QUIC then drains for three probe
timeouts; noq's connection driver holds the connection until that is over,
so the weak handle dies exactly when it has drained and the application has
dropped its own handles. Neither noq nor iroh signals that moment, so the
hook checks the weak handle every 10 ms after `on_closed`. On the mock radio
(RTT 8 ms) `on_closed` fired 0.25 ms after `close()` and the handle died
after 201 ms (346 ms on the peer's side).

When every connection using a link has drained, the transport closes it,
with no delay by default (`link_release_delay` can add one). A connection
that has taken the link over by then keeps it. An application that wants a
link kept for a connection it has yet to open keeps an earlier connection
open until then. The idle timeout stays as a backstop for a link no
connection ever used (a peer that dialled and never finished the handshake).
Fini's calls after a session, a rejection and an unused pairing link should
then become unnecessary.

This close is the idle kind: it ends a link this side dialled, and leaves a
link the peer dialled to the peer, whose own D3 ends it. Only `close_link`
and `close_links` drop a link the peer dialled (`DatagramChannel::disconnect`).
On Linux, dropping it ends the whole link to that address, including a busy
link this side dialled, so it must stay an explicit request.

**D4 — One link to a peer carries both directions.** A BLE link already does:
the central writes, the peripheral notifies. When iroh sends to a peer key
that has a link in either role, the transport sends on it and does not dial.
In most cases this removes crossed dials instead of breaking the tie.

*Outcome.* iroh does this itself. On the mock radio, after B dialled A, A
connected back to B's key with B recorded at another address: iroh sent
everything over the path B's connection came in on and never tried the
other address, even 8 s later. Rerouting in the transport changed nothing a
test could see, so it was not kept.

**D5 — Duplicates are tolerated, as iroh-gossip does.** If two dials still
cross, both links come up. The newest carries the traffic; the older one
closes when no connection uses it (D3). No comparison of ids. Precedent:
`iroh-gossip` (n0's own protocol on iroh, `src/net.rs`) keeps the old
connection, sends only on the new one, and lets the unused one close; it
also never starts a second dial to a peer while one is pending, which
`ble-gatt-iroh` already does.

*Outcome.* No transport code: `crossed_connects_both_work_and_their_links_close`
shows both connections working with no tie-break, and every link closing
once they end. Refusing the second connection, as Fini's crossing rule does,
is an application choice; iroh-gossip keeps the newest instead. The mock
radio does not reproduce BlueZ aborting one of two crossed dials, so
hardware decides whether Fini can drop its "smaller id dials" rule.

## Consequences

- An application must install the hooks to get D1–D4. Without them the
  transport behaves as before; the crate docs and `hooks()` say so.
- Fini can drop its "smaller id dials" rule on Bluetooth, or keep it only for
  a cold start, when neither device has a link yet. That is Fini's decision;
  this ADR only makes it possible.
- The mapping exists only once TLS has finished. A dial that starts before a
  handshake completes cannot use it, so a crossed dial is rarer, not
  impossible; D5 covers what is left.
- iroh's hooks and path events are stable API in iroh 1.3. The custom
  transport itself stays behind `unstable-custom-transports` (ADR-0006).

## Plan

1. Done: hooks that record key ↔ address and follow path events (D1), with
   `close_links` using the map (D2). Mock test:
   `close_links_reaches_a_link_the_peer_dialled`.
2. Done: close on the last connection's end (D3). Mock tests:
   `a_link_closes_when_its_last_connection_ends`,
   `a_new_connection_keeps_a_released_link`.
3. Done, no code: D4 and D5 come from iroh (see their outcomes). Mock test:
   `crossed_connects_both_work_and_their_links_close`.
4. Hardware run in Fini; Fini removes its own workarounds (the extra
   `close_links` calls, then the "smaller id dials" rule) only after a run
   shows they are no longer needed.

## Open questions

- Whether BlueZ still aborts one of two dials that cross at the same instant
  once D4 makes them rare. Only hardware shows that.
- Fini's reuse of the pairing link for the first session
  (`keep_pairing_link`): with no release delay, the link goes once the
  pairing connection drains, so Fini keeps that connection open until the
  session's is up.
