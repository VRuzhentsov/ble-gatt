# 0008 — `ble-gatt-iroh` follows iroh's connection model

## Status

Accepted. D1–D3 and D6 are implemented in `ble-gatt-iroh`; D3 was revised
after hardware runs (see its history). D4 and D5 needed no transport code:
on the mock radio iroh already does both (see their notes). What is left is
Fini's side and hardware.

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

**D3 — A link closes when the last iroh connection on it has ended.** The
hooks keep a weak handle per connection and watch `closed()`, which fires
when the close starts: at `close()`, before its CONNECTION_CLOSE is sent, or
when the peer's arrives. A connection has *ended* when:

- the peer closed it, reset it or it timed out: this side is draining and
  sends nothing more (RFC 9000 §10.2.2), so it ends at once;
- this side closed it, and its CONNECTION_CLOSE has been sent. noq counts
  the frame (`stats().frame_tx.connection_close`) when it builds the packet
  and hands the packet to the transport under the same lock; a barrier
  queued behind it on each link the connection used then shows it sent.
  QUIC's closing period after it, which only resends the close in answer to
  the peer's packets, is not waited for;
- otherwise, it has drained: noq's driver holds the connection until then,
  so the weak handle dies at that moment. This is the fallback when the
  close was not seen being sent, and when a link dropped a packet for a
  full queue while the connection closed (the dropped packet may be the
  close, and noq is not told).

Neither noq nor iroh signals the sent close or the drain, so the hook checks
both every 10 ms. After a connection ended, a `Connection`, `SendStream` or
`RecvStream` the application keeps no longer holds the link; before, each
holds the connection, and so the link, while it is open.

When every connection using a link has ended, the transport closes it, with
no delay by default (`link_release_delay` can add one, for every link). A
connection that has taken the link over by then keeps it. An application
that wants a link kept for a connection it has yet to open keeps an earlier
connection open until then. The idle timeout stays as a backstop for a link
no connection ever used (a peer that dialled and never finished the
handshake).

How the close ends the link depends on the role:

- **Central** (this side dialled): the link is closed.
- **Peripheral on Linux**: the central is disconnected. No connection uses
  the address any more, so the concern that made this an explicit request,
  dropping a busy link this side dialled to the same address, is covered.
- **Peripheral on Android**: the central is kept connected and subscribed.
  `cancelConnection` does not end a link the central opened but forgets its
  subscription, so each later write opened a channel whose first notify
  failed. Precedent: Apple's `CBPeripheralManager` has no call to
  disconnect a central at all. The same holds for a served channel closed
  with `DatagramChannel::close()`; dropping one without `close()` still
  disconnects it.

The redial yield (`REDIAL_YIELD`, see the glossary) starts after a link the
peer dialled drops only while a connection the hooks saw on it had not
started to close: only an open connection makes the peer dial back. Without
the hooks it starts as before.

*History.* The first version waited for the drain: cutting the link at
`closed()` lost the peer's copy of the close on hardware. Draining takes
three probe timeouts over all paths (noq-proto `set_close_timer`), seconds
on BLE: the phone, as central and closer, held its link 13.4 s, Android kept
the ACL 4 s more, and BlueZ aborted the desktop's three dials to the phone
meanwhile. The peripheral then kept every central, which on Linux left that
old link to block the next dial. A refused session showed the yield's gap:
the phone waited 30 s for a desktop that would never dial back. Precedent
for not waiting: `iroh-ble-transport` (AGPL, ideas only) tears its link down
as soon as `closed()` fires, in both directions, and drops queued writes;
here the close is sent first. Tests:
`the_closing_side_releases_its_link_once_its_close_is_sent`,
`the_receiving_peripheral_disconnects_the_central`,
`a_refused_connection_does_not_hold_back_a_dial`,
`closing_a_served_channel_keeps_its_central_for_the_next_one`.

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

**D6 — A connection that has ended dials nothing.** The hooks remember each
ended connection per address until it drains. A packet for an address that
only ended connections use is dropped instead of dialled, unless it is a
QUIC Initial: a new connection still dials. A closing connection answers
the peer's late packets with its CONNECTION_CLOSE; dialling to carry that
brought a link back up right after D3 released it (on the mock radio, about
1 run in 10 of `the_closing_side_releases_its_link_once_its_close_is_sent`).
Dropping it reads to QUIC as loss, which a closing connection already
tolerates: its close is only resent in answer to later packets. Precedent:
a draining connection sends nothing at all (RFC 9000 §10.2.2).

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
4. Done: D3 revised (no wait for the drain, peripheral role by platform,
   redial yield) and D6. Mock tests listed under D3 and D6.
5. Hardware run in Fini; Fini removes its own workarounds (the extra
   `close_links` calls, then the "smaller id dials" rule) only after a run
   shows they are no longer needed.

## Open questions

- Whether BlueZ still aborts one of two dials that cross at the same instant
  once D4 makes them rare. Only hardware shows that.
- Fini's reuse of the pairing link for the first session: with no release
  delay the link goes as soon as the pairing connection ends. Fini accepted
  that (paired → green 4–6 s, within its 10 s standard) rather than a
  delay, which would hold every link after each close.
