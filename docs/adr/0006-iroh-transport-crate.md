# 0006 — `ble-gatt-iroh`: an iroh transport over the datagram channel

## Status

Accepted. Implemented in `ble-gatt-iroh/`.

## Context

Fini moves its device-to-device communication onto
[iroh](https://github.com/n0-computer/iroh) (Fini ADR-0009). iroh dials by
public key, authenticates and encrypts with TLS 1.3 over QUIC, and accepts
custom transports for links it cannot make itself, such as Bluetooth.

An iroh BLE transport already exists (`iroh-ble-transport`, on `blew`).
Both are AGPL-3.0. `ble-gatt` is MIT and is meant for closed-source projects
as well, so neither can be a dependency or a source of code here.

## Decision

**A separate crate.** `ble-gatt-iroh` lives in this workspace and depends on
`ble-gatt` and iroh. The `ble-gatt` core takes no iroh dependency, so
consumers that do not use iroh are unaffected.

**One QUIC packet is one datagram message.** The transport implements iroh's
`CustomTransport` over `ble_gatt::datagram::DatagramChannel`, which already
fragments and reassembles. A packet is dropped when a peer's queue is full
or a message is lost; QUIC treats that as loss and recovers, so the
transport adds no retransmission of its own.

**The caller owns the radio.** The transport never scans or advertises:

- inbound channels are accepted by the caller (for instance with
  `datagram::serve` while it wants to be reachable) and handed over with
  `attach`;
- the caller records where a peer can be reached with `set_peer_address`,
  which backs the transport's iroh address lookup;
- a caller-supplied `Dialer` (`dial_with` wraps `datagram::connect`) opens a
  channel the first time iroh sends to an address with none. Packets sent
  while it dials wait in the peer's queue.

This lets an application keep its own battery policy for when the radio
works.

**Addresses.** A custom address is transport id `0x424C4547` ("BLEG") with
the backend's `PeerAddress` string as data. The id is not registered in
iroh's `TRANSPORTS.md`.

**GATT only.** No L2CAP: Android needs API 29+ for LE credit-based channels.

## Consequences

- iroh's custom transport API is behind its `unstable-custom-transports`
  feature and may change in a minor release; this crate pins iroh 1.3.
- `tests/mock_radio.rs` runs QUIC between two iroh endpoints over the
  in-process mock radio, dialling both by address and by key. Hardware
  behaviour (handshake time and throughput over real GATT) is not covered
  yet.
