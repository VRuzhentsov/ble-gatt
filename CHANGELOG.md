# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/VRuzhentsov/ble-gatt/compare/v0.2.0...v0.2.1) - 2026-11-22

### Other

- Cap Windows notifications at 512 bytes and read a rotated peer once ([#40](https://github.com/VRuzhentsov/ble-gatt/pull/40))

## [0.2.0]

### Added

- Windows WinRT driver (ADR-0011).
- Nearby-inspired managed core: discovery, request/both-accept establishment,
  nonce arbitration, liveness and scoped native ownership (ADR-0012).
- `PeerLink::update_advertisement(GattAdvertisement)`.

### Breaking public API changes

- `CharacteristicWritten` gains a `session` field.
- `Driver::set_characteristic_value` is new and required for every driver.
- `LinkId`, `LinkRole`, `PeerLinkEvent`, `PeerStatus` and `RetryBudget` are removed.
- `GattServiceSpec` has new fields.
