//! The `Backend` port: one implementation per platform. Every backend
//! speaks the same generic GATT vocabulary (`crate::models`) — callers never
//! see platform types (no `bluer::Device`, no JNI handles) crossing this
//! boundary.

#[cfg(target_os = "android")]
pub mod android;

#[cfg(target_os = "linux")]
pub mod linux;

pub mod mock;

#[cfg(target_os = "windows")]
pub mod windows;

use std::pin::Pin;

use async_trait::async_trait;
use tokio_stream::Stream;

use crate::error::Result;
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    ServiceUuid, WriteType,
};

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// The ATT MTU every BLE connection is required to support before any
/// negotiation happens (Bluetooth Core spec). Backends report this until a
/// larger MTU is actually agreed with the peer.
pub const DEFAULT_ATT_MTU: u16 = 23;

/// Bytes of ATT protocol header consumed by a write/notify PDU. Subtract
/// from the negotiated ATT MTU to get the usable payload size — see
/// `GattConnection::max_write_len`.
pub const ATT_HEADER_LEN: usize = 3;

/// One live GATT client connection to a remote peripheral (central role).
#[async_trait]
pub trait GattConnection: Send {
    fn peer(&self) -> PeerAddress;

    /// Identifies this connection among successive connections to the same
    /// peer, so a consumer can ignore lifecycle events belonging to a
    /// previous one. Matches the `session` on [`GattEvent`].
    fn session(&self) -> Option<u64> {
        None
    }

    /// The negotiated ATT MTU for this connection. Backends request a larger
    /// MTU on connect where the platform allows it, but the peer decides —
    /// never assume this is more than [`DEFAULT_ATT_MTU`].
    fn att_mtu(&self) -> u16;

    /// Largest payload that fits in a single write/notify on this
    /// connection (`att_mtu` minus [`ATT_HEADER_LEN`]). Chunk bulk transfers
    /// against *this*, not a hardcoded constant — the value is only known
    /// after MTU negotiation and differs per peer and per platform.
    fn max_write_len(&self) -> usize {
        (self.att_mtu() as usize).saturating_sub(ATT_HEADER_LEN)
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>>;

    /// Write acknowledged by the peer ([`WriteType::WithResponse`]).
    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.write_with_type(characteristic, value, WriteType::WithResponse)
            .await
    }

    /// Write with an explicit delivery mode — see [`WriteType`]. Implementors
    /// provide this; `write` is a convenience wrapper over it.
    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    ) -> Result<()>;

    /// Stream of server-initiated notifications for `characteristic`.
    ///
    /// Items are `Result` for the same reason `scan`'s are: the failure is
    /// asynchronous. A backend that must bound its inbound buffering drops
    /// payloads when the consumer falls behind, and the peer has *already*
    /// been told the notification was delivered — so unless the loss is
    /// reported here, neither endpoint ever learns, and a message either
    /// vanishes or expires with no error. An error item does not end the
    /// stream; it reports a gap.
    async fn subscribe(
        &mut self, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>>;
    async fn disconnect(&mut self) -> Result<()>;
}

/// Platform BLE backend: scan/connect as a GATT central, and/or
/// advertise/serve as a GATT peripheral. A backend that can't do peripheral
/// mode (most mobile OSes, some adapters) still implements this trait —
/// `capabilities()` reports it honestly and `advertise()` returns
/// `BleError::PeripheralUnsupported` rather than the caller having to guess
/// from the target OS. See `models::CapabilityReport`.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn capabilities(&self) -> CapabilityReport;

    // --- Central role ---

    /// Stream of peers advertising `service`, until the returned stream is
    /// dropped. Discovery is untrusted metadata (mirrors Fini's Sim/TcpWs
    /// discovery contract) — no authentication has happened yet.
    ///
    /// Items are `Result` because scanning fails *asynchronously*: the
    /// initial call only reports that the scan was accepted, and the reason
    /// it later stopped (Android's `onScanFailed`, an adapter powered off
    /// mid-scan) arrives on the stream. Without this, a failed scan is
    /// indistinguishable from an empty one — the caller silently reports
    /// "no peers found" when the real answer is "Bluetooth is off".
    /// An error item is terminal for that scan.
    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>>;

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>>;

    // --- Peripheral role ---

    /// Start advertising `service` and serving its characteristics locally.
    /// Idempotent-per-service is not guaranteed; call `stop_advertising`
    /// first to change the served service.
    async fn advertise(&self, service: GattServiceSpec) -> Result<()>;

    async fn stop_advertising(&self) -> Result<()>;

    /// Push a value to **every** central currently subscribed to
    /// `characteristic` on the local GATT server. Errors if it reached
    /// nobody — a reliable caller must not be told a dropped payload was
    /// sent.
    ///
    /// Prefer [`Backend::notify_peer`] for anything carrying per-peer data.
    /// Subscribing is a client-side act needing no server consent, so a
    /// central a higher layer believes it refused is still subscribed until
    /// it is disconnected, and this delivers to it.
    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()>;

    /// Push a value to exactly one subscribed central.
    ///
    /// Both platforms can address a notification: Android's
    /// `notifyCharacteristicChanged` takes a device, and on Linux the
    /// peripheral acquires one writer per subscriber via `AcquireNotify`
    /// (which, unlike `StartNotify`, carries the device address).
    ///
    /// This is what makes a single-peer server safe against the *window*
    /// between a second central subscribing and being disconnected:
    /// disconnecting is asynchronous, and a broadcast during that window
    /// would hand the refused peer the served peer's traffic. Errors if
    /// `peer` has no live notify session.
    /// `session` names which of that address's notify sessions to target,
    /// for the same reason [`Backend::disconnect_peer`] takes one: both
    /// platforms select subscriptions by address, so a caller holding a
    /// stale channel would otherwise route its payloads into the
    /// subscription that replaced it — where they are reassembled as
    /// current data. `None` targets whatever currently holds the address.
    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()>;

    /// Drop a remote central's connection to the local GATT server.
    ///
    /// The enforcement half of a single-peer server: a refused central stays
    /// subscribed until it is dropped, so this is what actually excludes it.
    /// Best-effort and idempotent: a peer that is already gone is `Ok`.
    ///
    /// `session` names *which* connection to drop — every backend addresses
    /// peers by address, so without it a caller holding stale state can
    /// disconnect a peer's replacement session instead of the one it meant.
    /// `None` disconnects whatever currently holds the address, which is
    /// only correct when the caller has no session to be stale about.
    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()>;

    // --- Lifecycle ---

    /// Connection lifecycle events for **both** roles, plus inbound writes to
    /// the local GATT server, fanned out to every subscriber (mirrors Fini's
    /// `transport::selection::LifecycleBus` pattern).
    ///
    /// Crucially this includes *unsolicited* central-role disconnects — a
    /// peripheral going out of range or powering off mid-conversation. There
    /// is no other way to learn about that: `GattConnection`'s methods only
    /// report failures of operations you initiated, so a caller partway
    /// through a long transfer would otherwise just hang. Anything holding a
    /// `GattConnection` open across time should watch this stream.
    ///
    /// **`Connected` is not once-per-connection — treat it as idempotent.**
    /// Neither platform backend has a reliable server-side connection
    /// signal, so both re-emit `Connected` for a peer ahead of every
    /// characteristic write. A consumer that allocates per-peer state on
    /// each `Connected` will tear down and replace that state mid-session;
    /// key off the peer address and ignore repeats.
    ///
    /// Characteristic *values* are not carried here — client-side
    /// notifications come back through `GattConnection::subscribe`.
    fn events(&self) -> BoxStream<GattEvent>;
}
