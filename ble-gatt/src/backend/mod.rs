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

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>>;
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
    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<DiscoveredPeer>>;

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>>;

    // --- Peripheral role ---

    /// Start advertising `service` and serving its characteristics locally.
    /// Idempotent-per-service is not guaranteed; call `stop_advertising`
    /// first to change the served service.
    async fn advertise(&self, service: GattServiceSpec) -> Result<()>;

    async fn stop_advertising(&self) -> Result<()>;

    /// Push a value to every central currently subscribed to `characteristic`
    /// on the local GATT server (the server-initiated half of GATT notify —
    /// the client-initiated half is `GattConnection::subscribe`). Errors if
    /// not currently advertising.
    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()>;

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
    /// Characteristic *values* are not carried here — client-side
    /// notifications come back through `GattConnection::subscribe`.
    fn events(&self) -> BoxStream<GattEvent>;
}
