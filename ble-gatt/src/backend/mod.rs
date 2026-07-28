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
    ServiceUuid,
};

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// One live GATT client connection to a remote peripheral (central role).
#[async_trait]
pub trait GattConnection: Send {
    fn peer(&self) -> PeerAddress;
    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>>;
    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()>;
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

    /// Connection lifecycle + inbound-write events for the local GATT
    /// server, fanned out to every subscriber (mirrors Fini's
    /// `transport::selection::LifecycleBus` pattern). Never carries GATT
    /// client (central-role) events — those come back through
    /// `GattConnection::subscribe`.
    fn events(&self) -> BoxStream<GattEvent>;
}
