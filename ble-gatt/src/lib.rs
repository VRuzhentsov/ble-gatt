//! Async, transport-agnostic BLE GATT primitives (central + peripheral role)
//! with no Tauri dependency — usable from any Tokio-based Rust program, not
//! just Tauri apps. See `backend::Backend` for the platform port and
//! `backend::mock::MockBackend` for a CI-safe, radio-free stand-in.

pub mod backend;
pub mod error;
pub mod models;

pub use backend::{Backend, BoxStream, GattConnection};
pub use error::{BleError, Result};
pub use models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattCharacteristicSpec, GattEvent,
    GattServiceSpec, PeerAddress, Role, ServiceUuid,
};
