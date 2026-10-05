//! Async, transport-agnostic BLE GATT primitives (central + peripheral role)
//! with no Tauri dependency — usable from any Tokio-based Rust program, not
//! just Tauri apps. See `hal::Backend` for the platform port and
//! `drivers::mock::MockBackend` for a CI-safe, radio-free stand-in.
//!
//! Modules follow the layer map in `docs/architecture.md` (Clean
//! Architecture layers, Bluetooth-specification module names):
//!
//! | Module | Layer |
//! |---|---|
//! | `entities` | Entities |
//! | `hal`, `roles`, `connection`, `profile` | Use Cases |
//! | `drivers` | Frameworks & Drivers |

pub mod connection;
pub mod drivers;
pub mod entities;
pub mod hal;
pub mod profile;
pub mod roles;

pub use connection::peer_link;
pub use entities::{error, models};
pub use profile::datagram;

/// The paths before the layer modules (`ble_gatt::backend::linux::…`,
/// `ble_gatt::backend::Backend`), kept so existing callers still build.
/// New code uses `hal` and `drivers`.
pub mod backend {
    #[cfg(target_os = "android")]
    pub use crate::drivers::android;
    #[cfg(target_os = "linux")]
    pub use crate::drivers::linux;
    pub use crate::drivers::mock;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub use crate::drivers::platform;
    #[cfg(target_os = "windows")]
    pub use crate::drivers::windows;
    pub use crate::hal::*;
}

pub use entities::error::{BleError, Result};
pub use entities::models::{
    CapabilityReport, CharacteristicUuid, ConnectionPriority, ConnectionState, DiscoveredPeer, GattCharacteristicSpec, GattEvent,
    GattServiceSpec, PeerAddress, RadioStatus, Role, ServiceUuid, WriteType,
};
pub use connection::peer_link::{
    LinkId, LinkRole, MaxLinks, PeerChannel, PeerLink, PeerLinkConfig, PeerLinkEvent, PeerStatus,
    RetryBudget,
};
pub use connection::Connection;
pub use hal::{Backend, BoxStream, GattConnection};
pub use roles::{Adapter, Central, Peripheral, ServerHandle};
