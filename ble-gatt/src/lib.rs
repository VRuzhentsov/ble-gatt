//! Async, transport-agnostic BLE GATT primitives (central + peripheral role)
//! with no Tauri dependency — usable from any Tokio-based Rust program, not
//! just Tauri apps. See `hal::Driver` for the platform port and
//! `drivers::mock::MockDriver` for a CI-safe, radio-free stand-in.
//!
//! Modules follow the layer map in `docs/architecture.md` (Clean
//! Architecture layers, Bluetooth-specification module names):
//!
//! | Module | Layer |
//! |---|---|
//! | `entities` | Entities |
//! | `hal`, `roles`, `connection`, `profile`, `power` | Use Cases |
//! | `drivers` | Frameworks & Drivers |

pub mod connection;
pub mod drivers;
pub mod entities;
pub mod hal;
pub mod power;
pub mod profile;
pub mod roles;

pub use connection::peer_link;
pub use entities::{error, models};
pub use profile::datagram;

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
pub use hal::{Driver, BoxStream, GattConnection};
pub use roles::{Adapter, Central, Peripheral, ServerHandle};
