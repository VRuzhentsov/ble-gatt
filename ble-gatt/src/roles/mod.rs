//! The BLE roles as objects (layer: Use Cases; see `docs/architecture.md`).
//!
//! An [`Adapter`] stands for the device's Bluetooth adapter (ADR-0007 D7,
//! as `bluest`'s `Adapter`); the roles are created from it, one object per
//! role (D4), as every platform splits them: [`Central`] dials out,
//! [`Peripheral`] hosts a GATT server and advertises it.
//!
//! Today's driver port (`hal::Backend`) still offers the GATT server and
//! advertising as one operation, so there is no separate `Advertiser` yet;
//! it comes when the drivers split that operation.

mod adapter;
mod central;
mod peripheral;

pub use adapter::Adapter;
pub use central::Central;
pub use peripheral::{Peripheral, ServerHandle};
