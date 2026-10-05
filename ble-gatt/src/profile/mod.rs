//! The datagram profile: whole messages over a GATT connection, however
//! small the MTU (layer: Use Cases; see `docs/architecture.md`). "Profile"
//! as in the Bluetooth specification: a set of GATT services and the rules
//! for using them for one purpose.

pub mod datagram;
