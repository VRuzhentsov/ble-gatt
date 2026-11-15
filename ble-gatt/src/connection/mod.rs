//! One connection to one peer, kept healthy: its state machine and the
//! `PeerLink` handle (layer: Use Cases; see `docs/architecture.md`).

mod acquired_cleanup;
mod handle;
mod managed_codec;
mod managed_driver;
#[cfg(test)]
mod managed_tests;
mod managed_worker;
pub mod peer_link;

pub use handle::Connection;
