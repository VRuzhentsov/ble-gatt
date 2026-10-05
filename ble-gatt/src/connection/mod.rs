//! One connection to one peer, kept healthy: its state machine and the
//! `PeerLink` handle (layer: Use Cases; see `docs/architecture.md`).

mod handle;
pub(crate) mod link_state;
pub mod peer_link;

pub use handle::Connection;
