use std::sync::Arc;

use crate::connection::Connection;
use crate::entities::error::Result;
use crate::entities::models::{DiscoveredPeer, PeerAddress, ServiceUuid};
use crate::hal::{Backend, BoxStream};

/// The central role: finds peripherals and connects to them. Created by
/// `Adapter::central`.
#[derive(Clone)]
pub struct Central {
    backend: Arc<dyn Backend>,
}

impl Central {
    pub(crate) fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Peripherals advertising `service`. The scan runs while the stream is
    /// held and stops when it is dropped (ADR-0007 D6).
    pub async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.backend.scan(service).await
    }

    /// Connects to `peer`. The returned `Connection` publishes its state
    /// (ADR-0007 D5).
    pub async fn connect(&self, peer: &PeerAddress) -> Result<Connection> {
        let events = self.backend.events();
        let inner = self.backend.connect(peer).await?;
        Ok(Connection::new(inner, events))
    }
}
