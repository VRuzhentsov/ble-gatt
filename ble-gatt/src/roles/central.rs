use std::sync::Arc;

use crate::connection::Connection;
use crate::entities::error::Result;
use crate::entities::models::{DiscoveredPeer, PeerAddress, ServiceUuid};
use crate::hal::{Driver, BoxStream};

/// The central role: finds peripherals and connects to them. Created by
/// `Adapter::central`.
#[derive(Clone)]
pub struct Central {
    driver: Arc<dyn Driver>,
}

impl Central {
    pub(crate) fn new(driver: Arc<dyn Driver>) -> Self {
        Self { driver }
    }

    /// Peripherals advertising `service`. The scan runs while the stream is
    /// held and stops when it is dropped (ADR-0007 D6).
    pub async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.driver.scan(service).await
    }

    /// Connects to `peer`. The returned `Connection` publishes its state
    /// (ADR-0007 D5).
    pub async fn connect(&self, peer: &PeerAddress) -> Result<Connection> {
        let events = self.driver.events();
        let inner = self.driver.connect(peer).await?;
        Ok(Connection::new(inner, events))
    }
}
