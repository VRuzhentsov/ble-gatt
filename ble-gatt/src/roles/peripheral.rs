use std::sync::Arc;

use crate::entities::error::Result;
use crate::entities::models::{CharacteristicUuid, GattServiceSpec, PeerAddress};
use crate::hal::Backend;

/// The peripheral role: hosts a GATT server that centrals connect to, and
/// advertises it. Created by `Adapter::peripheral`.
#[derive(Clone)]
pub struct Peripheral {
    backend: Arc<dyn Backend>,
}

impl Peripheral {
    pub(crate) fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Starts serving and advertising `service`. Both stop when the returned
    /// handle is dropped (ADR-0007 D6, as `bluer`'s `AdvertisementHandle`),
    /// so an early return cannot leave the radio advertising.
    pub async fn serve(&self, service: GattServiceSpec) -> Result<ServerHandle> {
        self.backend.advertise(service).await?;
        Ok(ServerHandle {
            backend: Some(self.backend.clone()),
        })
    }

    /// Notifies every subscribed central of `characteristic`'s new value.
    pub async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.backend.notify(characteristic, value).await
    }

    /// Notifies one central, on one connection (`session`) if given.
    pub async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        self.backend.notify_peer(peer, session, characteristic, value).await
    }

    /// Drops one central's connection to the server.
    pub async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        self.backend.disconnect_peer(peer, session).await
    }
}

/// A running GATT server and its advertisement. Dropping it stops both.
/// `Drop` cannot wait for the platform, so dropping stops them in the
/// background; [`ServerHandle::stop`] waits and reports the result.
pub struct ServerHandle {
    backend: Option<Arc<dyn Backend>>,
}

impl ServerHandle {
    /// Stops serving and advertising, and waits until the platform did.
    pub async fn stop(mut self) -> Result<()> {
        match self.backend.take() {
            Some(backend) => backend.stop_advertising().await,
            None => Ok(()),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(err) = backend.stop_advertising().await {
                        log::warn!("stopping a dropped GATT server failed: {err}");
                    }
                });
            }
            Err(_) => log::warn!("GATT server handle dropped outside a Tokio runtime; it keeps running"),
        }
    }
}
