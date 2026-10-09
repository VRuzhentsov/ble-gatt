use std::sync::Arc;

use tokio_stream::StreamExt;

use crate::entities::error::Result;
use crate::entities::models::{CharacteristicUuid, GattEvent, GattServiceSpec, PeerAddress, RadioStatus};
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
    ///
    /// Subscribing before `advertise` (rather than after) matches this
    /// crate's own "subscribe before the state-changing call" convention —
    /// see `datagram::connect`/`serve` — so a radio recovery landing in the
    /// gap is not missed.
    pub async fn serve(&self, service: GattServiceSpec) -> Result<ServerHandle> {
        let events = self.backend.events();
        self.backend.advertise(service.clone()).await?;
        let recovery = spawn_recovery(self.backend.clone(), service, events);
        Ok(ServerHandle {
            backend: Some(self.backend.clone()),
            recovery: Some(recovery),
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
    /// Re-registers the service on a radio recovery — see `spawn_recovery`.
    /// Aborted on `stop`/`drop` so it cannot re-advertise after the caller
    /// asked this server to stop.
    recovery: Option<tokio::task::JoinHandle<()>>,
}

impl ServerHandle {
    /// Stops serving and advertising, and waits until the platform did.
    pub async fn stop(mut self) -> Result<()> {
        if let Some(recovery) = self.recovery.take() {
            recovery.abort();
        }
        match self.backend.take() {
            Some(backend) => backend.stop_advertising().await,
            None => Ok(()),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(recovery) = self.recovery.take() {
            recovery.abort();
        }
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

/// Re-registers `service` whenever the radio reports `On` after this
/// subscription started — the backend's way of saying it just regained a
/// usable adapter (powered back on, or on Linux, `bluetoothd` itself
/// crashed and restarted). Either way BlueZ/the platform forgot whatever
/// GATT application and advertisement were live, and nothing else in this
/// crate re-registers them.
///
/// Every `On` re-advertises, even one this call's own prior success already
/// covers — cheap and idempotent-enough (the same operation a consumer like
/// Fini was already calling by hand before the generation-race fix), and
/// simpler than tracking whether a matching `Off` came first.
fn spawn_recovery(
    backend: Arc<dyn Backend>, service: GattServiceSpec, mut events: crate::hal::BoxStream<GattEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if let GattEvent::RadioChanged { status: RadioStatus::On } = event {
                log::info!("serve: radio recovered, re-registering service {}", service.uuid.0);
                if let Err(err) = backend.advertise(service.clone()).await {
                    log::warn!("serve: failed to re-register after radio recovery: {err}");
                }
            }
        }
    })
}
