//! A driver built on first use rather than at plugin setup.
//!
//! - **Android.** `.setup()` runs inside `tao`'s own Android context
//!   bring-up, before the JNI handles the driver needs exist; reading
//!   `ndk_context::android_context()` there panics with "android context was
//!   not initialized" (seen on hardware). The first command runs after the
//!   Activity is up.
//! - **Linux.** BlueZ needs a powered adapter. Building on first use means a
//!   machine with Bluetooth off or missing still starts the app.
//!
//! A failed construction is not cached: the next call tries again, so
//! turning Bluetooth on later is enough.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use ble_gatt::{
    Driver, BleError, BoxStream, CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattConnection,
    GattEvent, GattServiceSpec, PeerAddress, Result, ServiceUuid,
};
use tokio::sync::{broadcast, OnceCell};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

const EVENT_CHANNEL_CAPACITY: usize = 64;

type BuildFuture = Pin<Box<dyn Future<Output = Result<Arc<dyn Driver>>> + Send>>;
type Build = Box<dyn Fn() -> BuildFuture + Send + Sync>;

pub struct LazyDriver {
    build: Build,
    cell: OnceCell<Arc<dyn Driver>>,
    /// Events are republished through a channel owned by this wrapper, so a
    /// caller can subscribe before the driver exists. Returning the inner
    /// driver's stream directly meant an early `events()` got a stream that
    /// never yielded anything.
    events_tx: broadcast::Sender<GattEvent>,
}

impl LazyDriver {
    pub fn new<F, Fut>(build: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn Driver>>> + Send + 'static,
    {
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            build: Box::new(move || Box::pin(build())),
            cell: OnceCell::new(),
            events_tx,
        }
    }

    async fn inner(&self) -> Result<&Arc<dyn Driver>> {
        self.cell
            .get_or_try_init(|| async {
                let driver = (self.build)().await?;
                // Pump the real driver's events into our channel, so
                // subscriptions taken out before this point keep working.
                let mut source = driver.events();
                let sink = self.events_tx.clone();
                tokio::spawn(async move {
                    while let Some(event) = source.next().await {
                        // `send` errors only while nobody is subscribed,
                        // which is normal; stopping here would leave every
                        // later subscriber with a silent stream.
                        let _ = sink.send(event);
                    }
                });
                Ok::<_, BleError>(driver)
            })
            .await
    }
}

#[async_trait]
impl Driver for LazyDriver {
    async fn capabilities(&self) -> CapabilityReport {
        match self.inner().await {
            Ok(driver) => driver.capabilities().await,
            Err(err) => {
                log::error!("driver construction failed: {err}");
                CapabilityReport::default()
            }
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.inner().await?.scan(service).await
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        self.inner().await?.connect(peer).await
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        self.inner().await?.advertise(service).await
    }

    async fn update_advertisement(&self, service: GattServiceSpec) -> Result<()> {
        self.inner().await?.update_advertisement(service).await
    }

    async fn stop_advertising(&self) -> Result<()> {
        self.inner().await?.stop_advertising().await
    }

    async fn close(&self) -> Result<()> {
        // Nothing to release if the driver was never built; building one
        // just to close it would hold up an exit.
        match self.cell.get() {
            Some(driver) => driver.close().await,
            None => Ok(()),
        }
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.inner().await?.notify(characteristic, value).await
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        self.inner()
            .await?
            .notify_peer(peer, session, characteristic, value)
            .await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        self.inner().await?.disconnect_peer(peer, session).await
    }

    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        self.inner().await.ok()?.notify_max_len(peer, session).await
    }

    fn events(&self) -> BoxStream<GattEvent> {
        // Always a live subscription, whether or not the driver exists yet.
        let rx = self.events_tx.subscribe();
        Box::pin(BroadcastStream::new(rx).map(|item| match item {
            Ok(event) => event,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                GattEvent::Lagged { dropped: n }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_after_a_failed_build_and_then_keeps_the_driver() {
        let network = MockNetwork::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let lazy = LazyDriver::new(move || {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            let network = network.clone();
            async move {
                if attempt == 0 {
                    return Err(BleError::AdapterUnavailable("off".into()));
                }
                let driver = MockDriver::new(PeerAddress("AA".into()), network, CapabilityReport::default());
                Ok(Arc::new(driver) as Arc<dyn Driver>)
            }
        });

        assert!(lazy.stop_advertising().await.is_err());
        assert!(lazy.stop_advertising().await.is_ok());
        assert!(lazy.stop_advertising().await.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    /// Closing on exit must not build a driver the app never used.
    #[tokio::test]
    async fn close_does_not_build_the_driver() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let lazy = LazyDriver::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Err(BleError::AdapterUnavailable("off".into())) }
        });
        assert!(lazy.close().await.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }
}
