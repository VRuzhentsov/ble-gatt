use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use super::{Central, Peripheral};
use crate::entities::models::{CapabilityReport, GattEvent, RadioStatus};
use crate::hal::{Backend, BoxStream};

/// The device's Bluetooth adapter: whether it is usable, published as it
/// changes, and what it can do (ADR-0007 D7). Follows `bluest`'s `Adapter`
/// (`Adapter::default()`, `wait_available()`, `events()`), Android's
/// `BluetoothAdapter` and BlueZ's `Adapter1`. Cheap to clone.
#[derive(Clone)]
pub struct Adapter {
    inner: Arc<Inner>,
}

struct Inner {
    backend: Arc<dyn Backend>,
    status: watch::Receiver<RadioStatus>,
    watcher: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

impl Adapter {
    /// An adapter over an injected driver (ADR-0007 D3): a platform driver,
    /// the mock radio, or one an application wraps itself.
    pub async fn new(backend: Arc<dyn Backend>) -> Self {
        // Subscribe before reading the current status, so a change between
        // the two is not lost.
        let events = backend.events();
        let (status_tx, status) = watch::channel(backend.radio_status().await);
        let watcher = tokio::spawn(publish_status(events, status_tx));
        Self {
            inner: Arc::new(Inner {
                backend,
                status,
                watcher,
            }),
        }
    }

    /// The adapter of the platform this program runs on.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub async fn platform() -> crate::Result<Self> {
        Ok(Self::new(crate::drivers::platform().await?).await)
    }

    /// The driver behind this adapter, for the raw `hal` API.
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.inner.backend.clone()
    }

    /// Which roles the hardware supports.
    pub async fn capabilities(&self) -> CapabilityReport {
        self.inner.backend.capabilities().await
    }

    /// Whether the radio is usable, published (ADR-0007 D5): each receiver
    /// sees the current value at once and every change after it.
    pub fn status(&self) -> watch::Receiver<RadioStatus> {
        self.inner.status.clone()
    }

    /// Waits until the radio is on, as `bluest`'s `wait_available`.
    pub async fn wait_available(&self) {
        let mut status = self.status();
        let _ = status.wait_for(|status| *status == RadioStatus::On).await;
    }

    /// Every connection and radio event the driver reports.
    pub fn events(&self) -> BoxStream<GattEvent> {
        self.inner.backend.events()
    }

    /// The central role: scan and dial out.
    pub fn central(&self) -> Central {
        Central::new(self.inner.backend.clone())
    }

    /// The peripheral role: host a GATT server and advertise it.
    pub fn peripheral(&self) -> Peripheral {
        Peripheral::new(self.inner.backend.clone())
    }
}

async fn publish_status(mut events: BoxStream<GattEvent>, status_tx: watch::Sender<RadioStatus>) {
    while let Some(event) = events.next().await {
        if let GattEvent::RadioChanged { status } = event {
            status_tx.send_if_modified(|current| {
                let changed = *current != status;
                *current = status;
                changed
            });
        }
    }
}
