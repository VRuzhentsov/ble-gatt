//! A single pre-admitted native cleanup, polled by the existing worker JoinSet.
use crate::{AttemptError, BleError, BoxStream, CharacteristicUuid, ConnectionPriority, GattConnection, PeerAddress, Result, WriteType};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};

type Native = Box<dyn GattConnection>;
#[derive(Clone)]
pub(crate) struct Cleanup {
    sender: Arc<Mutex<Option<oneshot::Sender<Native>>>>,
    result: watch::Receiver<Option<std::result::Result<(), String>>>,
}
impl Cleanup {
    pub(super) fn reserved() -> (Self, impl std::future::Future<Output = Result<()>> + Send) {
        let (sender, receiver) = oneshot::channel::<Native>();
        let (result, outcome) = watch::channel(None);
        (Self { sender: Arc::new(Mutex::new(Some(sender))), result: outcome }, async move {
            let outcome = match receiver.await {
                Ok(mut native) => tokio::time::timeout(Duration::from_secs(5), native.disconnect()).await
                    .unwrap_or(Err(AttemptError::TimedOut.into())),
                Err(_) => Ok(()), // acquisition failed before a native connection existed
            };
            // An explicit native NotConnected confirms this owned session is
            // already absent (for example after peer-initiated retirement).
            let outcome = match outcome { Err(BleError::NotConnected(_)) => Ok(()), other => other };
            result.send_replace(Some(outcome.as_ref().map(|_| ()).map_err(ToString::to_string)));
            outcome
        })
    }
    fn submit(&self, native: Native) {
        if let Some(sender) = self.sender.lock().unwrap().take() {
            // Admission predates acquisition; there is exactly one owned native
            // connection and one slot. The worker drains before runtime destruction.
            if sender.send(native).is_err() {
                log::error!("managed cleanup owner ended before its acquired connection");
            }
        }
    }
    async fn wait(&self) -> Result<()> {
        let mut result = self.result.clone();
        loop {
            if let Some(outcome) = result.borrow_and_update().as_ref() {
                return outcome.as_ref().map(|_| ()).map_err(|error| BleError::Gatt(error.clone()));
            }
            result.changed().await.map_err(|_| BleError::Gatt("acquired cleanup owner ended".into()))?;
        }
    }
    pub(super) fn own(&self, native: Native) -> Owned {
        Owned { peer: native.peer(), session: native.session(), mtu: native.att_mtu(), maximum: native.max_write_len(), native: Some(native), cleanup: self.clone() }
    }
}

pub(super) struct Owned {
    native: Option<Native>,
    cleanup: Cleanup,
    peer: PeerAddress,
    session: Option<u64>,
    mtu: u16,
    maximum: usize,
}
impl Drop for Owned {
    fn drop(&mut self) {
        if let Some(native) = self.native.take() { self.cleanup.submit(native); }
    }
}
#[async_trait]
impl GattConnection for Owned {
    fn peer(&self) -> PeerAddress { self.peer.clone() }
    fn session(&self) -> Option<u64> { self.session }
    fn att_mtu(&self) -> u16 { self.mtu }
    fn max_write_len(&self) -> usize { self.maximum }
    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        self.native.as_mut().expect("live connection").read(characteristic).await
    }
    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.native.as_mut().expect("live connection").write(characteristic, value).await
    }
    async fn write_with_type(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>, kind: WriteType) -> Result<()> {
        self.native.as_mut().expect("live connection").write_with_type(characteristic, value, kind).await
    }
    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.native.as_mut().expect("live connection").subscribe(characteristic).await
    }
    async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        self.native.as_mut().expect("live connection").request_connection_priority(priority).await
    }
    async fn disconnect(&mut self) -> Result<()> {
        if let Some(native) = self.native.take() { self.cleanup.submit(native); }
        self.cleanup.wait().await
    }
}
