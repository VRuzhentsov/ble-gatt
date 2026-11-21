//! A single pre-admitted native cleanup, polled by the existing worker JoinSet.
use crate::{
    AttemptError, BleError, BoxStream, CharacteristicUuid, ConnectionPriority, GattConnection,
    PeerAddress, Result, WriteType,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};

type Native = Box<dyn GattConnection>;
#[derive(Clone)]
pub(crate) struct Cleanup {
    sender: Arc<Mutex<Option<oneshot::Sender<Native>>>>,
    result: watch::Receiver<Option<std::result::Result<(), Arc<BleError>>>>,
}
impl Cleanup {
    pub(super) fn reserved() -> (Self, impl std::future::Future<Output = Result<()>> + Send) {
        let (sender, receiver) = oneshot::channel::<Native>();
        let (result, outcome) = watch::channel(None);
        (
            Self {
                sender: Arc::new(Mutex::new(Some(sender))),
                result: outcome,
            },
            async move {
                let outcome = match receiver.await {
                    Ok(mut native) => {
                        tokio::time::timeout(Duration::from_secs(5), native.disconnect())
                            .await
                            .unwrap_or(Err(AttemptError::TimedOut.into()))
                    }
                    Err(_) => Ok(()), // acquisition failed before a native connection existed
                };
                // An explicit native NotConnected confirms this owned session is
                // already absent (for example after peer-initiated retirement).
                let outcome = match outcome {
                    Err(BleError::NotConnected(_)) => Ok(()),
                    other => other,
                };
                let outcome = outcome.map_err(Arc::new);
                result.send_replace(Some(outcome.clone()));
                outcome.map_err(|error| error.copy_error())
            },
        )
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
                return outcome
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| error.copy_error());
            }
            result
                .changed()
                .await
                .map_err(|_| BleError::Gatt("acquired cleanup owner ended".into()))?;
        }
    }
    pub(super) fn own(&self, native: Native) -> Owned {
        Owned {
            peer: native.peer(),
            session: native.session(),
            mtu: native.att_mtu(),
            maximum: native.max_write_len(),
            native: Some(native),
            cleanup: self.clone(),
        }
    }
}

// Both the worker and the awaiting connection observe one native result.
// Keep its typed contract without changing the public BleError API.
impl BleError {
    pub(super) fn copy_error(&self) -> BleError {
        match self {
            BleError::Attempt(reason) => BleError::Attempt(*reason),
            BleError::Protocol(message) => BleError::Protocol(message.clone()),
            BleError::AdapterUnavailable(message) => BleError::AdapterUnavailable(message.clone()),
            BleError::AdapterBusy(message) => BleError::AdapterBusy(message.clone()),
            BleError::PeripheralUnsupported => BleError::PeripheralUnsupported,
            BleError::Unsupported(message) => BleError::Unsupported(message.clone()),
            BleError::ConnectFailed { peer, reason } => BleError::ConnectFailed {
                peer: peer.clone(),
                reason: reason.clone(),
            },
            BleError::Gatt(message) => BleError::Gatt(message.clone()),
            BleError::ReceiveGap(message) => BleError::ReceiveGap(message.clone()),
            BleError::GattBusy(message) => BleError::GattBusy(message.clone()),
            BleError::NotConnected(message) => BleError::NotConnected(message.clone()),
            BleError::Transport(message) => BleError::Transport(message.clone()),
        }
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
        if let Some(native) = self.native.take() {
            self.cleanup.submit(native);
        }
    }
}
#[async_trait]
impl GattConnection for Owned {
    fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }
    fn session(&self) -> Option<u64> {
        self.session
    }
    // The native MTU can grow after connect (Linux reports 23 until
    // negotiation finishes), so read it live; the snapshot only serves
    // after the native connection is handed to cleanup.
    fn att_mtu(&self) -> u16 {
        self.native
            .as_ref()
            .map_or(self.mtu, |native| native.att_mtu())
    }
    fn max_write_len(&self) -> usize {
        self.native
            .as_ref()
            .map_or(self.maximum, |native| native.max_write_len())
    }
    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        self.native
            .as_mut()
            .expect("live connection")
            .read(characteristic)
            .await
    }
    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.native
            .as_mut()
            .expect("live connection")
            .write(characteristic, value)
            .await
    }
    async fn write_with_type(
        &mut self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
        kind: WriteType,
    ) -> Result<()> {
        self.native
            .as_mut()
            .expect("live connection")
            .write_with_type(characteristic, value, kind)
            .await
    }
    async fn subscribe(
        &mut self,
        characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.native
            .as_mut()
            .expect("live connection")
            .subscribe(characteristic)
            .await
    }
    async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        self.native
            .as_mut()
            .expect("live connection")
            .request_connection_priority(priority)
            .await
    }
    async fn disconnect(&mut self) -> Result<()> {
        if let Some(native) = self.native.take() {
            self.cleanup.submit(native);
        }
        self.cleanup.wait().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Refused(u8);
    #[async_trait]
    impl GattConnection for Refused {
        fn peer(&self) -> PeerAddress {
            PeerAddress("refused".into())
        }
        fn att_mtu(&self) -> u16 {
            23
        }
        async fn read(&mut self, _: CharacteristicUuid) -> Result<Vec<u8>> {
            unreachable!()
        }
        async fn write_with_type(
            &mut self,
            _: CharacteristicUuid,
            _: Vec<u8>,
            _: WriteType,
        ) -> Result<()> {
            unreachable!()
        }
        async fn subscribe(&mut self, _: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
            unreachable!()
        }
        async fn disconnect(&mut self) -> Result<()> {
            Err(match self.0 {
                0 => BleError::Unsupported("native disconnect refused".into()),
                1 => BleError::NotConnected("owned session already absent".into()),
                _ => BleError::Gatt("actual native disconnect failure".into()),
            })
        }
    }

    struct Negotiating(Arc<std::sync::atomic::AtomicU16>);
    #[async_trait]
    impl GattConnection for Negotiating {
        fn peer(&self) -> PeerAddress {
            PeerAddress("negotiating".into())
        }
        fn att_mtu(&self) -> u16 {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
        async fn read(&mut self, _: CharacteristicUuid) -> Result<Vec<u8>> {
            unreachable!()
        }
        async fn write_with_type(
            &mut self,
            _: CharacteristicUuid,
            _: Vec<u8>,
            _: WriteType,
        ) -> Result<()> {
            unreachable!()
        }
        async fn subscribe(&mut self, _: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
            unreachable!()
        }
        async fn disconnect(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn owned_reports_the_mtu_negotiated_after_connect() {
        let mtu = Arc::new(std::sync::atomic::AtomicU16::new(23));
        let (cleanup, completion) = Cleanup::reserved();
        let task = tokio::spawn(completion);
        let mut owned = cleanup.own(Box::new(Negotiating(mtu.clone())));
        assert_eq!((owned.att_mtu(), owned.max_write_len()), (23, 20));
        mtu.store(247, std::sync::atomic::Ordering::SeqCst);
        assert_eq!((owned.att_mtu(), owned.max_write_len()), (247, 244));
        owned.disconnect().await.unwrap();
        mtu.store(517, std::sync::atomic::Ordering::SeqCst);
        // After hand-off to cleanup, the snapshot taken at own() answers.
        assert_eq!((owned.att_mtu(), owned.max_write_len()), (23, 20));
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn owned_disconnect_preserves_the_native_unsupported_contract() {
        let (cleanup, completion) = Cleanup::reserved();
        let task = tokio::spawn(completion);
        let mut owned = cleanup.own(Box::new(Refused(0)));
        let outcome = owned.disconnect().await;
        assert!(
            matches!(outcome, Err(BleError::Unsupported(_))),
            "native refusal must remain typed: {outcome:?}"
        );
        assert!(matches!(task.await.unwrap(), Err(BleError::Unsupported(_))));
    }

    #[tokio::test]
    async fn only_typed_native_absence_is_idempotent_not_a_gatt_failure() {
        for error in [1, 2] {
            let (cleanup, completion) = Cleanup::reserved();
            let task = tokio::spawn(completion);
            let mut owned = cleanup.own(Box::new(Refused(error)));
            let outcome = owned.disconnect().await;
            let repeated = owned.disconnect().await;
            let completion = task.await.unwrap();
            if error == 1 {
                assert!(outcome.is_ok());
                assert!(repeated.is_ok());
                assert!(completion.is_ok());
            } else {
                assert!(matches!(outcome, Err(BleError::Gatt(_))));
                assert!(matches!(repeated, Err(BleError::Gatt(_))));
                assert!(matches!(completion, Err(BleError::Gatt(_))));
            }
        }
    }
}
