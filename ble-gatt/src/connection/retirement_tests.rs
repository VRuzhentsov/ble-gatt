//! The public last-use barrier over the real managed pump and owned native cleanup.
use crate::{
    drivers::mock::{MockDriver, MockNetwork},
    *,
};
use async_trait::async_trait;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::Notify;
use tokio_stream::StreamExt;

#[derive(Default)]
struct Gate {
    entered: Notify,
    resume: Notify,
    done: AtomicBool,
}
struct ResumeOnDrop(Arc<Gate>);
impl Drop for ResumeOnDrop {
    fn drop(&mut self) {
        self.0.resume.notify_one();
    }
}
struct Native {
    inner: Box<dyn GattConnection>,
    gate: Arc<Gate>,
}
#[async_trait]
impl GattConnection for Native {
    fn peer(&self) -> PeerAddress {
        self.inner.peer()
    }
    fn session(&self) -> Option<u64> {
        self.inner.session()
    }
    fn att_mtu(&self) -> u16 {
        self.inner.att_mtu()
    }
    fn max_write_len(&self) -> usize {
        self.inner.max_write_len()
    }
    async fn read(&mut self, c: CharacteristicUuid) -> Result<Vec<u8>> {
        self.inner.read(c).await
    }
    async fn write_with_type(
        &mut self,
        c: CharacteristicUuid,
        v: Vec<u8>,
        t: WriteType,
    ) -> Result<()> {
        self.inner.write_with_type(c, v, t).await
    }
    async fn subscribe(&mut self, c: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.inner.subscribe(c).await
    }
    async fn disconnect(&mut self) -> Result<()> {
        self.gate.entered.notify_one();
        self.gate.resume.notified().await;
        let result = self.inner.disconnect().await;
        self.gate.done.store(true, SeqCst);
        result
    }
}
struct Wrapped {
    inner: MockDriver,
    gate: Arc<Gate>,
    gated_peer: PeerAddress,
}
#[async_trait]
impl Driver for Wrapped {
    async fn capabilities(&self) -> CapabilityReport {
        self.inner.capabilities().await
    }
    async fn scan(&self, s: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.inner.scan(s).await
    }
    async fn connect(&self, p: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let inner = self.inner.connect(p).await?;
        if p == &self.gated_peer {
            Ok(Box::new(Native {
                inner,
                gate: self.gate.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    async fn advertise(&self, s: GattServiceSpec) -> Result<()> {
        self.inner.advertise(s).await
    }
    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service()
    }
    async fn set_characteristic_value(&self, c: CharacteristicUuid, v: Vec<u8>) -> Result<()> {
        self.inner.set_characteristic_value(c, v).await
    }
    async fn set_advertising_enabled(&self, v: bool) -> Result<()> {
        self.inner.set_advertising_enabled(v).await
    }
    async fn stop_advertising(&self) -> Result<()> {
        self.inner.stop_advertising().await
    }
    async fn notify(&self, c: CharacteristicUuid, v: Vec<u8>) -> Result<()> {
        self.inner.notify(c, v).await
    }
    async fn notify_peer(
        &self,
        p: &PeerAddress,
        s: Option<u64>,
        c: CharacteristicUuid,
        v: Vec<u8>,
    ) -> Result<()> {
        self.inner.notify_peer(p, s, c, v).await
    }
    async fn disconnect_peer(&self, p: &PeerAddress, s: Option<u64>) -> Result<()> {
        self.inner.disconnect_peer(p, s).await
    }
    async fn notify_max_len(&self, p: &PeerAddress, s: Option<u64>) -> Option<usize> {
        self.inner.notify_max_len(p, s).await
    }
    fn events(&self) -> BoxStream<GattEvent> {
        self.inner.events()
    }
    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.inner.lifecycle_events()
    }
}
async fn assert_pending(future: Pin<&mut impl Future>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "last client release returned while its native cleanup was gated"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn public_endpoint_disconnect_with_gated_crossed_loser_must_not_kill_worker() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let network = MockNetwork::new();
        let gate = Arc::new(Gate::default());
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = || {
            PeerLinkConfig::new(datagram::DatagramConfig::new(
                ServiceUuid(uuid::Uuid::from_u128(11)),
                CharacteristicUuid(uuid::Uuid::from_u128(12)),
            ))
        };
        let address_a = PeerAddress("crossed-a".into());
        let address_b = PeerAddress("crossed-b".into());
        let a = PeerLink::with_driver(
            Arc::new(Wrapped {
                inner: MockDriver::new(address_a.clone(), network.clone(), caps),
                gate: gate.clone(),
                gated_peer: address_b.clone(),
            }),
            config(),
        );
        let other_address = PeerAddress("crossed-other".into());
        let other = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                other_address.clone(),
                network.clone(),
                caps,
            )),
            config(),
        );
        let b = PeerLink::with_driver(
            Arc::new(MockDriver::new(address_b.clone(), network, caps)),
            config(),
        );
        let _resume = ResumeOnDrop(gate.clone());
        let mut incoming_a = a.incoming().unwrap();
        let _incoming_b = b.incoming().unwrap();
        a.set_advertising(true).await.unwrap();
        b.set_advertising(true).await.unwrap();
        super::peer_link::tests::nonce(&a, i32::MIN).await;
        super::peer_link::tests::nonce(&b, i32::MAX).await;
        let (loser, winner) = tokio::join!(a.request(&address_b), b.request(&address_a));
        // The losing public outgoing request is not fulfilled by the incoming winner.
        match loser {
            Err(BleError::Attempt(AttemptError::Superseded)) => {}
            Ok(loser) => assert!(matches!(
                loser.accept().await,
                Err(BleError::Attempt(AttemptError::Superseded))
            )),
            Err(error) => panic!("unexpected outgoing loser: {error:?}"),
        }
        let incoming = incoming_a.next().await.unwrap();
        let (left, right) = tokio::join!(incoming.accept(), winner.unwrap().accept());
        let left = left.unwrap();
        let right = right.unwrap();
        gate.entered.notified().await;
        right.send(vec![31]).await.unwrap();
        assert_eq!(left.recv().await.unwrap().unwrap(), vec![31]);
        assert!(!gate.done.load(SeqCst));
        let mut other_incoming = other.incoming().unwrap();
        other.set_advertising(true).await.unwrap();
        let (unrelated, other_remote) = tokio::join!(
            async {
                a.request(&other_address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { other_incoming.next().await.unwrap().accept().await.unwrap() },
        );
        let mut disconnect = Box::pin(a.disconnect(b.endpoint_id()));
        assert_pending(disconnect.as_mut()).await;
        // Round-trip the same worker and exchange unrelated payloads after the
        // aggregate has been dispatched, with the old native gate still held.
        tokio::time::timeout(Duration::from_millis(300), async {
            a.disconnect(EndpointId::from_bytes([0; 16])).await.unwrap();
            a.set_advertising(false).await.unwrap();
            other_remote.send(vec![32]).await.unwrap();
            assert_eq!(unrelated.recv().await.unwrap().unwrap(), vec![32]);
            unrelated.send(vec![33]).await.unwrap();
            assert_eq!(other_remote.recv().await.unwrap().unwrap(), vec![33]);
        })
        .await
        .expect("crossed cleanup cannot park another peer/control");
        assert_pending(disconnect.as_mut()).await;
        assert!(matches!(
            a.request(&address_b).await,
            Err(BleError::AdapterBusy(_))
        ));
        gate.resume.notify_one();
        let result = disconnect.await;
        assert!(
            result.is_ok(),
            "endpoint disconnect killed worker or dropped reply: {result:?}"
        );
        assert!(gate.done.load(SeqCst));
        a.close().await.unwrap();
        b.close().await.unwrap();
        other.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn last_client_release_after_peer_disconnect_joins_gated_native_cleanup() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let network = MockNetwork::new();
        let gate = Arc::new(Gate::default());
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = || {
            PeerLinkConfig::new(datagram::DatagramConfig::new(
                ServiceUuid(uuid::Uuid::from_u128(11)),
                CharacteristicUuid(uuid::Uuid::from_u128(12)),
            ))
        };
        let address = PeerAddress("barrier-server".into());
        let server = PeerLink::with_driver(
            Arc::new(MockDriver::new(address.clone(), network.clone(), caps)),
            config(),
        );
        let other_address = PeerAddress("barrier-other".into());
        let other = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                other_address.clone(),
                network.clone(),
                caps,
            )),
            config(),
        );
        let client = PeerLink::with_driver(
            Arc::new(Wrapped {
                inner: MockDriver::new(PeerAddress("barrier-client".into()), network, caps),
                gate: gate.clone(),
                gated_peer: address.clone(),
            }),
            config(),
        );
        // Unblock native Drop even if an assertion fails on the original core.
        let _resume = ResumeOnDrop(gate.clone());
        client.set_advertising(true).await.unwrap();
        let mut incoming = server.incoming().unwrap();
        server.set_advertising(true).await.unwrap();
        let (old_remote, old) = tokio::join!(
            async {
                client
                    .request(&address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { incoming.next().await.unwrap().accept().await.unwrap() }
        );
        let mut other_incoming = other.incoming().unwrap();
        other.set_advertising(true).await.unwrap();
        let (unrelated, other_remote) = tokio::join!(
            async {
                client
                    .request(&other_address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { other_incoming.next().await.unwrap().accept().await.unwrap() }
        );
        old_remote.send(vec![0]).await.unwrap();
        assert_eq!(old.recv().await.unwrap().unwrap(), vec![0]);
        old.disconnect().await.unwrap();
        gate.entered.notified().await;
        assert_eq!(
            old_remote.close_reason(),
            Some(PeerChannelCloseReason::PeerDisconnect)
        );
        // Repeated cancelled public disconnects must not accumulate replies or
        // cancel the native cleanup owned by this generation.
        for _ in 0..100 {
            let mut cancelled = Box::pin(old_remote.disconnect());
            assert_pending(cancelled.as_mut()).await;
            client
                .disconnect(EndpointId::from_bytes([0; 16]))
                .await
                .unwrap();
            assert_pending(cancelled.as_mut()).await;
            drop(cancelled);
        }
        let mut release = Box::pin(old_remote.release());
        assert_pending(release.as_mut()).await;
        tokio::time::timeout(Duration::from_millis(300), async {
            // This command round-trip also proves the earlier release command was dispatched.
            client.set_advertising(false).await.unwrap();
            client
                .disconnect(EndpointId::from_bytes([0; 16]))
                .await
                .unwrap();
            other_remote.send(vec![17]).await.unwrap();
            assert_eq!(unrelated.recv().await.unwrap().unwrap(), vec![17]);
            unrelated.send(vec![18]).await.unwrap();
            assert_eq!(other_remote.recv().await.unwrap().unwrap(), vec![18]);
        })
        .await
        .expect("generation retirement cannot park control or another peer");
        assert_pending(release.as_mut()).await;
        assert!(!gate.done.load(SeqCst));
        assert!(matches!(
            client.request(&address).await,
            Err(BleError::AdapterBusy(_))
        ));
        gate.resume.notify_one();
        release.await.unwrap();
        assert!(gate.done.load(SeqCst));
        let (new_remote, new) = tokio::join!(
            async {
                client
                    .request(&address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { incoming.next().await.unwrap().accept().await.unwrap() }
        );
        new_remote.send(vec![19]).await.unwrap();
        assert_eq!(new.recv().await.unwrap().unwrap(), vec![19]);
        // Only the first native disconnect needs gating.
        gate.resume.notify_one();
        client.close().await.unwrap();
        server.close().await.unwrap();
        other.close().await.unwrap();
    })
    .await
    .unwrap();
}
