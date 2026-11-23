//! Managed symmetric establishment. Raw GATT and datagram paths remain independent.
//! Identifiers correlate attempts. They do not authenticate or encrypt traffic.
use super::{
    managed_codec::Record,
    managed_worker::{Command, TransportCommand, Worker},
};
use crate::datagram::DatagramConfig;
use crate::{BleError, BoxStream, ConnectionPriority, DiscoveredPeer, Driver, PeerAddress, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// Random identifier for one worker lifetime. This is not an authenticated identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointId(pub(crate) [u8; 16]);
impl EndpointId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct MaxLinks(pub usize);
impl Default for MaxLinks {
    fn default() -> Self {
        Self(4)
    }
}
pub struct PeerLinkConfig {
    pub datagram: DatagramConfig,
    pub max_links: MaxLinks,
}
impl PeerLinkConfig {
    pub fn new(datagram: DatagramConfig) -> Self {
        Self {
            datagram,
            max_links: MaxLinks::default(),
        }
    }
}
pub(super) type Reply<T> = oneshot::Sender<Result<T>>;
pub(super) type ChannelTable = Arc<Mutex<HashMap<PeerAddress, Weak<ChannelState>>>>;

pub struct PeerLink {
    endpoint: EndpointId,
    commands: mpsc::Sender<Command>,
    incoming: Mutex<Option<mpsc::Receiver<ConnectionRequest>>>,
    channels: ChannelTable,
    shutdown: CancellationToken,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Drop for PeerLink {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }
}
impl PeerLink {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "windows"))]
    pub fn new(config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(None, config)
    }
    pub(super) fn closed_error() -> BleError {
        BleError::NotConnected("managed peer".into())
    }
    pub(super) fn protocol_error(reason: &str) -> BleError {
        BleError::Protocol(reason.into())
    }

    pub fn with_driver(driver: Arc<dyn Driver>, config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(Some(driver), config)
    }
    fn spawn(driver: Option<Arc<dyn Driver>>, config: PeerLinkConfig) -> Arc<Self> {
        let endpoint = EndpointId(*uuid::Uuid::new_v4().as_bytes());
        let (commands, receiver) = mpsc::channel(64);
        let (incoming, incoming_receiver) =
            mpsc::channel(config.datagram.accept_queue_depth.max(1));
        let channels = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = CancellationToken::new();
        let worker = Worker::new(
            endpoint,
            config,
            driver,
            commands.clone(),
            receiver,
            incoming,
            channels.clone(),
            shutdown.clone(),
        );
        let join = std::thread::Builder::new()
            .name("ble-gatt-peerlink".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("PeerLink runtime");
                runtime.block_on(worker.run());
            })
            .expect("PeerLink thread");
        Arc::new(Self {
            endpoint,
            commands,
            incoming: Mutex::new(Some(incoming_receiver)),
            channels,
            shutdown,
            worker: Mutex::new(Some(join)),
        })
    }
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint
    }
    pub fn incoming(&self) -> Result<BoxStream<ConnectionRequest>> {
        let receiver = self
            .incoming
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| BleError::AdapterBusy("incoming already claimed".into()))?;
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
    pub async fn request(&self, peer: &PeerAddress) -> Result<ConnectionRequest> {
        let cancellation = CancellationToken::new();
        let mut guard = CancelOnDrop(Some(cancellation.clone()));
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Request {
            peer: peer.clone(),
            cancellation,
            reply,
        })?;
        let result = receiver.await.map_err(|_| PeerLink::closed_error())?;
        if result.is_ok() {
            guard.0.take();
        }
        result
    }
    pub fn channel(&self, peer: &PeerAddress) -> Option<PeerChannel> {
        self.channels
            .lock()
            .unwrap()
            .get(peer)
            .and_then(Weak::upgrade)
            .filter(|state| !state.ended.is_cancelled())
            .map(|state| PeerChannel { state })
    }
    pub async fn set_advertising(&self, enabled: bool) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Advertising(enabled, reply))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    /// Replace application advertisement fields without replacing the server or channels.
    /// The configured service-data key is reserved for this owner's endpoint metadata.
    /// Oversized readable metadata and reserved-key injection fail before native work.
    ///
    /// Admission retains validated desired metadata even if this waiter is cancelled.
    /// Updates before startup or while the native server is absent stage that metadata.
    /// A live update preserves visibility and completes both native and readable writes.
    /// Native and readable updates are separate operations, not an atomic transaction.
    /// Platform limits can reject optional native fields; success does not guarantee their broadcast.
    ///
    /// An error after admission does not restore the previous metadata. Native errors,
    /// timeouts and intervening radio loss guard show and further native updates.
    /// A successful `set_advertising(false)` must fence late callbacks first.
    /// Then retry the complete desired advertisement while hidden before explicitly showing it.
    /// A failed or unsupported hide does not establish that fence.
    pub async fn update_advertisement(
        &self,
        advertisement: crate::advertisement::GattAdvertisement,
    ) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::UpdateAdvertisement(advertisement, reply))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn disconnect(&self, endpoint: EndpointId) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Disconnect(endpoint, reply))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn close(&self) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Close(reply))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn discover(&self) -> Result<BoxStream<Result<(EndpointId, DiscoveredPeer)>>> {
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Discover(reply))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    fn submit(&self, command: Command) -> Result<()> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    BleError::GattBusy("worker queue full".into())
                }
                mpsc::error::TrySendError::Closed(_) => PeerLink::closed_error(),
            })
    }
}

pub(super) struct CancelOnDrop(pub Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}
/// One attempt. Dropping this value cancels only this attempt.
pub struct ConnectionRequest {
    pub(super) endpoint: EndpointId,
    pub(super) peer: PeerAddress,
    pub(super) generation: u64,
    pub(super) commands: mpsc::Sender<Command>,
    pub(super) cancellation: CancelOnDrop,
    pub(super) outcome: Arc<Mutex<Option<crate::AttemptError>>>,
}
impl std::fmt::Debug for ConnectionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionRequest")
            .field("endpoint", &self.endpoint)
            .field("peer", &self.peer)
            .finish()
    }
}
impl ConnectionRequest {
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint
    }
    pub fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }
    pub async fn accept(mut self) -> Result<PeerChannel> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::Accept(self.generation, reply))
            .await
            .map_err(|_| PeerLink::closed_error())?;
        let result = receiver
            .await
            .map_err(|_| PeerLink::closed_error())?
            .map_err(|error| {
                self.outcome
                    .lock()
                    .unwrap()
                    .map_or(error, BleError::Attempt)
            });
        if result.is_ok() {
            self.cancellation.0.take();
        }
        result
    }
    pub async fn reject(self) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::Reject(self.generation, reply))
            .await
            .map_err(|_| PeerLink::closed_error())?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
}

/// Why this channel generation ended. This value is set before receive closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerChannelCloseReason {
    LocalDisconnect,
    PeerDisconnect,
    LinkLost,
    ProtocolError,
    Released,
    Shutdown,
}

pub(super) struct ChannelState {
    pub endpoint: EndpointId,
    pub peer: PeerAddress,
    pub generation: u64,
    pub maximum: usize,
    pub transport: mpsc::Sender<TransportCommand>,
    pub received: AsyncMutex<mpsc::Receiver<Result<Vec<u8>>>>,
    pub overflow: Arc<crate::datagram::OverflowSignal>,
    pub ended: CancellationToken,
    pub released: CancellationToken,
    pub commands: mpsc::Sender<Command>,
    pub attempt: u64,
    pub channels: ChannelTable,
    pub close_reason: Mutex<Option<PeerChannelCloseReason>>,
}
impl Drop for ChannelState {
    fn drop(&mut self) {
        self.released.cancel();
    }
}
/// A scoped use of one generation. Clones share a receive queue, not a broadcast.
#[derive(Clone)]
pub struct PeerChannel {
    pub(super) state: Arc<ChannelState>,
}
impl std::fmt::Debug for PeerChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerChannel")
            .field("endpoint", &self.state.endpoint)
            .finish()
    }
}
impl PeerChannel {
    pub fn endpoint_id(&self) -> EndpointId {
        self.state.endpoint
    }
    pub fn peer(&self) -> PeerAddress {
        self.state.peer.clone()
    }
    pub fn max_message_len(&self) -> usize {
        self.state.maximum
    }
    pub fn close_reason(&self) -> Option<PeerChannelCloseReason> {
        *self.state.close_reason.lock().unwrap()
    }
    pub async fn send(&self, payload: Vec<u8>) -> Result<()> {
        if self.state.ended.is_cancelled() {
            return Err(PeerLink::closed_error());
        }
        if payload.len() > self.state.maximum {
            return Err(BleError::Protocol(
                "managed message exceeds channel bound".into(),
            ));
        }
        let (reply, receiver) = oneshot::channel();
        self.state
            .transport
            .try_send(TransportCommand::Send(
                Record::Data {
                    attempt: self.state.attempt,
                    payload,
                },
                Some(reply),
            ))
            .map_err(|_| BleError::GattBusy("channel queue full or closed".into()))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn recv(&self) -> Option<Result<Vec<u8>>> {
        // Hold the receive lock across flag checks and wakeups too: clones
        // consume one shared queue and one shared gap report, never broadcast.
        let mut received = self.state.received.lock().await;
        loop {
            // The peer's disconnect follows the messages it sent before it, so
            // those are still yielded before the end. A local end or a lost
            // link discards what was not read, a pending gap included.
            let draining = self.state.ended.is_cancelled();
            if draining && self.close_reason() != Some(PeerChannelCloseReason::PeerDisconnect) {
                return None;
            }
            if self.state.overflow.take() {
                return Some(Err(BleError::ReceiveGap(
                    "managed inbound payload was lost".into(),
                )));
            }
            if draining {
                return received.try_recv().ok();
            }
            tokio::select! { biased;
                () = self.state.ended.cancelled() => {}
                () = self.state.overflow.notified() => {}
                item = received.recv() => return item,
            }
        }
    }
    pub async fn request_connection_priority(&self, priority: ConnectionPriority) -> Result<()> {
        if self.state.ended.is_cancelled() {
            return Err(PeerLink::closed_error());
        }
        let (reply, receiver) = oneshot::channel();
        self.state
            .transport
            .try_send(TransportCommand::Priority(priority, reply))
            .map_err(|_| BleError::GattBusy("priority queue full or closed".into()))?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn disconnect(&self) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.state
            .commands
            .send(Command::DisconnectGeneration(self.state.generation, reply))
            .await
            .map_err(|_| PeerLink::closed_error())?;
        receiver.await.map_err(|_| PeerLink::closed_error())?
    }
    pub async fn release(self) -> Result<()> {
        let generation = self.state.generation;
        let commands = self.state.commands.clone();
        // Serialize last-use retirement with weak acquisition and other releases.
        // No new use can appear between the count check and removal from the table.
        let table = self.state.channels.clone();
        let last = {
            let mut channels = table.lock().unwrap();
            let last = Arc::strong_count(&self.state) == 1;
            if last
                && channels
                    .get(&self.state.peer)
                    .is_some_and(|state| state.ptr_eq(&Arc::downgrade(&self.state)))
            {
                channels.remove(&self.state.peer);
            }
            drop(self);
            last
        };
        if last {
            let (reply, receiver) = oneshot::channel();
            commands
                .send(Command::Retire(generation, reply))
                .await
                .map_err(|_| PeerLink::closed_error())?;
            receiver.await.map_err(|_| PeerLink::closed_error())?
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::drivers::mock::{MockDriver, MockNetwork};
    use crate::{CapabilityReport, CharacteristicUuid, ServiceUuid};
    use std::time::Duration;
    use tokio_stream::StreamExt;

    fn pair() -> (Arc<PeerLink>, Arc<PeerLink>) {
        let network = MockNetwork::new();
        let make = |address: &str| {
            PeerLink::with_driver(
                Arc::new(MockDriver::new(
                    PeerAddress(address.into()),
                    network.clone(),
                    CapabilityReport {
                        central: true,
                        peripheral: true,
                    },
                )),
                PeerLinkConfig::new(DatagramConfig::new(
                    ServiceUuid(uuid::Uuid::from_u128(11)),
                    CharacteristicUuid(uuid::Uuid::from_u128(12)),
                )),
            )
        };
        (make("a"), make("b"))
    }
    pub(crate) async fn nonce(link: &PeerLink, value: i32) {
        let (reply, receiver) = oneshot::channel();
        link.submit(Command::SetNonce(value, reply)).unwrap();
        receiver.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn inbound_admission_obeys_managed_link_bound() {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(11)),
            CharacteristicUuid(uuid::Uuid::from_u128(12)),
        );
        let raw_driver = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let mut managed_config = PeerLinkConfig::new(config.clone());
        managed_config.max_links = MaxLinks(0);
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            managed_config,
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw = crate::datagram::DatagramChannel::connect_driver(
            raw_driver,
            &PeerAddress("managed".into()),
            &config,
        )
        .await
        .unwrap();
        let _ = raw
            .send(
                Record::Request {
                    sender: EndpointId([1; 16]),
                    target: managed.endpoint_id(),
                    attempt: 19,
                    nonce: 7,
                }
                .encode(),
            )
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), incoming.next())
                .await
                .is_err(),
            "inbound limit bypassed"
        );
        managed.close().await.unwrap();
    }

    #[tokio::test]
    async fn reject_sends_explicit_decision_before_retiring_attempt() {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(11)),
            CharacteristicUuid(uuid::Uuid::from_u128(12)),
        );
        let raw = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            PeerLinkConfig::new(config.clone()),
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw = crate::datagram::DatagramChannel::connect_driver(
            raw,
            &PeerAddress("managed".into()),
            &config,
        )
        .await
        .unwrap();
        raw.send(
            Record::Request {
                sender: EndpointId([1; 16]),
                target: managed.endpoint_id(),
                attempt: 19,
                nonce: 7,
            }
            .encode(),
        )
        .await
        .unwrap();
        let _response = raw.recv().await.unwrap().unwrap();
        incoming.next().await.unwrap().reject().await.unwrap();
        let decision = tokio::time::timeout(Duration::from_secs(1), raw.recv())
            .await
            .unwrap();
        assert_eq!(
            Record::decode(
                &decision.expect("explicit rejection record").unwrap(),
                config.max_message_len
            )
            .unwrap(),
            Record::Decision {
                attempt: 19,
                action: 1
            }
        );
        managed.close().await.unwrap();
    }

    #[tokio::test]
    async fn established_idle_peer_receives_keepalive_control() {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(11)),
            CharacteristicUuid(uuid::Uuid::from_u128(12)),
        );
        let raw = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            PeerLinkConfig::new(config.clone()),
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw = crate::datagram::DatagramChannel::connect_driver(
            raw,
            &PeerAddress("managed".into()),
            &config,
        )
        .await
        .unwrap();
        raw.send(
            Record::Request {
                sender: EndpointId([1; 16]),
                target: managed.endpoint_id(),
                attempt: 17,
                nonce: -7,
            }
            .encode(),
        )
        .await
        .unwrap();
        let _response = raw.recv().await.unwrap().unwrap();
        let request = incoming.next().await.unwrap();
        raw.send(
            Record::Decision {
                attempt: 17,
                action: 0,
            }
            .encode(),
        )
        .await
        .unwrap();
        let channel = request.accept().await.unwrap();
        assert_eq!(
            Record::decode(&raw.recv().await.unwrap().unwrap(), config.max_message_len).unwrap(),
            Record::Decision {
                attempt: 17,
                action: 0
            }
        );
        let ping = tokio::time::timeout(Duration::from_secs(7), raw.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            Record::decode(&ping, config.max_message_len).unwrap(),
            Record::Ping { attempt: 17 }
        );
        raw.send(Record::Pong { attempt: 17 }.encode())
            .await
            .unwrap();
        channel.send(b"healthy idle".to_vec()).await.unwrap();
        assert!(matches!(
            Record::decode(&raw.recv().await.unwrap().unwrap(), config.max_message_len).unwrap(),
            Record::Data { .. }
        ));
        managed.close().await.unwrap();
    }

    async fn raw_established(
        attempt: u64,
    ) -> (Arc<PeerLink>, crate::datagram::DatagramChannel, PeerChannel) {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(11)),
            CharacteristicUuid(uuid::Uuid::from_u128(12)),
        );
        let raw = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            PeerLinkConfig::new(config.clone()),
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw = crate::datagram::DatagramChannel::connect_driver(
            raw,
            &PeerAddress("managed".into()),
            &config,
        )
        .await
        .unwrap();
        raw.send(
            Record::Request {
                sender: EndpointId([1; 16]),
                target: managed.endpoint_id(),
                attempt,
                nonce: 7,
            }
            .encode(),
        )
        .await
        .unwrap();
        let _response = raw.recv().await.unwrap().unwrap();
        let request = incoming.next().await.unwrap();
        raw.send(Record::Decision { attempt, action: 0 }.encode())
            .await
            .unwrap();
        let channel = request.accept().await.unwrap();
        let _decision = raw.recv().await.unwrap().unwrap();
        (managed, raw, channel)
    }

    #[tokio::test]
    async fn receive_yields_admitted_message_after_peer_disconnect_then_ends() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (managed, mut raw, channel) = raw_established(29).await;
            raw.send(
                Record::Data {
                    attempt: 29,
                    payload: b"last words".to_vec(),
                }
                .encode(),
            )
            .await
            .unwrap();
            raw.send(Record::Disconnect { attempt: 29 }.encode())
                .await
                .unwrap();
            channel.state.ended.cancelled().await;
            assert_eq!(
                channel.close_reason(),
                Some(PeerChannelCloseReason::PeerDisconnect)
            );
            assert_eq!(channel.recv().await.unwrap().unwrap(), b"last words");
            assert!(channel.recv().await.is_none());
            managed.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn positive_nonce_beats_negative_when_other_peer_initiates_winner() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (a, b) = pair();
            let mut incoming_a = a.incoming().unwrap();
            let _incoming_b = b.incoming().unwrap();
            a.set_advertising(true).await.unwrap();
            b.set_advertising(true).await.unwrap();
            nonce(&a, i32::MIN).await;
            nonce(&b, i32::MAX).await;
            let address_a = PeerAddress("a".into());
            let address_b = PeerAddress("b".into());
            let (loser, winner) = tokio::join!(a.request(&address_b), b.request(&address_a));
            let incoming = incoming_a.next().await.unwrap();
            let (left, right) = tokio::join!(incoming.accept(), winner.unwrap().accept());
            let left = left.unwrap();
            let right = right.unwrap();
            if let Ok(loser) = loser {
                assert!(loser.accept().await.is_err());
            }
            right.send(b"positive winner".to_vec()).await.unwrap();
            assert_eq!(left.recv().await.unwrap().unwrap(), b"positive winner");
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn equal_nonces_fail_both_attempts_with_typed_outcomes() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (a, b) = pair();
            let _incoming_a = a.incoming().unwrap();
            let _incoming_b = b.incoming().unwrap();
            a.set_advertising(true).await.unwrap();
            b.set_advertising(true).await.unwrap();
            nonce(&a, i32::MIN).await;
            nonce(&b, i32::MIN).await;
            let address_a = PeerAddress("a".into());
            let address_b = PeerAddress("b".into());
            let (left, right) = tokio::join!(a.request(&address_b), b.request(&address_a));
            for result in [left, right] {
                let result = match result {
                    Ok(request) => request.accept().await.map(|_| ()),
                    Err(error) => Err(error),
                };
                assert!(
                    matches!(
                        result,
                        Err(crate::BleError::Attempt(crate::AttemptError::EqualNonce))
                    ),
                    "{result:?}"
                );
            }
            assert!(a.channel(&address_b).is_none());
            assert!(b.channel(&address_a).is_none());
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn crossed_signed_nonces_fail_outgoing_separately_from_incoming_winner() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (a, b) = pair();
            let mut incoming_a = a.incoming().unwrap();
            let mut incoming_b = b.incoming().unwrap();
            a.set_advertising(true).await.unwrap();
            b.set_advertising(true).await.unwrap();
            nonce(&a, -1).await;
            nonce(&b, i32::MIN).await;
            let address_a = PeerAddress("a".into());
            let address_b = PeerAddress("b".into());
            let (outgoing_a, outgoing_b) =
                tokio::join!(a.request(&address_b), b.request(&address_a));
            let outgoing_a = outgoing_a.unwrap();
            let winner_b = incoming_b.next().await.unwrap();
            let (accepted_a, accepted_b) = tokio::join!(outgoing_a.accept(), winner_b.accept());
            let channel_a = accepted_a.unwrap();
            let channel_b = accepted_b.unwrap();
            if let Ok(loser) = outgoing_b {
                assert!(loser.accept().await.is_err());
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(100), incoming_a.next())
                    .await
                    .is_err()
            );
            channel_b.send(b"winner".to_vec()).await.unwrap();
            assert_eq!(channel_a.recv().await.unwrap().unwrap(), b"winner");
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}
