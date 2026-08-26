//! `mock-broker` client: implements the same method surface as
//! `local::LocalRadio`, backed by a socket to a `MockNetwork::serve()`
//! broker instead of in-process state. See
//! docs/adr/0004-mock-broker-for-cross-process-e2e.md.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::backend::BoxStream;
use crate::error::{BleError, Result};
use crate::models::{
    CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress, ServiceUuid,
    WriteType,
};

use super::wire::{read_frame, write_frame, Envelope, Frame, Push, Request, Response};

/// `closed` lives under the *same* lock as the waiter map, not as a
/// separate flag — see `RemoteClient::call`'s doc comment for why that
/// matters: it's what makes "check closed, then register a waiter" atomic
/// with "mark closed, then drain every waiter," so no `call()` can register
/// a waiter after the reader has already given up resolving one.
#[derive(Default)]
struct PendingState {
    calls: HashMap<u64, oneshot::Sender<Response>>,
    closed: bool,
}
type PendingMap = Arc<Mutex<PendingState>>;
type EventsMap = Arc<Mutex<HashMap<PeerAddress, broadcast::Sender<GattEvent>>>>;
type SubscriptionsMap = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Result<Vec<u8>>>>>>;

pub(crate) struct RemoteClient {
    outbox: mpsc::UnboundedSender<Envelope>,
    next_correlation: AtomicU64,
    next_subscription: AtomicU64,
    pending: PendingMap,
    events: EventsMap,
    subscriptions: SubscriptionsMap,
    /// The reader/writer tasks each hold one half of the split `TcpStream`
    /// directly, not through `RemoteClient` — so without this, dropping
    /// every `Arc<MockNetwork>` referencing this client would NOT close the
    /// socket (both halves have to be dropped for `tokio::io::split`'s
    /// shared stream to actually close), and the broker would never learn
    /// this side is gone. `Drop` aborts both tasks, dropping their captured
    /// halves and closing the connection for real.
    reader_handle: tokio::task::JoinHandle<()>,
    writer_handle: tokio::task::JoinHandle<()>,
}

impl Drop for RemoteClient {
    fn drop(&mut self) {
        self.reader_handle.abort();
        self.writer_handle.abort();
    }
}

impl RemoteClient {
    pub(crate) async fn dial(endpoint: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await.map_err(|e| BleError::Transport(e.to_string()))?;
        let (read_half, write_half) = tokio::io::split(stream);
        let (outbox_tx, outbox_rx) = mpsc::unbounded_channel::<Envelope>();

        let pending = Arc::new(Mutex::new(PendingState::default()));
        let events = Arc::new(Mutex::new(HashMap::new()));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));

        let writer_handle = tokio::spawn(Self::writer_loop(write_half, outbox_rx));
        let reader_handle = tokio::spawn(Self::reader_loop(read_half, pending.clone(), events.clone(), subscriptions.clone()));

        Ok(Self {
            outbox: outbox_tx,
            next_correlation: AtomicU64::new(1),
            next_subscription: AtomicU64::new(0),
            pending,
            events,
            subscriptions,
            reader_handle,
            writer_handle,
        })
    }

    async fn writer_loop(mut write_half: WriteHalf<TcpStream>, mut rx: mpsc::UnboundedReceiver<Envelope>) {
        while let Some(env) = rx.recv().await {
            if write_frame(&mut write_half, &env).await.is_err() {
                break;
            }
        }
    }

    async fn reader_loop(
        mut read_half: ReadHalf<TcpStream>, pending: PendingMap, events: EventsMap, subscriptions: SubscriptionsMap,
    ) {
        loop {
            let env = match read_frame(&mut read_half).await {
                Ok(env) => env,
                Err(_) => break,
            };
            match env.frame {
                Frame::Resp(resp) => {
                    if let Some(tx) = pending.lock().unwrap().calls.remove(&env.correlation_id) {
                        let _ = tx.send(resp);
                    }
                }
                Frame::Push(Push::Event { address, event }) => {
                    if let Some(tx) = events.lock().unwrap().get(&address) {
                        let _ = tx.send(event);
                    }
                }
                Frame::Push(Push::NotifyItem { subscription_id, item }) => {
                    if let Some(tx) = subscriptions.lock().unwrap().get(&subscription_id) {
                        let _ = tx.send(item);
                    }
                }
                // Clients never receive requests; a well-behaved broker never
                // sends one.
                Frame::Req(_) => {}
            }
        }
        // Connection lost — every in-flight `call()` must resolve rather
        // than hang forever awaiting a response that will never arrive. Set
        // `closed` under the same lock as the drain, not after it: a `call()`
        // that raced in between would otherwise register a waiter this
        // reader has already stopped watching for, and never resolve.
        let mut state = pending.lock().unwrap();
        state.closed = true;
        for (_, tx) in state.calls.drain() {
            let _ = tx.send(Response::Err(BleError::Transport("broker connection closed".to_string())));
        }
        drop(state);
        // Every live `subscribe()` stream must end, not hang forever: with
        // this reader gone, nothing will ever push another `NotifyItem` to
        // any of these senders. Dropping each `tx` closes its receiver,
        // which is what makes `UnboundedReceiverStream::next()` finally
        // return `None` instead of waiting on a connection that no longer
        // exists. A `subscribe()` racing this drain is still safe: if its
        // insert lands first it gets swept here; if it lands after, `call()`
        // already sees `pending`'s `closed` flag and fails fast, and
        // `subscribe()`'s own error path removes the entry it just added.
        subscriptions.lock().unwrap().clear();
    }

    /// `closed` and the waiter map share one lock (`PendingState`) so this
    /// check-then-insert is atomic with `reader_loop`'s mark-then-drain on
    /// the other side: whichever runs first under the lock determines
    /// whether this call is rejected immediately or resolved (with an
    /// error) once the reader's cleanup catches it — either way, `call()`
    /// can never register a waiter with nobody left to ever resolve it.
    async fn call(&self, request: Request) -> Result<Response> {
        let correlation_id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.pending.lock().unwrap();
            if state.closed {
                return Err(BleError::Transport("broker connection closed".to_string()));
            }
            state.calls.insert(correlation_id, tx);
        }
        if self.outbox.send(Envelope { correlation_id, frame: Frame::Req(request) }).is_err() {
            self.pending.lock().unwrap().calls.remove(&correlation_id);
            return Err(BleError::Transport("broker connection closed".to_string()));
        }
        match rx.await {
            Ok(Response::Err(e)) => Err(e),
            Ok(resp) => Ok(resp),
            Err(_) => Err(BleError::Transport("broker connection closed before responding".to_string())),
        }
    }

    pub(crate) fn register_events_sender(&self, address: PeerAddress, sender: broadcast::Sender<GattEvent>) {
        self.events.lock().unwrap().insert(address.clone(), sender);
        // Fire-and-forget (correlation_id 0: no one is waiting on a
        // response). Always the first frame sent by this client — the
        // shared outbox queue preserves that ordering on the wire. See
        // `Request::RegisterAddress`'s doc comment for why no ack is needed.
        let _ = self.outbox.send(Envelope { correlation_id: 0, frame: Frame::Req(Request::RegisterAddress { address }) });
    }

    pub(crate) async fn scan(&self, requester: &PeerAddress, service: ServiceUuid) -> Result<(Vec<DiscoveredPeer>, Option<String>)> {
        match self.call(Request::Scan { requester: requester.clone(), service }).await? {
            Response::ScanSnapshot { peers, armed_failure } => Ok((peers, armed_failure)),
            _ => Err(BleError::Transport("unexpected response to Scan".to_string())),
        }
    }

    pub(crate) async fn connect(&self, central: &PeerAddress, peer: &PeerAddress) -> Result<u64> {
        match self.call(Request::Connect { central: central.clone(), peer: peer.clone() }).await? {
            Response::Session(session) => Ok(session),
            _ => Err(BleError::Transport("unexpected response to Connect".to_string())),
        }
    }

    pub(crate) async fn advertise(&self, address: &PeerAddress, service: GattServiceSpec) -> Result<()> {
        match self.call(Request::Advertise { address: address.clone(), service }).await? {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to Advertise".to_string())),
        }
    }

    pub(crate) async fn stop_advertising(&self, address: &PeerAddress) -> Result<()> {
        match self.call(Request::StopAdvertising { address: address.clone() }).await? {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to StopAdvertising".to_string())),
        }
    }

    pub(crate) async fn notify(&self, address: &PeerAddress, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        match self.call(Request::Notify { address: address.clone(), characteristic, value }).await? {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to Notify".to_string())),
        }
    }

    pub(crate) async fn notify_peer(
        &self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>,
        characteristic: CharacteristicUuid, value: Vec<u8>,
    ) -> Result<()> {
        match self
            .call(Request::NotifyPeer { address: address.clone(), peer: peer.clone(), session, characteristic, value })
            .await?
        {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to NotifyPeer".to_string())),
        }
    }

    pub(crate) async fn disconnect_peer(&self, address: &PeerAddress, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        match self.call(Request::DisconnectPeer { address: address.clone(), peer: peer.clone(), session }).await? {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to DisconnectPeer".to_string())),
        }
    }

    pub(crate) async fn read(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<Vec<u8>> {
        match self
            .call(Request::Read { session, central: central.clone(), peripheral: peripheral.clone(), characteristic })
            .await?
        {
            Response::Bytes(bytes) => Ok(bytes),
            _ => Err(BleError::Transport("unexpected response to Read".to_string())),
        }
    }

    pub(crate) async fn write_with_type(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
        value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        match self
            .call(Request::WriteWithType {
                session, central: central.clone(), peripheral: peripheral.clone(), characteristic, value, write_type,
            })
            .await?
        {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to WriteWithType".to_string())),
        }
    }

    pub(crate) async fn subscribe(
        &self, session: u64, central: &PeerAddress, peripheral: &PeerAddress, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        // Client-allocated and registered *before* the request is sent — see
        // `Request::Subscribe`'s doc comment for why this ordering matters.
        let subscription_id = self.next_subscription.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscriptions.lock().unwrap().insert(subscription_id, tx);
        let request = Request::Subscribe {
            session, central: central.clone(), peripheral: peripheral.clone(), characteristic, subscription_id,
        };
        match self.call(request).await {
            Ok(Response::Unit) => Ok(Box::pin(UnboundedReceiverStream::new(rx))),
            Ok(_) => {
                self.subscriptions.lock().unwrap().remove(&subscription_id);
                Err(BleError::Transport("unexpected response to Subscribe".to_string()))
            }
            Err(e) => {
                self.subscriptions.lock().unwrap().remove(&subscription_id);
                Err(e)
            }
        }
    }

    pub(crate) async fn disconnect(&self, session: u64, central: &PeerAddress, peripheral: &PeerAddress) -> Result<()> {
        match self.call(Request::Disconnect { session, central: central.clone(), peripheral: peripheral.clone() }).await? {
            Response::Unit => Ok(()),
            _ => Err(BleError::Transport("unexpected response to Disconnect".to_string())),
        }
    }
}
