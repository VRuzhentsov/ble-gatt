//! One worker owns managed attempt state and native channel use.
use super::{
    acquired_cleanup::Cleanup,
    managed_codec::{self, Record},
    managed_driver::ManagedDriver,
    peer_link::*,
};
use crate::datagram::{self, DatagramChannel};
use crate::{
    AttemptError, BleError, BoxStream, ConnectionPriority, DiscoveredPeer, Driver, PeerAddress,
    Result,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
const ESTABLISHMENT_BOUND: Duration = Duration::from_secs(30);
const IO_BOUND: Duration = Duration::from_secs(5);
type DiscoveryOutput = mpsc::Sender<Result<(EndpointId, DiscoveredPeer)>>;
type DiscoveryScan = (BoxStream<Result<DiscoveredPeer>>, DiscoveryOutput);
const MAX_PENDING_OPERATIONS: usize = 64;
type Teardown = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
enum Operation {
    Cleaned(PeerAddress, u64, Result<()>),
    Advertising(Result<Option<BoxStream<DatagramChannel>>>, Reply<()>),
    Discovery(
        Result<BoxStream<Result<DiscoveredPeer>>>,
        Reply<BoxStream<Result<(EndpointId, DiscoveredPeer)>>>,
    ),
    Probe(
        u64,
        PeerAddress,
        DiscoveryOutput,
        Result<(EndpointId, DiscoveredPeer)>,
    ),
    Retired(Vec<(PeerAddress, u64)>, Result<()>, Option<Reply<()>>),
}

pub(super) enum Command {
    #[cfg(test)]
    SetNonce(i32, Reply<()>),
    Request {
        peer: PeerAddress,
        cancellation: CancellationToken,
        reply: Reply<ConnectionRequest>,
    },
    Advertising(bool, Reply<()>),
    Discover(Reply<BoxStream<Result<(EndpointId, DiscoveredPeer)>>>),
    Accept(u64, Reply<PeerChannel>),
    Reject(u64, Reply<()>),
    Disconnect(EndpointId, Reply<()>),
    DisconnectGeneration(u64, Reply<()>),
    Retire(u64, Reply<()>),
    Close(Reply<()>),
    Dialed(u64, Result<(DatagramChannel, EndpointId)>),
    Wire(
        u64,
        Option<Result<Record>>,
        Option<oneshot::Sender<Option<Teardown>>>,
    ),
}
pub(super) enum TransportCommand {
    Send(Record, Option<Reply<()>>),
    Priority(ConnectionPriority, Reply<()>),
    Stop {
        reply: Reply<()>,
        confirmation: Option<Record>,
    },
}
struct Attempt {
    outcome: Arc<Mutex<Option<AttemptError>>>,
    last_received: tokio::time::Instant,
    next_ping: tokio::time::Instant,
    peer: PeerAddress,
    endpoint: Option<EndpointId>,
    wire_attempt: u64,
    nonce: i32,
    outgoing: bool,
    initiation: Option<Reply<ConnectionRequest>>,
    accepted: Option<Reply<PeerChannel>>,
    local_accept: bool,
    remote_accept: bool,
    rejection_received: bool,
    cancellation: CancellationToken,
    deadline: tokio::time::Instant,
    transport: Option<mpsc::Sender<TransportCommand>>,
    task: Option<tokio::task::AbortHandle>,
    maximum: usize,
    channel: Option<Weak<ChannelState>>,
    messages: Option<mpsc::Sender<Result<Vec<u8>>>>,
    ended: CancellationToken,
    released: CancellationToken,
}
struct Retirement {
    generation: u64,
    // One newly acquired raw channel may arrive before the previous native
    // cleanup completion is dispatched. Keep it under the existing reservation;
    // do not admit its managed attempt or disconnect its newer driver session.
    inbound: Option<DatagramChannel>,
}

pub(super) struct Worker {
    #[cfg(test)]
    next_nonce: Option<i32>,
    endpoint: EndpointId,
    config: PeerLinkConfig,
    driver: Option<Arc<dyn Driver>>,
    commands: mpsc::Sender<Command>,
    receiver: mpsc::Receiver<Command>,
    incoming: mpsc::Sender<ConnectionRequest>,
    channels: ChannelTable,
    shutdown: CancellationToken,
    attempts: HashMap<u64, Attempt>,
    operations: tokio::task::JoinSet<Operation>,
    retiring: HashMap<PeerAddress, Retirement>,
    probing: HashMap<PeerAddress, u64>,
    acquired: HashMap<PeerAddress, u64>,
    cleanup_failure: Option<BleError>,
    next_generation: u64,
}
impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        endpoint: EndpointId,
        config: PeerLinkConfig,
        driver: Option<Arc<dyn Driver>>,
        commands: mpsc::Sender<Command>,
        receiver: mpsc::Receiver<Command>,
        incoming: mpsc::Sender<ConnectionRequest>,
        channels: ChannelTable,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            #[cfg(test)]
            next_nonce: None,
            endpoint,
            config,
            driver,
            commands,
            receiver,
            incoming,
            channels,
            shutdown,
            attempts: HashMap::new(),
            operations: tokio::task::JoinSet::new(),
            retiring: HashMap::new(),
            probing: HashMap::new(),
            acquired: HashMap::new(),
            cleanup_failure: None,
            next_generation: 0,
        }
    }
    pub(super) async fn run(mut self) {
        let driver = match self.driver.take() {
            Some(driver) => driver,
            None => match tokio::select! {
                _ = self.shutdown.cancelled() => return,
                result = tokio::time::timeout(IO_BOUND, crate::drivers::platform()) => result,
            } {
                Ok(Ok(driver)) => driver,
                _ => return,
            },
        };
        let mut served: Option<BoxStream<DatagramChannel>> = None;
        let mut scan: Option<DiscoveryScan> = None;
        let mut probed = std::collections::HashSet::new();
        let mut scan_generation = 0u64;
        let mut advertising_pending = false;
        let mut discovery_pending = false;
        let mut close_reply = None;
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            let scan_output = scan.as_ref().map(|(_, output)| output.clone());
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                completed = self.operations.join_next(), if !self.operations.is_empty() => {
                    if let Some(Ok(operation)) = completed {
                        match operation {
                            Operation::Cleaned(peer, generation, result) => self.cleaned(peer, generation, result),
                            Operation::Advertising(result, reply) => {
                                advertising_pending = false;
                                let result = result.map(|stream| { if let Some(stream) = stream { served = Some(stream); } });
                                let _ = reply.send(result);
                            }
                            Operation::Discovery(result, reply) => {
                                discovery_pending = false;
                                let result = result.map(|stream| {
                                    let (output, receiver) = mpsc::channel(32);
                                    scan = Some((stream, output));
                                    Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver)) as BoxStream<_>
                                });
                                let _ = reply.send(result);
                            }
                            Operation::Probe(generation, peer, output, result) => {
                                if self.probing.get(&peer) == Some(&generation) { self.probing.remove(&peer); }
                                if generation == scan_generation { let _ = output.try_send(result); }
                            }
                            Operation::Retired(targets, result, reply) => {
                                self.retired(targets);
                                if let Some(reply) = reply { let _ = reply.send(result); }
                            }
                        }
                    }
                }
                _ = tick.tick() => {
                    let now = tokio::time::Instant::now();
                    let expired: Vec<_> = self.attempts.iter().filter(|(_, a)| a.released.is_cancelled() || (a.channel.is_none() && (a.cancellation.is_cancelled() || now >= a.deadline)) || (a.channel.is_some() && now.duration_since(a.last_received) >= ESTABLISHMENT_BOUND)).map(|(generation, _)| *generation).collect();
                    for generation in expired {
                        let attempt = &self.attempts[&generation];
                        let reason = if attempt.released.is_cancelled() { AttemptError::Released }
                            else if attempt.channel.is_some() { AttemptError::PeerLost }
                            else if attempt.cancellation.is_cancelled() { AttemptError::Cancelled }
                            else { AttemptError::TimedOut };
                        self.retire(vec![generation], reason, None);
                    }
                    for attempt in self.attempts.values_mut().filter(|attempt| attempt.channel.is_some() && now >= attempt.next_ping) {
                        Self::send(attempt, Record::Ping { attempt: attempt.wire_attempt });
                        attempt.next_ping = now + IO_BOUND;
                    }
                }
                _ = async { match &scan_output { Some(output) => output.closed().await, None => std::future::pending().await } } => { scan = None; }
                item = async { match &mut scan { Some((stream, _)) => stream.next().await, None => std::future::pending().await } } => {
                    match item {
                        Some(item) => {
                            let result = match item {
                                Ok(mut peer) => {
                                    let service = self.config.datagram.service;
                                    if !peer.service_data.contains_key(&service) {
                                        if let Some(attempt) = self.attempts.values().find(|attempt| attempt.peer == peer.address) {
                                            // A pending dial may not have validated metadata yet. Defer it.
                                            let Some(endpoint) = attempt.endpoint else { continue; };
                                            peer.service_data.insert(service, managed_codec::metadata(endpoint));
                                        } else {
                                            // One bounded read per address per discovery. Never replace an owned session.
                                            if probed.len() >= 64 || self.acquired.contains_key(&peer.address) || self.probing.contains_key(&peer.address) || self.retiring.contains_key(&peer.address) || !probed.insert(peer.address.clone()) { continue; }
                                            let output = scan_output.as_ref().expect("active scan").clone();
                                            if !self.operation_capacity() || self.link_count() >= self.config.max_links.0 { let _ = output.try_send(Err(AttemptError::QueueFull.into())); continue; }
                                            let cleanup_generation = self.generation();
                                            let cleanup = self.reserve_cleanup(peer.address.clone(), cleanup_generation);
                                            let driver = Arc::new(ManagedDriver { inner: driver.clone(), service, endpoint: Arc::new(Mutex::new(None)), cleanup, validate_metadata: false });
                                            let shutdown = self.shutdown.clone();
                                            let generation = scan_generation;
                                            let address = peer.address.clone();
                                            self.probing.insert(address.clone(), generation);
                                            self.operations.spawn(async move {
                                                let read = tokio::select! {
                                                    _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                                    _ = output.closed() => Err(AttemptError::Cancelled.into()),
                                                    result = tokio::time::timeout(IO_BOUND, crate::advertisement::read_managed(driver.as_ref(), &peer.address)) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                                                };
                                                let result = read.and_then(|metadata| {
                                                    let peer = metadata.merge_into(service, peer);
                                                    peer.service_data.get(&service).ok_or_else(|| failed("missing managed profile"))
                                                        .and_then(|bytes| managed_codec::endpoint(bytes)).map(|endpoint| (endpoint, peer))
                                                });
                                                Operation::Probe(generation, address, output, result)
                                            });
                                            continue;
                                        }
                                    }
                                    peer.service_data.get(&service).ok_or_else(|| failed("missing managed profile"))
                                        .and_then(|bytes| managed_codec::endpoint(bytes)).map(|endpoint| (endpoint, peer))
                                }
                                Err(error) => Err(error),
                            };
                            if let Some((_, output)) = &scan { if output.try_send(result).is_err() { scan = None; } }
                        }
                        None => scan = None,
                    }
                }
                item = async { match &mut served { Some(stream) => stream.next().await, None => std::future::pending().await } } => {
                    if let Some(channel) = item { self.inbound(channel); } else { served = None; }
                }
                command = self.receiver.recv() => {
                    let Some(command) = command else { break; };
                    match command {
                        #[cfg(test)] Command::SetNonce(value, reply) => { self.next_nonce = Some(value); let _ = reply.send(Ok(())); }
                        Command::Advertising(enabled, reply) => {
                            if advertising_pending || !self.operation_capacity() {
                                let _ = reply.send(Err(BleError::GattBusy("managed native operation pending".into())));
                                continue;
                            }
                            advertising_pending = true;
                            let startup = served.is_none() && enabled;
                            let mut config = self.config.datagram.clone();
                            config.advertised_service_data.insert(config.service, managed_codec::metadata(self.endpoint));
                            let driver = driver.clone();
                            let shutdown = self.shutdown.clone();
                            self.operations.spawn(async move {
                                let result = tokio::select! {
                                    _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                    result = tokio::time::timeout(IO_BOUND, async {
                                        if startup { datagram::serve_driver(driver.clone(), &config).await.map(Some) }
                                        else { driver.set_advertising_enabled(enabled).await.map(|()| None) }
                                    }) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                                };
                                Operation::Advertising(result, reply)
                            });
                        }
                        Command::Request { peer, cancellation, reply } => self.request(driver.clone(), peer, cancellation, reply),
                        Command::Dialed(generation, result) => self.dialed(generation, result),
                        Command::Wire(generation, record, reply) => {
                            let admission = self.wire(generation, record);
                            if let Some(reply) = reply { let _ = reply.send(admission); }
                        },
                        Command::Accept(generation, reply) => {
                            if let Some(attempt) = self.attempts.get_mut(&generation) {
                                attempt.local_accept = true; attempt.accepted = Some(reply);
                                Self::send(attempt, Record::Decision { attempt: attempt.wire_attempt, action: 0 });
                                self.establish(generation);
                            } else { let _ = reply.send(Err(closed())); }
                        }
                        Command::Reject(generation, reply) => self.retire(vec![generation], AttemptError::Rejected, Some(reply)),
                        Command::Disconnect(endpoint, reply) => {
                            let generations: Vec<_> = self.attempts.iter().filter(|(_, attempt)| attempt.endpoint == Some(endpoint)).map(|(generation, _)| *generation).collect();
                            self.retire(generations, AttemptError::Disconnected, Some(reply));
                        }
                        Command::Retire(generation, reply) => self.retire(vec![generation], AttemptError::Released, Some(reply)),
                        Command::DisconnectGeneration(generation, reply) => self.retire(vec![generation], AttemptError::Disconnected, Some(reply)),
                        Command::Discover(reply) => {
                            if discovery_pending || !self.operation_capacity() || scan.as_ref().is_some_and(|(_, output)| !output.is_closed()) {
                                let _ = reply.send(Err(BleError::AdapterBusy("managed discovery already active or operation limit reached".into())));
                            } else {
                                scan = None;
                                scan_generation += 1;
                                probed.clear();
                                discovery_pending = true;
                                let driver = driver.clone();
                                let service = self.config.datagram.service;
                                let shutdown = self.shutdown.clone();
                                self.operations.spawn(async move {
                                    let result = tokio::select! {
                                        _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                        result = tokio::time::timeout(IO_BOUND, driver.scan(service)) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                                    };
                                    Operation::Discovery(result, reply)
                                });
                            }
                        }
                        Command::Close(reply) => { close_reply = Some(reply); break; }
                    }
                }
            }
        }
        // Explicit close and Drop share one bounded cleanup path. Cancel HAL
        // startups/probes before stopping the server, but let generation-owned
        // retirements make progress independently of the command dispatcher.
        self.shutdown.cancel();
        drop(scan);
        drop(served);
        let generations: Vec<_> = self.attempts.keys().copied().collect();
        self.retire(generations, AttemptError::Closed, None);
        let mut teardown = self.cleanup_failure.take().map_or(Ok(()), Err);
        let drained = tokio::time::timeout(IO_BOUND, async {
            while let Some(completed) = self.operations.join_next().await {
                match completed {
                    Ok(Operation::Cleaned(peer, generation, result)) => {
                        self.cleaned(peer, generation, result);
                        if let Some(error) = self.cleanup_failure.take() { teardown = Err(error); }
                    }
                    Ok(Operation::Retired(targets, result, reply)) => {
                        self.retired(targets);
                        if let Some(reply) = reply {
                            if let Err(error) = &result {
                                teardown = Err(BleError::Gatt(format!(
                                    "pending retirement failed: {error}"
                                )));
                            }
                            let _ = reply.send(result);
                        } else if result.is_err() {
                            teardown = result;
                        }
                    }
                    Ok(Operation::Advertising(_, reply)) => {
                        let _ = reply.send(Err(AttemptError::Cancelled.into()));
                    }
                    Ok(Operation::Discovery(_, reply)) => {
                        let _ = reply.send(Err(AttemptError::Cancelled.into()));
                    }
                    _ => {}
                }
            }
        })
        .await;
        if drained.is_err() {
            self.operations.abort_all();
            while self.operations.join_next().await.is_some() {}
            teardown = Err(AttemptError::TimedOut.into());
        }
        let stopped = tokio::time::timeout(IO_BOUND, driver.stop_advertising())
            .await
            .unwrap_or(Err(AttemptError::TimedOut.into()));
        if let Some(reply) = close_reply {
            let _ = reply.send(stopped.and(teardown));
        }
    }
    fn operation_capacity(&self) -> bool {
        self.operations.len() + self.attempts.len() + self.retiring.len() < MAX_PENDING_OPERATIONS
    }
    fn link_count(&self) -> usize {
        self.attempts.len() + self.retiring.len() + self.acquired.keys().filter(|peer| {
            !self.retiring.contains_key(*peer) && !self.attempts.values().any(|a| &a.peer == *peer)
        }).count()
    }
    fn reserve_cleanup(&mut self, peer: PeerAddress, generation: u64) -> Cleanup {
        let (cleanup, completion) = Cleanup::reserved();
        self.acquired.insert(peer.clone(), generation);
        self.operations.spawn(async move { Operation::Cleaned(peer, generation, completion.await) });
        cleanup
    }
    fn cleaned(&mut self, peer: PeerAddress, generation: u64, result: Result<()>) {
        if self.acquired.get(&peer) == Some(&generation) { self.acquired.remove(&peer); }
        if let Err(error) = result { self.cleanup_failure = Some(error); }
    }
    fn retired(&mut self, targets: Vec<(PeerAddress, u64)>) {
        for (peer, generation) in targets {
            if self
                .retiring
                .get(&peer)
                .is_some_and(|retirement| retirement.generation == generation)
            {
                let retirement = self.retiring.remove(&peer).expect("matched reservation");
                if !self.shutdown.is_cancelled() {
                    if let Some(channel) = retirement.inbound {
                        self.inbound(channel);
                    }
                }
            }
        }
    }
    fn retire(&mut self, generations: Vec<u64>, reason: AttemptError, reply: Option<Reply<()>>) {
        let targets: Vec<_> = generations
            .iter()
            .filter_map(|generation| {
                self.attempts
                    .get(generation)
                    .map(|a| (a.peer.clone(), *generation))
            })
            .collect();
        if targets.is_empty() {
            if let Some(reply) = reply {
                let _ = reply.send(Ok(()));
            }
            return;
        }
        let mut tasks: Vec<_> = targets
            .iter()
            .map(|(_, generation)| Some(self.finish(*generation, reason)))
            .collect();
        self.operations.spawn(async move {
            let mut result = Ok(());
            std::future::poll_fn(|cx| {
                let mut pending = false;
                for task in &mut tasks {
                    if let Some(future) = task {
                        match future.as_mut().poll(cx) {
                            std::task::Poll::Ready(outcome) => {
                                if outcome.is_err() {
                                    result = outcome;
                                }
                                *task = None;
                            }
                            std::task::Poll::Pending => pending = true,
                        }
                    }
                }
                if pending {
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(())
                }
            })
            .await;
            Operation::Retired(targets, result, reply)
        });
    }
    fn generation(&mut self) -> u64 {
        self.next_generation += 1;
        self.next_generation
    }
    fn attempt(
        &self,
        peer: PeerAddress,
        outgoing: bool,
        cancellation: CancellationToken,
    ) -> Attempt {
        let random = uuid::Uuid::new_v4();
        Attempt {
            outcome: Arc::new(Mutex::new(None)),
            last_received: tokio::time::Instant::now(),
            next_ping: tokio::time::Instant::now() + IO_BOUND,
            peer,
            endpoint: None,
            wire_attempt: u64::from_be_bytes(random.as_bytes()[..8].try_into().unwrap()),
            nonce: i32::from_be_bytes(random.as_bytes()[8..12].try_into().unwrap()),
            outgoing,
            initiation: None,
            accepted: None,
            local_accept: false,
            remote_accept: false,
            rejection_received: false,
            cancellation,
            deadline: tokio::time::Instant::now() + ESTABLISHMENT_BOUND,
            transport: None,
            task: None,
            maximum: 0,
            channel: None,
            messages: None,
            ended: CancellationToken::new(),
            released: CancellationToken::new(),
        }
    }
    fn request(
        &mut self,
        driver: Arc<dyn Driver>,
        peer: PeerAddress,
        cancellation: CancellationToken,
        reply: Reply<ConnectionRequest>,
    ) {
        if self.acquired.contains_key(&peer)
            || self.probing.contains_key(&peer)
            || self.retiring.contains_key(&peer)
            || self.attempts.values().any(|attempt| attempt.peer == peer)
        {
            let _ = reply.send(Err(BleError::AdapterBusy(
                "peer already has an attempt or channel".into(),
            )));
            return;
        }
        if !self.operation_capacity()
            || self.link_count() >= self.config.max_links.0
        {
            let _ = reply.send(Err(BleError::AdapterBusy(
                "managed link limit reached".into(),
            )));
            return;
        }
        let generation = self.generation();
        let cleanup = self.reserve_cleanup(peer.clone(), generation);
        let mut attempt = self.attempt(peer.clone(), true, cancellation);
        #[cfg(test)]
        if let Some(nonce) = self.next_nonce.take() {
            attempt.nonce = nonce;
        }
        attempt.initiation = Some(reply);
        let commands = self.commands.clone();
        let config = self.config.datagram.clone();
        let task = tokio::spawn(async move {
            let endpoint = Arc::new(Mutex::new(None));
            let driver = Arc::new(ManagedDriver {
                inner: driver,
                service: config.service,
                endpoint: endpoint.clone(),
                cleanup,
                validate_metadata: true,
            });
            let result = datagram::connect_managed(driver, &peer, &config)
                .await
                .and_then(|channel| {
                    let endpoint = endpoint
                        .lock()
                        .unwrap()
                        .ok_or_else(|| failed("missing managed endpoint"))?;
                    Ok((channel, endpoint))
                });
            let _ = commands.send(Command::Dialed(generation, result)).await;
        });
        attempt.task = Some(task.abort_handle());
        self.attempts.insert(generation, attempt);
    }
    fn inbound(&mut self, channel: DatagramChannel) {
        let peer = channel.peer();
        if let Some(retirement) = self.retiring.get_mut(&peer) {
            if retirement.inbound.is_none() {
                retirement.inbound = Some(channel);
            }
            return;
        }
        let crossed =
            self.attempts.values().any(|attempt| {
                attempt.outgoing && attempt.peer == peer && attempt.channel.is_none()
            }) && !self
                .attempts
                .values()
                .any(|attempt| !attempt.outgoing && attempt.peer == peer);
        if !self.operation_capacity()
            || self.probing.contains_key(&peer)
            || self.retiring.contains_key(&peer)
            || (self.attempts.len() + self.retiring.len() >= self.config.max_links.0 && !crossed)
        {
            return;
        }
        let generation = self.generation();
        let mut attempt = self.attempt(channel.peer(), false, CancellationToken::new());
        self.transport(generation, &mut attempt, channel);
        self.attempts.insert(generation, attempt);
    }
    fn transport(&self, generation: u64, attempt: &mut Attempt, mut channel: DatagramChannel) {
        let (transport, mut receiver) = mpsc::channel(16);
        attempt.maximum = channel
            .max_message_len()
            .saturating_sub(managed_codec::DATA_HEADER);
        let commands = self.commands.clone();
        let maximum = attempt.maximum;
        let ended = attempt.ended.clone();
        let native_ended = channel.termination();
        let task = tokio::spawn(async move {
            let mut admission: Option<Teardown> = None;
            let mut sending: Option<Teardown> = None;
            let mut send_reply: Option<Reply<()>> = None;
            let mut receiving = true;
            loop {
                tokio::select! {
                    // Retirement interrupts application admission, not the pump's
                    // control path: it still owns native confirmation/teardown.
                    _ = native_ended.cancelled(), if receiving => {
                        admission = None;
                        receiving = false;
                        let _ = commands.send(Command::Wire(generation, None, None)).await;
                    }
                    _ = ended.cancelled(), if receiving => {
                        receiving = false;
                        admission = None;
                    }
                    result = async { admission.as_mut().expect("guarded").await }, if admission.is_some() => {
                        admission = None;
                        if result.is_err() {
                            let _ = commands.send(Command::Wire(generation, None, None)).await;
                            receiving = false;
                        }
                    }
                    result = async { sending.as_mut().expect("guarded").await }, if sending.is_some() => {
                        sending = None;
                        let failed = result.is_err();
                        if let Some(reply) = send_reply.take() { let _ = reply.send(result); }
                        if failed { let _ = commands.send(Command::Wire(generation, None, None)).await; break; }
                    }
                    item = channel.recv(), if receiving && admission.is_none() => {
                        let terminal = item.is_none();
                        let record = item.map(|item| item.and_then(|bytes| Record::decode(&bytes, maximum)));
                        let commands = commands.clone();
                        // This one future owns command admission, validation reply,
                        // and application admission. No per-record tasks or extra queue.
                        admission = Some(Box::pin(async move {
                            tokio::time::timeout(IO_BOUND, async {
                                let (reply, receiver) = oneshot::channel();
                                commands.send(Command::Wire(generation, record, Some(reply))).await.map_err(|_| closed())?;
                                if let Some(admit) = receiver.await.map_err(|_| closed())? { admit.await?; }
                                Ok(())
                            }).await.unwrap_or(Err(AttemptError::TimedOut.into()))
                        }));
                        if terminal { receiving = false; }
                    }
                    command = receiver.recv(), if sending.is_none() => match command {
                        Some(TransportCommand::Send(record, reply)) => {
                            let sender = channel.sender();
                            sending = Some(Box::pin(async move {
                                tokio::time::timeout(IO_BOUND, sender.send(record.encode()))
                                    .await.unwrap_or_else(|_| Err(failed("managed send expired")))
                            }));
                            send_reply = reply;
                        }
                        Some(TransportCommand::Priority(priority, reply)) => { let _ = reply.send(channel.request_connection_priority(priority).await); }
                        Some(TransportCommand::Stop { reply, confirmation }) => {
                            if let Some(confirmation) = confirmation {
                                let _ = tokio::time::timeout(IO_BOUND / 2, async {
                                    while let Some(Ok(bytes)) = channel.recv().await {
                                        if Record::decode(&bytes, maximum).is_ok_and(|record| record == confirmation) { break; }
                                    }
                                }).await;
                            }
                            let _ = reply.send(channel.disconnect().await); break;
                        }
                        None => break,
                    }
                }
            }
        });
        attempt.transport = Some(transport);
        attempt.task = Some(task.abort_handle());
    }
    fn dialed(&mut self, generation: u64, result: Result<(DatagramChannel, EndpointId)>) {
        let Some(mut attempt) = self.attempts.remove(&generation) else {
            return;
        };
        match result {
            Ok((channel, endpoint)) => {
                attempt.endpoint = Some(endpoint);
                self.transport(generation, &mut attempt, channel);
                Self::send(
                    &attempt,
                    Record::Request {
                        sender: self.endpoint,
                        target: endpoint,
                        attempt: attempt.wire_attempt,
                        nonce: attempt.nonce,
                    },
                );
                self.attempts.insert(generation, attempt);
            }
            Err(error) => {
                if let Some(reply) = attempt.initiation.take() {
                    let _ = reply.send(Err(error));
                }
            }
        }
    }
    fn send(attempt: &Attempt, record: Record) {
        if let Some(transport) = &attempt.transport {
            let _ = transport.try_send(TransportCommand::Send(record, None));
        }
    }
    fn initiation(&self, generation: u64, attempt: &Attempt) -> ConnectionRequest {
        ConnectionRequest {
            endpoint: attempt.endpoint.expect("validated endpoint"),
            peer: attempt.peer.clone(),
            generation,
            commands: self.commands.clone(),
            cancellation: CancelOnDrop(Some(attempt.cancellation.clone())),
            outcome: attempt.outcome.clone(),
        }
    }
    fn wire(&mut self, generation: u64, record: Option<Result<Record>>) -> Option<Teardown> {
        let mut attempt = self.attempts.remove(&generation)?;
        let mut admission: Option<Teardown> = None;
        let mut failure = None;
        match record {
            Some(Ok(Record::Request {
                sender,
                target,
                attempt: wire_attempt,
                nonce,
            })) if !attempt.outgoing
                && attempt.endpoint.is_none()
                && target == self.endpoint
                && sender != self.endpoint =>
            {
                attempt.endpoint = Some(sender);
                attempt.wire_attempt = wire_attempt;
                attempt.nonce = nonce;
                // Compare signed attempt nonces, never endpoint identities or native addresses.
                let competitors: Vec<_> = self
                    .attempts
                    .iter()
                    .filter(|(_, other)| {
                        other.endpoint == Some(sender)
                            || (other.outgoing && other.peer == attempt.peer)
                    })
                    .map(|(generation, other)| {
                        (
                            *generation,
                            other.nonce,
                            other.channel.is_some(),
                            other.outgoing,
                        )
                    })
                    .collect();
                let mut incoming_wins = true;
                for (other_generation, other_nonce, established, outgoing) in competitors {
                    if established || !outgoing || other_nonce > nonce {
                        incoming_wins = false;
                    } else {
                        if other_nonce == nonce {
                            incoming_wins = false;
                        }
                        self.retire(
                            vec![other_generation],
                            if other_nonce == nonce {
                                AttemptError::EqualNonce
                            } else {
                                AttemptError::Superseded
                            },
                            None,
                        );
                    }
                }
                if incoming_wins {
                    Self::send(
                        &attempt,
                        Record::Response {
                            sender: self.endpoint,
                            target: sender,
                            attempt: wire_attempt,
                        },
                    );
                    let request = self.initiation(generation, &attempt);
                    if self.incoming.try_send(request).is_err() {
                        failure = Some(AttemptError::QueueFull);
                    }
                } else {
                    failure = Some(AttemptError::Superseded);
                }
            }
            Some(Ok(Record::Response {
                sender,
                target,
                attempt: wire_attempt,
            })) if attempt.outgoing
                && attempt.endpoint == Some(sender)
                && target == self.endpoint
                && attempt.wire_attempt == wire_attempt =>
            {
                let request = self.initiation(generation, &attempt);
                if let Some(reply) = attempt.initiation.take() {
                    let _ = reply.send(Ok(request));
                } else {
                    failure = Some(AttemptError::InvalidRecord);
                }
            }
            Some(Ok(Record::Decision {
                attempt: wire_attempt,
                action: 0,
            })) if attempt.wire_attempt == wire_attempt => {
                attempt.remote_accept = true;
            }
            Some(Ok(Record::Ping {
                attempt: wire_attempt,
            })) if attempt.channel.is_some() && attempt.wire_attempt == wire_attempt => {
                attempt.last_received = tokio::time::Instant::now();
                Self::send(
                    &attempt,
                    Record::Pong {
                        attempt: wire_attempt,
                    },
                );
            }
            Some(Ok(Record::Pong {
                attempt: wire_attempt,
            })) if attempt.channel.is_some() && attempt.wire_attempt == wire_attempt => {
                attempt.last_received = tokio::time::Instant::now();
            }
            Some(Ok(Record::Data {
                attempt: wire_attempt,
                payload,
            })) if attempt.channel.is_some() && attempt.wire_attempt == wire_attempt => {
                attempt.last_received = tokio::time::Instant::now();
                // Validate in the state owner, but await admission only in this
                // generation's existing pump. There is at most one pending record.
                if let Some(messages) = attempt.messages.clone() {
                    admission = Some(Box::pin(async move {
                        messages.send(Ok(payload)).await.map_err(|_| closed())
                    }));
                } else {
                    failure = Some(AttemptError::Released);
                }
            }
            Some(Ok(Record::Decision {
                attempt: wire_attempt,
                action,
            })) if attempt.wire_attempt == wire_attempt && action != 0 => {
                attempt.rejection_received = action == 1;
                failure = Some(if action == 1 {
                    AttemptError::Rejected
                } else {
                    AttemptError::Cancelled
                });
            }
            Some(Ok(Record::Disconnect {
                attempt: wire_attempt,
            })) if attempt.wire_attempt == wire_attempt => {
                failure = Some(AttemptError::PeerDisconnected)
            }
            Some(Err(BleError::ReceiveGap(_))) if attempt.channel.is_some() => {
                log::warn!("managed generation {generation}: forwarding known raw receive gap");
                if let Some(state) = attempt.channel.as_ref().and_then(Weak::upgrade) {
                    state.overflow.raise();
                }
            }
            Some(Err(BleError::Protocol(_))) => failure = Some(AttemptError::InvalidRecord),
            None | Some(Err(_)) => failure = Some(AttemptError::PeerLost),
            _ => failure = Some(AttemptError::InvalidRecord),
        }
        self.attempts.insert(generation, attempt);
        if let Some(reason) = failure {
            self.retire(vec![generation], reason, None);
        } else {
            self.establish(generation);
        }
        admission
    }
    fn establish(&mut self, generation: u64) {
        let Some(attempt) = self.attempts.get_mut(&generation) else {
            return;
        };
        if !attempt.local_accept || !attempt.remote_accept || attempt.channel.is_some() {
            return;
        }
        attempt.last_received = tokio::time::Instant::now();
        attempt.next_ping = attempt.last_received + IO_BOUND;
        let (messages, receiver) = mpsc::channel(self.config.datagram.inbound_queue_depth.max(1));
        let state = Arc::new(ChannelState {
            endpoint: attempt.endpoint.expect("validated endpoint"),
            peer: attempt.peer.clone(),
            generation,
            maximum: attempt.maximum,
            transport: attempt.transport.as_ref().expect("transport ready").clone(),
            received: AsyncMutex::new(receiver),
            overflow: Arc::new(datagram::OverflowSignal::default()),
            ended: attempt.ended.clone(),
            released: attempt.released.clone(),
            commands: self.commands.clone(),
            attempt: attempt.wire_attempt,
            channels: self.channels.clone(),
            close_reason: Mutex::new(None),
        });
        self.channels
            .lock()
            .unwrap()
            .insert(attempt.peer.clone(), Arc::downgrade(&state));
        attempt.channel = Some(Arc::downgrade(&state));
        attempt.messages = Some(messages);
        if let Some(reply) = attempt.accepted.take() {
            let _ = reply.send(Ok(PeerChannel { state }));
        }
    }
    fn finish(&mut self, generation: u64, reason: AttemptError) -> Teardown {
        let Some(mut attempt) = self.attempts.remove(&generation) else {
            return Box::pin(async { Ok(()) });
        };
        log::debug!("managed generation {generation} ended: {reason:?}");
        *attempt.outcome.lock().unwrap() = Some(reason);
        if let Some(state) = attempt.channel.as_ref().and_then(Weak::upgrade) {
            *state.close_reason.lock().unwrap() = Some(match reason {
                AttemptError::Disconnected => PeerChannelCloseReason::LocalDisconnect,
                AttemptError::PeerDisconnected => PeerChannelCloseReason::PeerDisconnect,
                AttemptError::Released => PeerChannelCloseReason::Released,
                AttemptError::Closed => PeerChannelCloseReason::Shutdown,
                AttemptError::InvalidRecord | AttemptError::QueueFull => {
                    PeerChannelCloseReason::ProtocolError
                }
                _ => PeerChannelCloseReason::LinkLost,
            });
        }
        attempt.ended.cancel();
        if let Some(reply) = attempt.initiation.take() {
            let _ = reply.send(Err(reason.into()));
        }
        if let Some(reply) = attempt.accepted.take() {
            let _ = reply.send(Err(reason.into()));
        }
        self.retiring.insert(
            attempt.peer.clone(),
            Retirement {
                generation,
                inbound: None,
            },
        );
        let mut channels = self.channels.lock().unwrap();
        if channels
            .get(&attempt.peer)
            .and_then(Weak::upgrade)
            .is_none_or(|state| state.generation == generation)
        {
            channels.remove(&attempt.peer);
        }
        drop(channels);
        Box::pin(async move {
            let teardown = if let Some(transport) = attempt.transport.take() {
                let record = if attempt.channel.is_some() {
                    Record::Disconnect {
                        attempt: attempt.wire_attempt,
                    }
                } else {
                    Record::Decision {
                        attempt: attempt.wire_attempt,
                        action: if reason == AttemptError::Rejected {
                            1
                        } else {
                            2
                        },
                    }
                };
                // Bound both queue admission and native teardown. The worker must not wait on its own event queue.
                tokio::time::timeout(IO_BOUND, async {
                    let (reply, receiver) = oneshot::channel();
                    transport
                        .send(TransportCommand::Send(record, Some(reply)))
                        .await
                        .map_err(|_| closed())?;
                    receiver.await.map_err(|_| closed())??;
                    let (reply, receiver) = oneshot::channel();
                    let confirmation =
                        if reason == AttemptError::Rejected && !attempt.rejection_received {
                            Some(Record::Decision {
                                attempt: attempt.wire_attempt,
                                action: 1,
                            })
                        } else if attempt.channel.is_some()
                            && matches!(
                                reason,
                                AttemptError::Disconnected
                                    | AttemptError::Released
                                    | AttemptError::Closed
                            )
                        {
                            Some(Record::Disconnect {
                                attempt: attempt.wire_attempt,
                            })
                        } else {
                            None
                        };
                    transport
                        .send(TransportCommand::Stop {
                            reply,
                            confirmation,
                        })
                        .await
                        .map_err(|_| closed())?;
                    receiver.await.map_err(|_| closed())?
                })
                .await
                .unwrap_or(Err(AttemptError::TimedOut.into()))
            } else {
                Ok(())
            };
            if let Some(task) = attempt.task.take() {
                task.abort();
            }

            // The peer may finish native teardown after it confirms the control record.
            // An already-disconnected session satisfies retirement, unlike an unsupported operation.
            match teardown {
                Err(BleError::NotConnected(_)) => Ok(()),
                result => result,
            }
        })
    }
}

#[cfg(test)]
mod teardown_tests {
    use super::*;
    #[tokio::test]
    async fn inbound_waits_for_the_matching_acquired_cleanup_completion() {
        use crate::drivers::mock::{MockDriver, MockNetwork};
        let network = MockNetwork::new();
        let make = |name: &str| Arc::new(MockDriver::new(PeerAddress(name.into()), network.clone(), crate::CapabilityReport { central: true, peripheral: true }));
        let a = make("cleanup-a");
        let b = make("cleanup-b");
        let config = PeerLinkConfig::new(datagram::DatagramConfig::new(crate::ServiceUuid(uuid::Uuid::from_u128(11)), crate::CharacteristicUuid(uuid::Uuid::from_u128(12))));
        let _served = datagram::serve_driver(b, &config.datagram).await.unwrap();
        let peer = PeerAddress("cleanup-b".into());
        let channel = datagram::connect_driver(a.clone(), &peer, &config.datagram).await.unwrap();
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _requests) = mpsc::channel(1);
        let mut worker = Worker::new(EndpointId::from_bytes([1; 16]), config, Some(a), commands, receiver, incoming, Arc::new(Mutex::new(HashMap::new())), CancellationToken::new());
        let cleanup = worker.reserve_cleanup(peer.clone(), 1);
        worker.inbound(channel);
        assert!(worker.attempts.is_empty(), "an old acquired cleanup still owns this address");
        drop(cleanup);
        match worker.operations.join_next().await.unwrap().unwrap() {
            Operation::Cleaned(peer, generation, result) => worker.cleaned(peer, generation, result),
            _ => panic!("expected acquired cleanup completion"),
        }
        assert_eq!(worker.attempts.len(), 1, "the held channel must be admitted only after matching completion");
        worker.shutdown.cancel();
        for attempt in worker.attempts.values() { if let Some(task) = &attempt.task { task.abort(); } }
    }

    #[tokio::test]
    async fn awaited_retirement_reports_native_teardown_failure() {
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _requests) = mpsc::channel(1);
        let config = PeerLinkConfig::new(crate::datagram::DatagramConfig::new(
            crate::ServiceUuid(uuid::Uuid::from_u128(11)),
            crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
        ));
        let mut worker = Worker::new(
            EndpointId::from_bytes([1; 16]),
            config,
            None,
            commands,
            receiver,
            incoming,
            Arc::new(Mutex::new(HashMap::new())),
            CancellationToken::new(),
        );
        let mut attempt =
            worker.attempt(PeerAddress("peer".into()), true, CancellationToken::new());
        let (transport, mut receiver) = mpsc::channel(4);
        attempt.transport = Some(transport);
        worker.attempts.insert(1, attempt);
        let handler = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    TransportCommand::Send(_, Some(reply)) => {
                        let _ = reply.send(Ok(()));
                    }
                    TransportCommand::Stop { reply, .. } => {
                        let _ = reply.send(Err(BleError::Unsupported(
                            "native disconnect refused".into(),
                        )));
                        break;
                    }
                    _ => panic!("unexpected transport command"),
                }
            }
        });
        let result = worker.finish(1, AttemptError::Released).await;
        assert!(
            matches!(result, Err(BleError::Unsupported(_))),
            "native teardown failure was swallowed: {result:?}"
        );
        handler.await.unwrap();
    }
}
