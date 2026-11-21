//! One worker owns managed attempt state and native channel use.
use super::{
    acquired_cleanup::Cleanup,
    managed_codec::{self, ManagedMetadata, RadioRecord, Record},
    managed_driver::ManagedDriver,
    peer_link::*,
};
use crate::advertisement::{GattAdvertisement, MetadataCache, MetadataKey, TrackedPeer};
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
/// Bound on one native show (a startup, restore or recovery). It is longer
/// than the drivers' own bounds, so a driver reports its own failure and
/// cleans up first: Windows creates its provider, then waits
/// `ADVERTISE_START_TIMEOUT` (5 s) for the start.
const ADVERTISE_BOUND: Duration = Duration::from_secs(15);
type DiscoveryOutput = mpsc::Sender<Result<(EndpointId, DiscoveredPeer)>>;
type DiscoveryScan = (BoxStream<Result<DiscoveredPeer>>, DiscoveryOutput);
const MAX_PENDING_OPERATIONS: usize = 64;
type Teardown = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
enum Operation {
    Cleaned(PeerAddress, u64, Result<()>),
    /// A visibility change finished: its result, the caller, whether it
    /// showed, whether it was a startup or a restore, and the loss revision.
    Advertising(
        Result<Option<BoxStream<DatagramChannel>>>,
        Reply<()>,
        bool,
        bool,
        u64,
    ),
    /// A failed startup, restore or recovery was stopped: the stop result,
    /// and the advertise error for the caller (none for a recovery).
    Withdrawn(Result<()>, BleError, Option<Reply<()>>),
    Metadata(Result<()>, Reply<()>, u64),
    Recovered(Result<()>, u64),
    Discovery(
        Result<BoxStream<Result<DiscoveredPeer>>>,
        Reply<BoxStream<Result<(EndpointId, DiscoveredPeer)>>>,
    ),
    /// A metadata read finished: the scan generation, the address, the key
    /// the read was scheduled for, and the endpoint, the complete peer and
    /// the hash of the readable value read.
    Probe(
        u64,
        PeerAddress,
        Option<MetadataKey>,
        DiscoveryOutput,
        Result<(EndpointId, DiscoveredPeer, u32)>,
    ),
    Retired(Vec<(PeerAddress, u64)>, Result<()>),
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
    UpdateAdvertisement(crate::advertisement::GattAdvertisement, Reply<()>),
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
    endpoint: Option<EndpointId>,
    complete: bool,
    waiters: Vec<Arc<Mutex<RetirementReply>>>,
    error: Option<BleError>,
    // One newly acquired raw channel may arrive before the previous native
    // cleanup completion is dispatched. Keep it under the existing reservation;
    // do not admit its managed attempt or disconnect its newer driver session.
    inbound: Option<DatagramChannel>,
}
struct RetirementReply {
    remaining: usize,
    reply: Option<Reply<()>>,
    error: Option<BleError>,
}
/// When a failed recovery (the restore after a radio loss) runs again: 1 s
/// after the first failure, doubling up to 30 s, until one succeeds.
#[derive(Default)]
struct RecoveryRetry {
    at: Option<tokio::time::Instant>,
    failures: u32,
}
impl RecoveryRetry {
    const FIRST: Duration = Duration::from_secs(1);
    const LONGEST: Duration = Duration::from_secs(30);
    fn due(&self) -> bool {
        !self.at.is_some_and(|at| tokio::time::Instant::now() < at)
    }
    fn failed(&mut self, error: &BleError) {
        let wait = Self::FIRST
            .saturating_mul(1 << self.failures.min(5))
            .min(Self::LONGEST);
        self.failures = self.failures.saturating_add(1);
        self.at = Some(tokio::time::Instant::now() + wait);
        log::warn!("advertise: recovery failed ({error}), next try in {wait:?}");
    }
    fn succeeded(&mut self) {
        *self = Self::default();
    }
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
    retiring: HashMap<(PeerAddress, u64), Retirement>,
    probing: HashMap<PeerAddress, u64>,
    /// Completion reads in flight, cancelled when a dial to the address is
    /// requested: a name read must not hold a link the application wants.
    completion_reads: HashMap<PeerAddress, CancellationToken>,
    /// Readable values by the radio record that names them, for the
    /// worker's lifetime (not one scan): see `MetadataCache`.
    metadata_cache: MetadataCache<(EndpointId, GattAdvertisement)>,
    /// Where a read found each endpoint, and the reverse: joins a fast
    /// advertisement sent from an address of its own (a Windows publisher,
    /// not connectable) to the connectable address of the same endpoint.
    endpoint_addresses: HashMap<EndpointId, PeerAddress>,
    address_endpoints: HashMap<PeerAddress, EndpointId>,
    /// The value the last read at each address found (its cache key).
    address_keys: HashMap<PeerAddress, MetadataKey>,
    /// The newest record key each such publisher advertised.
    published: HashMap<EndpointId, MetadataKey>,
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
            completion_reads: HashMap::new(),
            metadata_cache: MetadataCache::default(),
            endpoint_addresses: HashMap::new(),
            address_endpoints: HashMap::new(),
            address_keys: HashMap::new(),
            published: HashMap::new(),
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
        // A scan result the full output could not take: the scan is not read
        // again until the application takes one.
        let mut scan_held: Option<Result<(EndpointId, DiscoveredPeer)>> = None;
        let mut tracked_metadata = HashMap::<PeerAddress, TrackedPeer>::new();
        let mut scan_exhausted = false;
        let mut scan_generation = 0u64;
        let mut advertising_pending = false;
        let mut metadata_uncertain = false;
        let mut metadata_fenced = false;
        let mut native_present = false;
        // Set once this worker starts its own server: close stops only that,
        // never a server the caller runs on the same driver.
        let mut server_acquired = false;
        let mut desired_visible = false;
        let mut recovery_needed = false;
        let mut recovery_retry = RecoveryRetry::default();
        let mut radio_available = true;
        let mut loss_revision = 0u64;
        let mut lifecycle: BoxStream<crate::GattEvent> =
            Box::pin(driver.lifecycle_events().filter(|event| {
                matches!(
                    event,
                    crate::GattEvent::RadioChanged { .. } | crate::GattEvent::Lagged { .. }
                )
            }));
        let mut discovery_pending = false;
        let mut close_reply = None;
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            if recovery_needed
                && radio_available
                && desired_visible
                && !metadata_uncertain
                && !advertising_pending
                && self.operation_capacity()
                && recovery_retry.due()
            {
                recovery_needed = false;
                match (ManagedMetadata {
                    endpoint: self.endpoint,
                })
                .service_spec(&self.config.datagram)
                {
                    Ok(spec) => {
                        advertising_pending = true;
                        server_acquired = true;
                        let driver = driver.clone();
                        let shutdown = self.shutdown.clone();
                        let revision = loss_revision;
                        self.operations.spawn(async move {
                            let result = tokio::select! {
                                biased;
                                _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                result = tokio::time::timeout(ADVERTISE_BOUND, driver.advertise(spec)) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                            };
                            Operation::Recovered(result, revision)
                        });
                    }
                    Err(_) => {
                        metadata_uncertain = true;
                        metadata_fenced = false;
                    }
                }
            }
            let scan_output = scan.as_ref().map(|(_, output)| output.clone());
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                event = lifecycle.next() => {
                    match event {
                        Some(crate::GattEvent::RadioChanged { status: crate::RadioStatus::On }) => { radio_available = true; }
                        Some(crate::GattEvent::RadioChanged { .. }) => {
                            radio_available = false;
                            native_present = false;
                            loss_revision = loss_revision.wrapping_add(1);
                            recovery_needed = served.is_some() || advertising_pending;
                        }
                        Some(crate::GattEvent::Lagged { .. }) => {
                            // Missed events do not confirm radio loss or native absence.
                            loss_revision = loss_revision.wrapping_add(1);
                            if served.is_some() || advertising_pending {
                                metadata_uncertain = true;
                                metadata_fenced = false;
                            }
                        }
                        _ => lifecycle = Box::pin(tokio_stream::pending()),
                    }
                }
                completed = self.operations.join_next(), if !self.operations.is_empty() => {
                    if let Some(Ok(operation)) = completed {
                        match operation {
                            Operation::Cleaned(peer, generation, result) => self.cleaned(peer, generation, result),
                            Operation::Advertising(mut result, reply, enabled, fresh, revision) => {
                                if revision != loss_revision { result = Err(AttemptError::PeerLost.into()); }
                                advertising_pending = false;
                                if result.is_err() { metadata_uncertain = true; metadata_fenced = false; }
                                else {
                                    if enabled { native_present = true; recovery_needed = false; recovery_retry.succeeded(); }
                                    else {
                                        // Successful hide confirms a retained server and invalidates late callbacks.
                                        native_present = true;
                                        if metadata_uncertain { metadata_fenced = true; }
                                    }
                                }
                                match result {
                                    Err(error) if fresh && served.is_none() && !native_present => { advertising_pending = true; self.withdraw(driver.clone(), error, Some(reply)); }
                                    result => { let _ = reply.send(result.map(|stream| { if let Some(stream) = stream { served = Some(stream); } })); }
                                }
                            }
                            Operation::Withdrawn(stopped, error, reply) => {
                                advertising_pending = false;
                                // Nothing is served now: nothing to stop at close. A failed startup has
                                // nothing to recover; a failed recovery (no caller) runs again later.
                                if stopped.is_ok() { server_acquired = false; metadata_uncertain = false; metadata_fenced = false; recovery_needed &= reply.is_none(); }
                                if let Some(reply) = reply { let _ = reply.send(Err(error)); }
                            }
                            Operation::Metadata(mut result, reply, revision) => {
                                if revision != loss_revision { result = Err(AttemptError::PeerLost.into()); }
                                advertising_pending = false;
                                metadata_uncertain = result.is_err();
                                metadata_fenced = false;
                                let _ = reply.send(result);
                            }
                            Operation::Recovered(result, revision) => {
                                advertising_pending = false;
                                match if revision == loss_revision { result } else { Err(AttemptError::PeerLost.into()) } {
                                    Ok(()) => { native_present = true; recovery_retry.succeeded(); }
                                    // Stop what it may have left behind, like a failed startup, then try again.
                                    Err(error) => { recovery_needed = true; recovery_retry.failed(&error); advertising_pending = true; self.withdraw(driver.clone(), error, None); }
                                }
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
                            Operation::Probe(generation, peer, key, output, result) => {
                                if self.probing.get(&peer) == Some(&generation) { self.probing.remove(&peer); }
                                self.completion_reads.remove(&peer);
                                let tracked = (generation == scan_generation).then_some(&mut tracked_metadata);
                                if let Some(item) = self.probe_finished(tracked, &peer, key, result) { let _ = output.try_send(item); }
                            }
                            Operation::Retired(targets, result) => {
                                self.retired(targets, result);
                            }
                        }
                    }
                }
                _ = tick.tick() => {
                    let now = tokio::time::Instant::now();
                    if let Some((_, output)) = &scan {
                        let output = output.clone();
                        let due: Vec<_> = tracked_metadata.iter().filter(|(_, entry)| entry.managed_due(now, !scan_exhausted)).map(|(address, entry)| (address.clone(), entry.radio_peer())).collect();
                        for (address, peer) in due {
                            let entry = tracked_metadata.get_mut(&address).expect("tracked peer");
                            if let Some((endpoint, fields)) = entry.key().and_then(|key| self.metadata_cache.hit(key, now)) {
                                // Another read (of this address or a rotated one) already has it.
                                let (endpoint, peer) = (*endpoint, Self::cached_peer(fields, peer));
                                if entry.managed_result(Some(&peer), None) { let _ = output.try_send(Ok((endpoint, peer))); }
                                continue;
                            }
                            let completion = entry.is_completion();
                            match self.probe_metadata(driver.clone(), peer, entry.key(), output.clone(), scan_generation, completion) {
                                Ok(()) => entry.managed_scheduled(),
                                Err(error) => {
                                    let initial = entry.managed_initial();
                                    entry.managed_deferred();
                                    // A completion read is low priority: it waits, silently, while a
                                    // dial or another read holds the address. Its peer is listed already.
                                    if initial && !completion { let _ = output.try_send(Err(error)); }
                                }
                            }
                        }
                        if scan_exhausted && !tracked_metadata.values().any(|entry| entry.managed_initial()) && !self.probing.values().any(|generation| *generation == scan_generation) { scan = None; }
                    }
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
                permit = async { match &scan_output { Some(output) => output.reserve().await, None => std::future::pending().await } }, if scan_held.is_some() => {
                    match permit { Ok(permit) => permit.send(scan_held.take().expect("held scan result")), Err(_) => scan = None }
                }
                item = async { match &mut scan { Some((stream, _)) => stream.next().await, None => std::future::pending().await } }, if scan_held.is_none() => {
                    match item {
                        Some(item) => {
                            let result = match item {
                                Ok(peer) => match self.sighting(peer, &mut tracked_metadata) {
                                    Some(result) => result,
                                    None => continue,
                                },
                                Err(error) => Err(error),
                            };
                            // A full output holds the result (backpressure); only a closed one ends the scan.
                            if let Some((_, output)) = &scan { match output.try_send(result) { Err(mpsc::error::TrySendError::Full(result)) => scan_held = Some(result), Err(mpsc::error::TrySendError::Closed(_)) => scan = None, Ok(()) => {} } }
                        }
                        None => {
                            scan_exhausted = true;
                            if let Some((stream, _)) = &mut scan { *stream = Box::pin(tokio_stream::pending()); }
                        },
                    }
                }
                item = async { match &mut served { Some(stream) => stream.next().await, None => std::future::pending().await } } => {
                    if let Some(channel) = item { self.inbound(channel); } else { served = None; }
                }
                command = self.receiver.recv() => {
                    let Some(command) = command else { break; };
                    match command {
                        #[cfg(test)] Command::SetNonce(value, reply) => { self.next_nonce = Some(value); let _ = reply.send(Ok(())); }
                        Command::UpdateAdvertisement(advertisement, reply) => {
                            if advertising_pending || (metadata_uncertain && !metadata_fenced) || !self.operation_capacity() {
                                let _ = reply.send(Err(BleError::GattBusy("managed native operation pending".into())));
                                continue;
                            }
                            let mut config = self.config.datagram.clone();
                            config.advertised_manufacturer_data = advertisement.manufacturer_data;
                            config.advertised_service_data = advertisement.service_data;
                            config.advertised_local_name = advertisement.local_name;
                            let spec = match (ManagedMetadata { endpoint: self.endpoint }).service_spec(&config) {
                                Ok(spec) => spec,
                                Err(error) => { let _ = reply.send(Err(error)); continue; }
                            };
                            self.config.datagram = config;
                            if !native_present { let _ = reply.send(Ok(())); continue; }
                            advertising_pending = true;
                            let driver = driver.clone();
                            let shutdown = self.shutdown.clone();
                            let revision = loss_revision;
                            self.operations.spawn(async move {
                                let result = tokio::select! {
                                    biased;
                                    _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                    result = tokio::time::timeout(IO_BOUND, datagram::DatagramConfig::update_spec(driver.as_ref(), spec)) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                                };
                                Operation::Metadata(result, reply, revision)
                            });
                        }
                        Command::Advertising(enabled, reply) => {
                            if advertising_pending || (enabled && metadata_uncertain) || !self.operation_capacity() {
                                let _ = reply.send(Err(BleError::GattBusy("managed native operation pending".into())));
                                continue;
                            }
                            if !native_present && !enabled && !metadata_uncertain { desired_visible = false; let _ = reply.send(Ok(())); continue; }
                            advertising_pending = true;
                            let startup = served.is_none() && enabled;
                            let restore = !native_present && enabled;
                            let revision = loss_revision;
                            let config = self.config.datagram.clone();
                            let spec = match (ManagedMetadata { endpoint: self.endpoint }).service_spec(&config) {
                                Ok(spec) => spec,
                                Err(error) => { advertising_pending = false; let _ = reply.send(Err(error)); continue; }
                            };
                            desired_visible = enabled;
                            let fresh = startup || restore;
                            server_acquired |= fresh;
                            let driver = driver.clone();
                            let shutdown = self.shutdown.clone();
                            self.operations.spawn(async move {
                                let result = tokio::select! {
                                    biased;
                                    _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                                    result = tokio::time::timeout(ADVERTISE_BOUND, async {
                                        if startup { config.serve_spec(driver.clone(), spec, false).await.map(Some) }
                                        else if restore { driver.advertise(spec).await.map(|()| None) }
                                        else { driver.set_advertising_enabled(enabled).await.map(|()| None) }
                                    }) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
                                };
                                Operation::Advertising(result, reply, enabled, fresh, revision)
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
                            } else { let _ = reply.send(Err(PeerLink::closed_error())); }
                        }
                        Command::Reject(generation, reply) => self.retire(vec![generation], AttemptError::Rejected, Some(reply)),
                        Command::Disconnect(endpoint, reply) => {
                            let generations = self.endpoint_generations(endpoint);
                            self.retire(generations, AttemptError::Disconnected, Some(reply));
                        }
                        Command::Retire(generation, reply) => self.retire(vec![generation], AttemptError::Released, Some(reply)),
                        Command::DisconnectGeneration(generation, reply) => self.retire(vec![generation], AttemptError::Disconnected, Some(reply)),
                        Command::Discover(reply) => {
                            if discovery_pending || !self.operation_capacity() || scan.as_ref().is_some_and(|(_, output)| !output.is_closed()) {
                                let _ = reply.send(Err(BleError::AdapterBusy("managed discovery already active or operation limit reached".into())));
                            } else {
                                scan = None;
                                scan_held = None;
                                scan_generation += 1;
                                tracked_metadata.clear();
                                scan_exhausted = false;
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
                        if let Some(error) = self.cleanup_failure.take() {
                            teardown = Err(error);
                        }
                    }
                    Ok(Operation::Retired(targets, result)) => {
                        if let Err(error) = &result {
                            teardown = Err(error.copy_error());
                        }
                        self.retired(targets, result);
                    }
                    Ok(
                        Operation::Advertising(_, reply, _, _, _)
                        | Operation::Metadata(_, reply, _),
                    ) => {
                        let _ = reply.send(Err(AttemptError::Cancelled.into()));
                    }
                    Ok(Operation::Withdrawn(_, error, Some(reply))) => {
                        let _ = reply.send(Err(error));
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
        // An aborted/panicking operation must never turn a retained explicit
        // retirement reply into success or silently drop its typed outcome.
        for retirement in self.retiring.values_mut() {
            for waiter in retirement.waiters.drain(..) {
                let mut waiter = waiter.lock().unwrap();
                if let Some(reply) = waiter.reply.take() {
                    let reason = if drained.is_err() {
                        AttemptError::TimedOut
                    } else {
                        AttemptError::Cancelled
                    };
                    let _ = reply.send(Err(reason.into()));
                }
            }
        }
        let stopped = if server_acquired {
            tokio::time::timeout(IO_BOUND, driver.stop_advertising())
                .await
                .unwrap_or(Err(AttemptError::TimedOut.into()))
        } else {
            Ok(())
        };
        if let Some(reply) = close_reply {
            let _ = reply.send(stopped.and(teardown));
        }
    }
    /// One result of a managed scan (ADR-0012 §2): what to report now, if
    /// anything. A fast advertisement is reported at once, an advertisement
    /// header from the cache, and anything else after a read.
    fn sighting(
        &mut self,
        peer: DiscoveredPeer,
        tracked: &mut HashMap<PeerAddress, TrackedPeer>,
    ) -> Option<Result<(EndpointId, DiscoveredPeer)>> {
        let service = self.config.datagram.service;
        let company = self.config.datagram.fast_advertisement_company;
        let now = tokio::time::Instant::now();
        let record = RadioRecord::find(&peer, service, company);
        let mut radio = match &record {
            Some(record) => record.radio_fields(peer, service, company),
            None => peer,
        };
        if let Some(attempt) = self
            .attempts
            .values()
            .find(|attempt| attempt.peer == radio.address)
        {
            // A pending dial may not have validated metadata yet. Defer it.
            let endpoint = attempt.endpoint?;
            if record.is_some() || !radio.service_data.contains_key(&service) {
                radio
                    .service_data
                    .insert(service, ManagedMetadata { endpoint }.encode());
                return Some(Ok((endpoint, radio)));
            }
        }
        match record {
            Some(RadioRecord::Fast { hash, endpoint, .. }) => {
                let key = MetadataKey {
                    tag: endpoint.tag(),
                    hash,
                };
                if !radio.services.contains(&service) {
                    // A publisher's advertisement of its own, without the service
                    // and not connectable (a Windows peer, ADR-0011 §3). Remember its
                    // record. Report it at the connectable address a read found for
                    // this endpoint; until a read did, the provider's advertisement
                    // is read as a peer without a record.
                    self.remember_published(endpoint, key);
                    let address = self.endpoint_addresses.get(&endpoint).cloned()?;
                    radio.address = address;
                    radio.services.push(service);
                }
                if tracked
                    .get(&radio.address)
                    .is_some_and(|entry| entry.is_unnamed())
                {
                    tracked.remove(&radio.address);
                }
                if let Some((_, fields)) = self
                    .metadata_cache
                    .fresh_hit(key, &radio, now)
                    .filter(|(cached, _)| *cached == endpoint)
                {
                    return Some(Ok((endpoint, Self::cached_peer(fields, radio))));
                }
                let complete = ManagedMetadata::radio_hash(endpoint, service, &radio) == hash;
                radio
                    .service_data
                    .insert(service, ManagedMetadata { endpoint }.encode());
                if complete {
                    log::debug!(
                        "discover: {} complete from its fast advertisement",
                        radio.address.0
                    );
                    self.metadata_cache
                        .succeeded(key, (endpoint, Self::fields(&radio)), now);
                } else if let Some(entry) = tracked.get_mut(&radio.address) {
                    let read_at = self.metadata_cache.retry_at(key, now);
                    entry.update_keyed(radio.clone(), key, read_at, true);
                } else if self.tracked_room(tracked) {
                    // Reported now; the fields the radio could not carry (a
                    // name, on Android) follow from one read.
                    log::debug!(
                        "discover: {} from its fast advertisement; one read completes it",
                        radio.address.0
                    );
                    let read_at = self.metadata_cache.retry_at(key, now);
                    tracked.insert(
                        radio.address.clone(),
                        TrackedPeer::completion(radio.clone(), key, read_at),
                    );
                }
                Some(Ok((endpoint, radio)))
            }
            Some(RadioRecord::Header { hash, tag }) => {
                let key = MetadataKey { tag, hash };
                if let Some((endpoint, fields)) = self.metadata_cache.fresh_hit(key, &radio, now) {
                    let endpoint = *endpoint;
                    return Some(Ok((endpoint, Self::cached_peer(fields, radio))));
                }
                if self.metadata_cache.hit(key, now).is_some() {
                    // Cached, but a stale platform entry: not a sighting.
                    return None;
                }
                let read_at = self.metadata_cache.retry_at(key, now);
                if let Some(entry) = tracked.get_mut(&radio.address) {
                    // Not cached (expired or evicted): a finished read runs again.
                    entry.update_keyed(radio, key, read_at, false);
                    entry.rearm(read_at);
                } else if self.tracked_room(tracked) {
                    tracked.insert(
                        radio.address.clone(),
                        TrackedPeer::header(radio, key, read_at),
                    );
                }
                None
            }
            None => {
                if !radio.services.contains(&service) && !radio.service_data.contains_key(&service)
                {
                    // Not a managed peer: another company's data shaped like a
                    // fast advertisement passed the scanner. Do not read it.
                    return None;
                }
                let own = radio.service_data.get(&service).map(|bytes| {
                    let unknown_record = bytes.first().is_some_and(|tag| tag & 0xF8 == 0xF8);
                    (unknown_record, ManagedMetadata::decode(bytes))
                });
                match own {
                    // A radio record of a version this side does not know:
                    // read the readable value instead.
                    Some((true, _)) => {
                        radio.service_data.remove(&service);
                    }
                    Some((false, metadata)) => {
                        return Some(metadata.map(|metadata| (metadata.endpoint, radio)));
                    }
                    None => {}
                }
                let published = self
                    .address_endpoints
                    .get(&radio.address)
                    .and_then(|endpoint| self.published.get(endpoint))
                    .copied();
                if let Some(key) = published {
                    // A read found this address's endpoint, and its publisher names
                    // the current value: answer from the cache, or read once per key.
                    if let Some((endpoint, fields)) =
                        self.metadata_cache.fresh_hit(key, &radio, now)
                    {
                        let endpoint = *endpoint;
                        return Some(Ok((endpoint, Self::cached_peer(fields, radio))));
                    }
                    let read_at = self.metadata_cache.retry_at(key, now);
                    let gone = self.metadata_cache.hit(key, now).is_none();
                    if let Some(entry) = tracked.get_mut(&radio.address) {
                        entry.update_keyed(radio, key, read_at, false);
                        if gone {
                            entry.rearm(read_at);
                        }
                    } else if self.tracked_room(tracked) {
                        tracked.insert(
                            radio.address.clone(),
                            TrackedPeer::header(radio, key, read_at),
                        );
                    }
                    return None;
                }
                // A read of this address found its endpoint before: report that
                // value on every fresh sighting, and read again only when the radio
                // fields change or after `KNOWN_PEER_REFRESH`. Reading a phone's or a
                // PC's metadata every 10 s held a link to it half of the time, and
                // the other side's reads crossed those links (ADR-0012 §2).
                let known = self
                    .address_keys
                    .get(&radio.address)
                    .copied()
                    .filter(|key| self.metadata_cache.hit(*key, now).is_some());
                if let Some(entry) = tracked.get_mut(&radio.address) {
                    if entry.is_unnamed() {
                        entry.update_radio(radio.clone());
                    }
                } else if self.tracked_room(tracked) {
                    let entry = match known {
                        Some(_) => TrackedPeer::known(radio.clone()),
                        None => TrackedPeer::managed(radio.clone()),
                    };
                    tracked.insert(radio.address.clone(), entry);
                }
                let key = known?;
                let (endpoint, fields) = self.metadata_cache.fresh_hit(key, &radio, now)?;
                let endpoint = *endpoint;
                Some(Ok((endpoint, Self::cached_peer(fields, radio))))
            }
        }
    }

    /// Whether a scan's table of tracked peers has room for one more. A full
    /// table forgets its stalest peer without a read in flight (that read
    /// still needs its entry); with none such, there is no room.
    fn tracked_room(&self, tracked: &mut HashMap<PeerAddress, TrackedPeer>) -> bool {
        if tracked.len() < crate::advertisement::METADATA_CACHE_CAPACITY {
            return true;
        }
        let stalest = tracked
            .iter()
            .filter(|(address, _)| !self.probing.contains_key(*address))
            .min_by_key(|(_, entry)| entry.last_seen())
            .map(|(address, _)| address.clone());
        match stalest {
            Some(address) => {
                tracked.remove(&address);
                true
            }
            None => false,
        }
    }

    /// Remember the record a publisher advertises for `endpoint`. Bounded
    /// like the cache: a crowd of endpoints starts the maps over.
    fn remember_published(&mut self, endpoint: EndpointId, key: MetadataKey) {
        if self.published.len() >= crate::advertisement::METADATA_CACHE_CAPACITY
            && !self.published.contains_key(&endpoint)
        {
            self.published.clear();
        }
        self.published.insert(endpoint, key);
    }

    /// Remember that a read at `address` found `endpoint` and the value `key`.
    fn remember_address(&mut self, endpoint: EndpointId, address: &PeerAddress, key: MetadataKey) {
        if self.endpoint_addresses.len() >= crate::advertisement::METADATA_CACHE_CAPACITY
            && !self.endpoint_addresses.contains_key(&endpoint)
        {
            self.endpoint_addresses.clear();
            self.address_endpoints.clear();
            self.address_keys.clear();
        }
        if let Some(old) = self.endpoint_addresses.insert(endpoint, address.clone()) {
            self.address_endpoints.remove(&old);
            self.address_keys.remove(&old);
        }
        self.address_endpoints.insert(address.clone(), endpoint);
        self.address_keys.insert(address.clone(), key);
    }

    /// The radio fields of `peer` as a readable value.
    fn fields(peer: &DiscoveredPeer) -> GattAdvertisement {
        GattAdvertisement {
            manufacturer_data: peer.manufacturer_data.clone(),
            service_data: peer.service_data.clone(),
            local_name: peer.name.clone(),
        }
    }

    /// `radio` (address, services, signal strength) with the readable
    /// fields read for it earlier.
    fn cached_peer(fields: &GattAdvertisement, mut radio: DiscoveredPeer) -> DiscoveredPeer {
        radio.manufacturer_data = fields.manufacturer_data.clone();
        radio.service_data = fields.service_data.clone();
        radio.name = fields.local_name.clone();
        radio
    }

    /// A metadata read finished. Remembers a successful read for the
    /// records that name it, and returns what to report: changed fields, or
    /// the first failure of a peer not reported yet. `tracked` is `None`
    /// for a read of an earlier scan, which reports nothing. `key` is the
    /// value the read was scheduled for: a sighting during the read may
    /// have named another one since.
    fn probe_finished(
        &mut self,
        tracked: Option<&mut HashMap<PeerAddress, TrackedPeer>>,
        address: &PeerAddress,
        key: Option<MetadataKey>,
        result: Result<(EndpointId, DiscoveredPeer, u32)>,
    ) -> Option<Result<(EndpointId, DiscoveredPeer)>> {
        let now = tokio::time::Instant::now();
        let current = tracked.is_some();
        let entry = tracked.and_then(|tracked| tracked.get_mut(address));
        // A record names its endpoint (a tag, or the whole endpoint): a read
        // answering for another one is a failure, not a value to remember.
        let result = result.and_then(|(endpoint, peer, hash)| match key {
            Some(key) if key.tag != endpoint.tag() => Err(PeerLink::protocol_error(
                "the readable endpoint does not match the advertised one",
            )),
            _ => Ok((endpoint, peer, hash)),
        });
        match &result {
            Ok((endpoint, peer, hash)) => {
                let value = (*endpoint, Self::fields(peer));
                let read = MetadataKey {
                    tag: endpoint.tag(),
                    hash: *hash,
                };
                self.remember_address(*endpoint, address, read);
                if let Some(key) = key.filter(|key| *key != read) {
                    // The radio named an older value: remember the newer one
                    // under that name too, so it is not read again.
                    self.metadata_cache.succeeded(key, value.clone(), now);
                }
                self.metadata_cache.succeeded(read, value, now);
            }
            Err(error) => {
                if let Some(key) = key {
                    let wait = self.metadata_cache.failed(key, now);
                    log::debug!(
                        "discover: reading {} failed ({error}), next try in {wait:?}",
                        address.0
                    );
                }
            }
        }
        let Some(entry) = entry else {
            // This scan stopped tracking the address (a record arrived): report
            // the read as before. A read of an earlier scan reports nothing.
            return current.then(|| result.map(|(endpoint, peer, _)| (endpoint, peer)));
        };
        if entry.key() != key {
            // The radio named another value during the read: this read does
            // not complete the entry, which waits for its own.
            return None;
        }
        let retry_at = key.map(|key| self.metadata_cache.retry_at(key, now));
        let report = entry.managed_result(result.as_ref().ok().map(|(_, peer, _)| peer), retry_at);
        if result.is_ok() && entry.is_unnamed() {
            // Its endpoint is known now: sightings report this value, and the
            // next read waits `KNOWN_PEER_REFRESH`.
            entry.known_refresh();
        }
        report.then(|| result.map(|(endpoint, peer, _)| (endpoint, peer)))
    }

    /// Read the metadata of `peer`, scheduled for `key` (the value its radio
    /// record named, if any).
    fn probe_metadata(
        &mut self,
        driver: Arc<dyn Driver>,
        peer: DiscoveredPeer,
        key: Option<MetadataKey>,
        output: DiscoveryOutput,
        generation: u64,
        completion: bool,
    ) -> Result<()> {
        if self.acquired.contains_key(&peer.address)
            || self.probing.contains_key(&peer.address)
            || self.address_retiring(&peer.address)
            || self
                .attempts
                .values()
                .any(|attempt| attempt.peer == peer.address)
        {
            return Err(BleError::AdapterBusy("peer address is reserved".into()));
        }
        if self.link_count() >= self.config.max_links.0
            || self.operations.len() + self.attempts.len() + self.retiring.len() + 2
                > MAX_PENDING_OPERATIONS
        {
            return Err(AttemptError::QueueFull.into());
        }
        let cleanup_generation = self.generation();
        let cleanup = self.reserve_cleanup(peer.address.clone(), cleanup_generation);
        let service = self.config.datagram.service;
        let driver = Arc::new(ManagedDriver {
            inner: driver,
            service,
            endpoint: Arc::new(Mutex::new(None)),
            cleanup,
            validate_metadata: false,
        });
        let shutdown = self.shutdown.clone();
        let address = peer.address.clone();
        self.probing.insert(address.clone(), generation);
        let yielded = CancellationToken::new();
        if completion {
            self.completion_reads
                .insert(address.clone(), yielded.clone());
        }
        let read_timeout = if completion {
            crate::advertisement::COMPLETION_READ_TIMEOUT
        } else {
            crate::advertisement::GattAdvertisementPolicy::default().timeout
        };
        self.operations.spawn(async move {
            let read = tokio::select! {
                biased;
                _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                _ = output.closed() => Err(AttemptError::Cancelled.into()),
                _ = yielded.cancelled() => Err(AttemptError::Cancelled.into()),
                // One read includes the first connection to the peer; bound it like an unmanaged
                // GATT advertisement read. Windows took 3-5 s and more for a first connection to a
                // phone, and a read cut at `IO_BOUND` started over after the next scan.
                // A completion read has a shorter deadline (`COMPLETION_READ_TIMEOUT`).
                result = tokio::time::timeout(read_timeout, crate::advertisement::GattAdvertisement::read_managed(driver.as_ref(), &peer.address)) => result.unwrap_or(Err(AttemptError::TimedOut.into())),
            };
            let result = read.and_then(|metadata| {
                let hash = RadioRecord::metadata_hash(&metadata.encode());
                ManagedMetadata::complete_peer(metadata, service, peer)
                    .map(|(endpoint, peer)| (endpoint, peer, hash))
            });
            Operation::Probe(generation, address, key, output, result)
        });
        Ok(())
    }
    /// A startup, restore or recovery failed with nothing served: stop what
    /// it may have left behind (a timed-out advertise can still finish),
    /// bounded, then give the caller, if any, `error`. A stopped server is a
    /// known state, so the next show or recovery may start again.
    fn withdraw(&mut self, driver: Arc<dyn Driver>, error: BleError, reply: Option<Reply<()>>) {
        let shutdown = self.shutdown.clone();
        self.operations.spawn(async move {
            let stopped = tokio::select! {
                biased;
                _ = shutdown.cancelled() => Err(AttemptError::Cancelled.into()),
                result = tokio::time::timeout(IO_BOUND, driver.stop_advertising()) => {
                    result.unwrap_or(Err(AttemptError::TimedOut.into()))
                }
            };
            Operation::Withdrawn(stopped, error, reply)
        });
    }
    fn address_retiring(&self, peer: &PeerAddress) -> bool {
        self.retiring.keys().any(|(address, _)| address == peer)
    }
    fn operation_capacity(&self) -> bool {
        self.operations.len() + self.attempts.len() + self.retiring.len() < MAX_PENDING_OPERATIONS
    }
    fn endpoint_generations(&self, endpoint: EndpointId) -> Vec<u64> {
        self.attempts
            .iter()
            .filter(|(_, a)| a.endpoint == Some(endpoint))
            .map(|(generation, _)| *generation)
            .chain(
                self.retiring
                    .values()
                    .filter(|r| r.endpoint == Some(endpoint))
                    .map(|r| r.generation),
            )
            .collect()
    }
    fn link_count(&self) -> usize {
        self.attempts.len()
            + self.retiring.len()
            + self
                .acquired
                .iter()
                .filter(|(peer, generation)| {
                    !self.retiring.contains_key(&((*peer).clone(), **generation))
                        && !self.attempts.contains_key(*generation)
                })
                .count()
    }
    fn reserve_cleanup(&mut self, peer: PeerAddress, generation: u64) -> Cleanup {
        let (cleanup, completion) = Cleanup::reserved();
        self.acquired.insert(peer.clone(), generation);
        self.operations
            .spawn(async move { Operation::Cleaned(peer, generation, completion.await) });
        cleanup
    }
    fn cleaned(&mut self, peer: PeerAddress, generation: u64, result: Result<()>) {
        if self.acquired.get(&peer) != Some(&generation) {
            return;
        }
        self.acquired.remove(&peer);
        if let Err(error) = result {
            if let Some(retirement) = self.retiring.get_mut(&(peer.clone(), generation)) {
                retirement.error = Some(error.copy_error());
            }
            self.cleanup_failure = Some(error);
        }
        self.release_retirement(&peer, generation);
    }
    fn retired(&mut self, targets: Vec<(PeerAddress, u64)>, result: Result<()>) {
        for (peer, generation) in targets {
            if let Some(retirement) = self.retiring.get_mut(&(peer.clone(), generation)) {
                retirement.complete = true;
                if let Err(error) = &result {
                    retirement.error = Some(error.copy_error());
                }
            }
            self.release_retirement(&peer, generation);
        }
    }
    fn release_retirement(&mut self, peer: &PeerAddress, generation: u64) {
        if self.acquired.get(peer) == Some(&generation) {
            return;
        }
        if self
            .retiring
            .get(&(peer.clone(), generation))
            .is_some_and(|r| r.complete)
        {
            let retirement = self
                .retiring
                .remove(&(peer.clone(), generation))
                .expect("matched reservation");
            for waiter in retirement.waiters {
                let mut waiter = waiter.lock().unwrap();
                if let Some(error) = &retirement.error {
                    waiter.error = Some(error.copy_error());
                }
                waiter.remaining -= 1;
                if waiter.remaining == 0 {
                    if let Some(reply) = waiter.reply.take() {
                        let result = waiter.error.take().map_or(Ok(()), Err);
                        let _ = reply.send(result);
                    }
                }
            }
            if !self.shutdown.is_cancelled() {
                if let Some(channel) = retirement.inbound {
                    self.inbound(channel);
                }
            }
        }
    }
    fn retire(&mut self, generations: Vec<u64>, reason: AttemptError, reply: Option<Reply<()>>) {
        let mut generations = generations;
        generations.sort_unstable();
        generations.dedup();
        let targets: Vec<_> = generations
            .iter()
            .filter_map(|generation| {
                self.attempts
                    .get(generation)
                    .map(|a| (a.peer.clone(), *generation))
            })
            .collect();
        let mut owners = targets.clone();
        owners.extend(
            self.retiring
                .iter()
                .filter(|(_, r)| generations.contains(&r.generation))
                .map(|((peer, _), r)| (peer.clone(), r.generation)),
        );
        if owners.is_empty() {
            if let Some(reply) = reply {
                let _ = reply.send(Ok(()));
            }
            return;
        }
        // Bound retained replies, not just command queue admission. Cancelled
        // callers free their slots without cancelling generation-owned cleanup.
        for retirement in self.retiring.values_mut() {
            retirement.waiters.retain(|waiter| {
                waiter
                    .lock()
                    .unwrap()
                    .reply
                    .as_ref()
                    .is_some_and(|reply| !reply.is_closed())
            });
        }
        if self
            .retiring
            .values()
            .map(|r| r.waiters.len())
            .sum::<usize>()
            + owners.len()
            > MAX_PENDING_OPERATIONS
        {
            if let Some(reply) = reply {
                let _ = reply.send(Err(BleError::GattBusy(
                    "managed retirement waiter limit reached".into(),
                )));
                return;
            }
        }
        let mut tasks: Vec<_> = targets
            .iter()
            .map(|(_, generation)| Some(self.finish(*generation, reason)))
            .collect();
        if let Some(reply) = reply {
            let waiter = Arc::new(Mutex::new(RetirementReply {
                remaining: owners.len(),
                reply: Some(reply),
                error: None,
            }));
            for (peer, generation) in owners {
                self.retiring
                    .get_mut(&(peer.clone(), generation))
                    .expect("matched retirement owner")
                    .waiters
                    .push(waiter.clone());
            }
        }
        if targets.is_empty() {
            return;
        }
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
            Operation::Retired(targets, result)
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
        if let Some(read) = self.completion_reads.remove(&peer) {
            // The dial wins over a completion read. Its connection is still
            // being released, so this request is refused like one that meets a
            // probe, and a retry gets the address.
            log::debug!("request: {} cancels its completion read", peer.0);
            read.cancel();
        }
        if self.acquired.contains_key(&peer)
            || self.probing.contains_key(&peer)
            || self.address_retiring(&peer)
            || self.attempts.values().any(|attempt| attempt.peer == peer)
        {
            let _ = reply.send(Err(BleError::AdapterBusy(
                "peer already has an attempt or channel".into(),
            )));
            return;
        }
        if !self.operation_capacity() || self.link_count() >= self.config.max_links.0 {
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
            let result = DatagramChannel::connect_managed(driver, &peer, &config)
                .await
                .and_then(|channel| {
                    let endpoint = endpoint
                        .lock()
                        .unwrap()
                        .ok_or_else(|| PeerLink::protocol_error("missing managed endpoint"))?;
                    Ok((channel, endpoint))
                });
            let _ = commands.send(Command::Dialed(generation, result)).await;
        });
        attempt.task = Some(task.abort_handle());
        self.attempts.insert(generation, attempt);
    }
    fn inbound(&mut self, channel: DatagramChannel) {
        let peer = channel.peer();
        if let Some(&generation) = self.acquired.get(&peer) {
            if !self.attempts.values().any(|attempt| attempt.peer == peer) {
                self.retiring
                    .entry((peer.clone(), generation))
                    .or_insert(Retirement {
                        generation,
                        endpoint: None,
                        complete: true,
                        waiters: Vec::new(),
                        error: None,
                        inbound: None,
                    });
            }
        }
        if self.address_retiring(&peer) {
            // The channel is address-scoped, not an additional slot per owner.
            // Transfer it through remaining owners until every blocker is gone.
            if !self
                .retiring
                .iter()
                .any(|((address, _), r)| address == &peer && r.inbound.is_some())
            {
                let (_, retirement) = self
                    .retiring
                    .iter_mut()
                    .find(|((address, _), _)| address == &peer)
                    .expect("address reservation");
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
            || self.address_retiring(&peer)
            || (self.link_count() >= self.config.max_links.0 && !crossed)
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
                                commands.send(Command::Wire(generation, record, Some(reply))).await.map_err(|_| PeerLink::closed_error())?;
                                if let Some(admit) = receiver.await.map_err(|_| PeerLink::closed_error())? { admit.await?; }
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
                                    .await.unwrap_or_else(|_| Err(PeerLink::protocol_error("managed send expired")))
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
                        messages
                            .send(Ok(payload))
                            .await
                            .map_err(|_| PeerLink::closed_error())
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
            (attempt.peer.clone(), generation),
            Retirement {
                generation,
                endpoint: attempt.endpoint,
                complete: false,
                waiters: Vec::new(),
                error: None,
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
                        .map_err(|_| PeerLink::closed_error())?;
                    receiver.await.map_err(|_| PeerLink::closed_error())??;
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
                        .map_err(|_| PeerLink::closed_error())?;
                    receiver.await.map_err(|_| PeerLink::closed_error())?
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
    fn worker() -> Worker {
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _) = mpsc::channel(1);
        Worker::new(
            EndpointId::from_bytes([1; 16]),
            PeerLinkConfig::new(datagram::DatagramConfig::new(
                crate::ServiceUuid(uuid::Uuid::from_u128(11)),
                crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
            )),
            None,
            commands,
            receiver,
            incoming,
            Arc::new(Mutex::new(HashMap::new())),
            CancellationToken::new(),
        )
    }

    #[tokio::test]
    async fn crossed_retirement_preserves_individual_loser_reply_and_aggregate_owners() {
        for cleanup_first in [true, false] {
            for winner_first in [true, false] {
                let mut worker = worker();
                let peer = PeerAddress("crossed-retained".into());
                let endpoint = EndpointId::from_bytes([2; 16]);
                for generation in [1, 2] {
                    let mut attempt =
                        worker.attempt(peer.clone(), generation == 1, CancellationToken::new());
                    attempt.endpoint = Some(endpoint);
                    worker.attempts.insert(generation, attempt);
                }
                worker.acquired.insert(peer.clone(), 1);
                let (reply, mut individual) = oneshot::channel();
                worker.retire(vec![1], AttemptError::Superseded, Some(reply));
                let (reply, winner_reply) = oneshot::channel();
                worker.retire(vec![2], AttemptError::PeerDisconnected, Some(reply));
                assert!(
                    matches!(
                        individual.try_recv(),
                        Err(oneshot::error::TryRecvError::Empty)
                    ),
                    "winner retirement dropped loser's retained reply before its cleanup/transport"
                );
                let (reply, mut aggregate) = oneshot::channel();
                worker.retire(
                    worker.endpoint_generations(endpoint),
                    AttemptError::Disconnected,
                    Some(reply),
                );
                let mut pending = |worker: &Worker| {
                    assert!(matches!(
                        aggregate.try_recv(),
                        Err(oneshot::error::TryRecvError::Empty)
                    ));
                    assert_eq!(
                        worker.endpoint_generations(endpoint).len(),
                        if winner_first { 1 } else { 2 }
                    );
                };
                if winner_first {
                    worker.retired(
                        vec![(peer.clone(), 2)],
                        Err(BleError::Gatt("winner transport".into())),
                    );
                }
                if cleanup_first {
                    worker.cleaned(
                        peer.clone(),
                        1,
                        Err(BleError::Unsupported("loser cleanup".into())),
                    );
                } else {
                    worker.retired(vec![(peer.clone(), 1)], Ok(()));
                }
                assert!(matches!(
                    individual.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                pending(&worker);
                if cleanup_first {
                    worker.retired(vec![(peer.clone(), 1)], Ok(()));
                } else {
                    worker.cleaned(
                        peer.clone(),
                        1,
                        Err(BleError::Unsupported("loser cleanup".into())),
                    );
                }
                assert!(
                    matches!(individual.await.unwrap(), Err(BleError::Unsupported(message)) if message == "loser cleanup")
                );
                if !winner_first {
                    assert!(matches!(
                        aggregate.try_recv(),
                        Err(oneshot::error::TryRecvError::Empty)
                    ));
                    // Stale A notifications cannot remove B, replace its error or decrement twice.
                    worker.retired(vec![(peer.clone(), 1)], Err(BleError::Gatt("stale".into())));
                    worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale".into())));
                    assert_eq!(worker.endpoint_generations(endpoint), vec![2]);
                    worker.retired(
                        vec![(peer.clone(), 2)],
                        Err(BleError::Gatt("winner transport".into())),
                    );
                }
                assert!(
                    matches!(winner_reply.await.unwrap(), Err(BleError::Gatt(message)) if message == "winner transport"),
                    "A's cleanup error cannot overwrite B's transport outcome"
                );
                let aggregate = aggregate.await.unwrap();
                if winner_first {
                    assert!(
                        matches!(aggregate, Err(BleError::Unsupported(message)) if message == "loser cleanup")
                    );
                } else {
                    assert!(
                        matches!(aggregate, Err(BleError::Gatt(message)) if message == "winner transport")
                    );
                }
                assert!(worker.endpoint_generations(endpoint).is_empty());
            }
        }
    }

    #[tokio::test]
    async fn crossed_retirement_counts_cleanup_and_waiter_references_by_owner() {
        let mut worker = worker();
        let peer = PeerAddress("crossed-budget".into());
        worker.acquired.insert(peer.clone(), 1);
        worker.attempts.insert(
            2,
            worker.attempt(peer.clone(), false, CancellationToken::new()),
        );
        assert_eq!(
            worker.link_count(),
            2,
            "B cannot hide acquired A's link budget"
        );
        worker.retire(vec![2], AttemptError::PeerDisconnected, None);
        assert_eq!(
            worker.link_count(),
            2,
            "retiring B cannot hide acquired A either"
        );
        worker.retiring.insert(
            (peer.clone(), 1),
            Retirement {
                generation: 1,
                endpoint: None,
                complete: false,
                waiters: Vec::new(),
                error: None,
                inbound: None,
            },
        );
        assert_eq!(
            worker.link_count(),
            2,
            "matching acquired A is counted once"
        );
        let operations = worker.operations.len();
        let mut receivers = Vec::new();
        for _ in 0..MAX_PENDING_OPERATIONS / 2 {
            let (reply, receiver) = oneshot::channel();
            worker.retire(vec![1, 2], AttemptError::Disconnected, Some(reply));
            receivers.push(receiver);
        }
        assert_eq!(
            worker
                .retiring
                .values()
                .map(|r| r.waiters.len())
                .sum::<usize>(),
            MAX_PENDING_OPERATIONS
        );
        let (reply, receiver) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        assert!(matches!(
            receiver.await.unwrap(),
            Err(BleError::GattBusy(_))
        ));
        drop(receivers.pop());
        let (reply, receiver) = oneshot::channel();
        worker.retire(vec![1, 2], AttemptError::Disconnected, Some(reply));
        receivers.push(receiver);
        assert_eq!(
            worker
                .retiring
                .values()
                .map(|r| r.waiters.len())
                .sum::<usize>(),
            MAX_PENDING_OPERATIONS
        );
        assert_eq!(
            worker.operations.len(),
            operations,
            "joining owners cannot allocate native operations"
        );
        worker.retired(vec![(peer.clone(), 2)], Ok(()));
        worker.cleaned(peer.clone(), 1, Ok(()));
        for receiver in &mut receivers {
            assert!(matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        worker.retired(vec![(peer, 1)], Ok(()));
        for receiver in receivers {
            receiver.await.unwrap().unwrap();
        }
        assert_eq!(worker.link_count(), 0);
        for generation in 1..=MAX_PENDING_OPERATIONS {
            let peer = PeerAddress(format!("budget-{generation}"));
            worker.attempts.insert(
                generation as u64,
                worker.attempt(peer, true, CancellationToken::new()),
            );
        }
        assert!(
            !worker.operation_capacity(),
            "native operation cap remains 64"
        );
    }

    #[tokio::test]
    async fn shutdown_answers_every_crossed_retirement_owner_and_shared_aggregate() {
        for abort in [false, true] {
            let mut worker = worker();
            let peer = PeerAddress("shutdown-crossed".into());
            let endpoint = EndpointId::from_bytes([2; 16]);
            for generation in [1, 2] {
                let mut attempt =
                    worker.attempt(peer.clone(), generation == 1, CancellationToken::new());
                attempt.endpoint = Some(endpoint);
                worker.attempts.insert(generation, attempt);
            }
            worker.acquired.insert(peer, 1);
            let (reply, individual) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Released, Some(reply));
            let (reply, aggregate) = oneshot::channel();
            worker.retire(
                worker.endpoint_generations(endpoint),
                AttemptError::Closed,
                Some(reply),
            );
            worker.operations.spawn(std::future::pending());
            worker.driver = Some(Arc::new(crate::drivers::mock::MockDriver::new(
                PeerAddress("shutdown-worker".into()),
                crate::drivers::mock::MockNetwork::new(),
                crate::CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            )));
            worker.shutdown.cancel();
            if abort {
                worker.operations.abort_all();
            }
            worker.run().await;
            let reason = if abort {
                AttemptError::Cancelled
            } else {
                AttemptError::TimedOut
            };
            for receiver in [individual, aggregate] {
                assert!(
                    matches!(receiver.await, Ok(Err(BleError::Attempt(actual))) if actual == reason)
                );
            }
        }
    }

    #[tokio::test]
    async fn crossed_retirement_transfers_one_held_inbound_until_all_address_owners_finish() {
        use crate::drivers::mock::{MockDriver, MockNetwork};
        for first in [1, 2] {
            let mut worker = worker();
            let peer = PeerAddress("held-crossed-peer".into());
            worker.next_generation = 2;
            worker.acquired.insert(peer.clone(), 1);
            for generation in [1, 2] {
                worker.attempts.insert(
                    generation,
                    worker.attempt(peer.clone(), generation == 1, CancellationToken::new()),
                );
            }
            worker.retire(vec![1], AttemptError::Superseded, None);
            worker.retire(vec![2], AttemptError::PeerDisconnected, None);
            let mut servers = Vec::new();
            // Two independent raw sessions with the same address exercise the
            // address-scoped holding cap without one replacing the other's HAL.
            for _ in 0..2 {
                let network = MockNetwork::new();
                let caps = crate::CapabilityReport {
                    central: true,
                    peripheral: true,
                };
                let local = Arc::new(MockDriver::new(
                    PeerAddress("held-local".into()),
                    network.clone(),
                    caps,
                ));
                let remote = Arc::new(MockDriver::new(peer.clone(), network, caps));
                servers.push(worker.config.datagram.serve_driver(remote).await.unwrap());
                let channel =
                    DatagramChannel::connect_driver(local, &peer, &worker.config.datagram)
                        .await
                        .unwrap();
                worker.inbound(channel);
                assert!(worker.attempts.is_empty());
                assert_eq!(
                    worker
                        .retiring
                        .values()
                        .filter(|r| r.inbound.is_some())
                        .count(),
                    1
                );
            }
            // Force the held channel onto the first completed owner so both
            // removal orders exercise transfer, regardless of HashMap iteration.
            let held = worker
                .retiring
                .values_mut()
                .find_map(|r| r.inbound.take())
                .unwrap();
            worker
                .retiring
                .get_mut(&(peer.clone(), first))
                .unwrap()
                .inbound = Some(held);
            if first == 1 {
                worker.cleaned(peer.clone(), 1, Ok(()));
            }
            worker.retired(vec![(peer.clone(), first)], Ok(()));
            assert!(
                worker.attempts.is_empty(),
                "other retirement must still exclude address admission"
            );
            assert!(worker.address_retiring(&peer));
            assert_eq!(
                worker
                    .retiring
                    .values()
                    .filter(|r| r.inbound.is_some())
                    .count(),
                1
            );
            let last = if first == 1 { 2 } else { 1 };
            worker.retired(vec![(peer.clone(), last)], Ok(()));
            if last == 1 {
                assert!(
                    worker.attempts.is_empty(),
                    "A transport cannot bypass matching acquired cleanup"
                );
                worker.cleaned(peer.clone(), 1, Ok(()));
            }
            assert!(!worker.address_retiring(&peer));
            assert_eq!(worker.attempts.len(), 1);
            assert!(worker.attempts.contains_key(&3));
            let ended = worker.attempts[&3].ended.clone();
            worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale".into())));
            worker.retired(vec![(peer, 2)], Err(BleError::Gatt("stale".into())));
            assert!(
                !ended.is_cancelled(),
                "old completions must not close admitted replacement"
            );
            worker.shutdown.cancel();
            for attempt in worker.attempts.values() {
                if let Some(task) = &attempt.task {
                    task.abort();
                }
            }
        }
    }

    #[tokio::test]
    async fn retirement_waiters_are_bounded_and_cancelled_slots_are_reclaimed() {
        let mut worker = worker();
        let peer = PeerAddress("bounded-waiters".into());
        let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        worker.attempts.insert(1, attempt);
        worker.acquired.insert(peer.clone(), 1);
        worker.retire(vec![1], AttemptError::PeerDisconnected, None);
        let operations = worker.operations.len();
        let mut results = Vec::new();
        for _ in 0..MAX_PENDING_OPERATIONS {
            let (reply, result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
            results.push(result);
        }
        let (reply, result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        assert!(matches!(result.await.unwrap(), Err(BleError::GattBusy(_))));
        drop(results.pop());
        for _ in 0..100 {
            let (reply, result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
            assert_eq!(
                worker.retiring[&(peer.clone(), 1)].waiters.len(),
                MAX_PENDING_OPERATIONS
            );
            drop(result);
        }
        assert_eq!(
            worker.operations.len(),
            operations,
            "joining cannot spawn unbounded waiter tasks"
        );
        worker.retired(vec![(peer.clone(), 1)], Ok(()));
        for result in &mut results {
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        worker.cleaned(peer.clone(), 1, Ok(()));
        for result in results {
            result.await.unwrap().unwrap();
        }
        assert!(!worker.address_retiring(&peer));
    }

    #[tokio::test]
    async fn stale_retirement_and_cleanup_cannot_acknowledge_or_remove_another_owner() {
        let mut worker = worker();
        let peer = PeerAddress("stale-owner".into());
        let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        worker.attempts.insert(2, attempt);
        worker.acquired.insert(peer.clone(), 2);
        worker.retire(vec![2], AttemptError::PeerDisconnected, None);
        let (reply, mut result) = oneshot::channel();
        worker.retire(vec![2], AttemptError::Released, Some(reply));
        let (stale, stale_result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(stale));
        stale_result.await.unwrap().unwrap();
        worker.retired(
            vec![(peer.clone(), 1)],
            Err(BleError::Unsupported("stale failure".into())),
        );
        worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale cleanup".into())));
        assert_eq!(worker.acquired.get(&peer), Some(&2));
        assert!(worker.cleanup_failure.is_none());
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        worker.retired(vec![(peer.clone(), 2)], Ok(()));
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        worker.cleaned(peer, 2, Ok(()));
        result.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_timeout_answers_pending_retirement_with_typed_timeout() {
        for abort in [false, true] {
            let mut worker = worker();
            let peer = PeerAddress("shutdown-retirement".into());
            let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
            worker.attempts.insert(1, attempt);
            worker.acquired.insert(peer, 1);
            let (reply, result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Released, Some(reply));
            worker.operations.spawn(std::future::pending());
            worker.driver = Some(Arc::new(crate::drivers::mock::MockDriver::new(
                PeerAddress("shutdown-worker".into()),
                crate::drivers::mock::MockNetwork::new(),
                crate::CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            )));
            worker.shutdown.cancel();
            if abort {
                worker.operations.abort_all();
            }
            worker.run().await;
            let reason = if abort {
                AttemptError::Cancelled
            } else {
                AttemptError::TimedOut
            };
            assert!(matches!(result.await, Ok(Err(BleError::Attempt(actual))) if actual == reason));
        }
    }

    #[tokio::test]
    async fn endpoint_disconnect_joins_only_its_known_retirement() {
        let mut worker = worker();
        let endpoint = EndpointId::from_bytes([9; 16]);
        let peer = PeerAddress("endpoint-retirement".into());
        let mut attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        attempt.endpoint = Some(endpoint);
        worker.attempts.insert(1, attempt);
        worker.acquired.insert(peer.clone(), 1);
        drop(worker.finish(1, AttemptError::PeerDisconnected));
        let generations = worker.endpoint_generations(endpoint);
        let (reply, mut result) = oneshot::channel();
        worker.retire(generations, AttemptError::Disconnected, Some(reply));
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(worker
            .endpoint_generations(EndpointId::from_bytes([8; 16]))
            .is_empty());
        worker.retired(vec![(peer.clone(), 1)], Ok(()));
        worker.cleaned(peer, 1, Ok(()));
        result.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn retirement_reply_preserves_transport_failure() {
        for cleanup_first in [true, false] {
            let mut worker = worker();
            let peer = PeerAddress("transport-refusal".into());
            let mut attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
            let (transport, mut commands) = mpsc::channel(4);
            attempt.transport = Some(transport);
            worker.attempts.insert(1, attempt);
            worker.acquired.insert(peer.clone(), 1);
            let (reply, mut result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
            let handler = tokio::spawn(async move {
                while let Some(command) = commands.recv().await {
                    match command {
                        TransportCommand::Send(_, Some(reply)) => {
                            let _ = reply.send(Ok(()));
                        }
                        TransportCommand::Stop { reply, .. } => {
                            let _ =
                                reply.send(Err(BleError::Unsupported("transport refusal".into())));
                            break;
                        }
                        _ => panic!("unexpected command"),
                    }
                }
            });
            if cleanup_first {
                worker.cleaned(peer.clone(), 1, Ok(()));
                assert!(matches!(
                    result.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
            }
            match worker.operations.join_next().await.unwrap().unwrap() {
                Operation::Retired(targets, result) => worker.retired(targets, result),
                _ => panic!("expected retirement"),
            }
            if !cleanup_first {
                assert!(matches!(
                    result.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                worker.cleaned(peer, 1, Ok(()));
            }
            assert!(
                matches!(result.await.unwrap(), Err(BleError::Unsupported(message)) if message == "transport refusal")
            );
            handler.await.unwrap();
        }
    }

    #[tokio::test]
    async fn retirement_reply_preserves_cleanup_error_in_both_completion_orders() {
        for cleanup_first in [true, false] {
            let mut worker = worker();
            let peer = PeerAddress("typed-cleanup".into());
            let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
            worker.attempts.insert(1, attempt);
            worker.acquired.insert(peer.clone(), 1);
            let (reply, mut result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
            if cleanup_first {
                worker.cleaned(
                    peer.clone(),
                    1,
                    Err(BleError::Unsupported("cleanup refusal".into())),
                );
            } else {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            }
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            if cleanup_first {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            } else {
                worker.cleaned(
                    peer.clone(),
                    1,
                    Err(BleError::Unsupported("cleanup refusal".into())),
                );
            }
            assert!(
                matches!(result.await.unwrap(), Err(BleError::Unsupported(message)) if message == "cleanup refusal")
            );
            assert!(matches!(
                worker.cleanup_failure,
                Some(BleError::Unsupported(_))
            ));
        }
    }
    #[tokio::test]
    async fn inbound_waits_for_the_matching_acquired_cleanup_completion() {
        use crate::drivers::mock::{MockDriver, MockNetwork};
        let network = MockNetwork::new();
        let make = |name: &str| {
            Arc::new(MockDriver::new(
                PeerAddress(name.into()),
                network.clone(),
                crate::CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            ))
        };
        let a = make("cleanup-a");
        let b = make("cleanup-b");
        let config = PeerLinkConfig::new(datagram::DatagramConfig::new(
            crate::ServiceUuid(uuid::Uuid::from_u128(11)),
            crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
        ));
        let _served = config.datagram.serve_driver(b).await.unwrap();
        let peer = PeerAddress("cleanup-b".into());
        let channel = DatagramChannel::connect_driver(a.clone(), &peer, &config.datagram)
            .await
            .unwrap();
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _requests) = mpsc::channel(1);
        let mut worker = Worker::new(
            EndpointId::from_bytes([1; 16]),
            config,
            Some(a),
            commands,
            receiver,
            incoming,
            Arc::new(Mutex::new(HashMap::new())),
            CancellationToken::new(),
        );
        let cleanup = worker.reserve_cleanup(peer.clone(), 1);
        worker.inbound(channel);
        assert!(
            worker.attempts.is_empty(),
            "an old acquired cleanup still owns this address"
        );
        worker.cleaned(peer.clone(), 0, Ok(()));
        assert_eq!(
            worker.acquired.get(&peer),
            Some(&1),
            "stale completion must not release another generation"
        );
        assert!(
            worker.attempts.is_empty(),
            "stale completion must not admit the held channel"
        );
        drop(cleanup);
        match worker.operations.join_next().await.unwrap().unwrap() {
            Operation::Cleaned(peer, generation, result) => {
                worker.cleaned(peer, generation, result)
            }
            _ => panic!("expected acquired cleanup completion"),
        }
        assert_eq!(
            worker.attempts.len(),
            1,
            "the held channel must be admitted only after matching completion"
        );
        worker.shutdown.cancel();
        for attempt in worker.attempts.values() {
            if let Some(task) = &attempt.task {
                task.abort();
            }
        }
    }

    #[tokio::test]
    async fn retirement_and_acquired_cleanup_must_both_complete_in_either_order() {
        for cleanup_first in [true, false] {
            let (commands, receiver) = mpsc::channel(4);
            let (incoming, _requests) = mpsc::channel(1);
            let config = PeerLinkConfig::new(datagram::DatagramConfig::new(
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
            let peer = PeerAddress("both-completions".into());
            worker.acquired.insert(peer.clone(), 1);
            worker.retiring.insert(
                (peer.clone(), 1),
                Retirement {
                    generation: 1,
                    endpoint: None,
                    complete: false,
                    waiters: Vec::new(),
                    error: None,
                    inbound: None,
                },
            );
            let (reply, mut result) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Released, Some(reply));
            assert!(
                matches!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "already-retiring last release must join its generation"
            );
            if cleanup_first {
                worker.cleaned(peer.clone(), 1, Ok(()));
            } else {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            }
            assert!(
                matches!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "one completion cannot acknowledge release"
            );
            assert!(
                worker.address_retiring(&peer),
                "the other owner still holds the reservation"
            );
            if cleanup_first {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            } else {
                worker.cleaned(peer.clone(), 1, Ok(()));
            }
            assert!(!worker.address_retiring(&peer));
            assert!(!worker.acquired.contains_key(&peer));
            assert!(result.try_recv().unwrap().is_ok());
        }
    }

    #[tokio::test]
    async fn inbound_admission_counts_an_unrelated_acquired_cleanup_reservation() {
        use crate::drivers::mock::{MockDriver, MockNetwork};
        let network = MockNetwork::new();
        let make = |name: &str| {
            Arc::new(MockDriver::new(
                PeerAddress(name.into()),
                network.clone(),
                crate::CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            ))
        };
        let a = make("admission-a");
        let b = make("admission-b");
        let mut config = PeerLinkConfig::new(datagram::DatagramConfig::new(
            crate::ServiceUuid(uuid::Uuid::from_u128(11)),
            crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
        ));
        config.max_links = MaxLinks(1);
        let _served = config.datagram.serve_driver(b).await.unwrap();
        let channel = DatagramChannel::connect_driver(
            a.clone(),
            &PeerAddress("admission-b".into()),
            &config.datagram,
        )
        .await
        .unwrap();
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _requests) = mpsc::channel(1);
        let mut worker = Worker::new(
            EndpointId::from_bytes([1; 16]),
            config,
            Some(a),
            commands,
            receiver,
            incoming,
            Arc::new(Mutex::new(HashMap::new())),
            CancellationToken::new(),
        );
        let cleanup = worker.reserve_cleanup(PeerAddress("unrelated-cleanup".into()), 1);
        worker.inbound(channel);
        let admitted = worker.attempts.len();
        // Clean up even on the RED path, without leaving an extra pump running.
        for attempt in worker.attempts.values() {
            if let Some(task) = &attempt.task {
                task.abort();
            }
        }
        drop(cleanup);
        worker.shutdown.cancel();
        assert_eq!(
            admitted, 0,
            "MaxLinks(1) is still held by acquired cleanup, not only active attempts"
        );
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
