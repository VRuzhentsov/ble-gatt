//! Tier 3: `PeerLink` — keep app-to-app links to a set of peers alive.
//!
//! **Read `docs/interface.md` for when to use this instead of the raw
//! `Backend` or the `datagram` tier, and `docs/adr/0005` for the state
//! machines underneath.**
//!
//! The consumer declares which peers it cares about (`track`) and one policy
//! (`role` — who dials). This module owns dialing, the redial ladder and
//! giving up, one link per peer, and tearing everything down on radio-off /
//! re-establishing on radio-on. The consumer consumes a channel and a
//! per-peer status; it never sees the state machines.
//!
//! Everything BLE happens on one dedicated OS thread with its own Tokio
//! runtime (spawned by `new`), so `new` is synchronous, infallible, and
//! works in a binary with no async runtime of its own. The [`PeerChannel`]
//! handed out on [`PeerLinkEvent::Up`] is a message-passing handle to that
//! thread.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::backend::link_state::{CentralEvent, CentralLink, RadioEvent, RadioState};
use crate::backend::{Backend, BoxStream};
use crate::datagram::{self, DatagramChannel, DatagramConfig};
use crate::error::{BleError, Result};
use crate::models::{GattEvent, PeerAddress, RadioStatus, Role};

const EVENT_CHANNEL_CAPACITY: usize = 128;
const TICK_INTERVAL: Duration = Duration::from_secs(1);
const CHANNEL_SEND_QUEUE: usize = 16;
const CHANNEL_RECV_QUEUE: usize = 64;
const DISCOVER_WINDOW: Duration = Duration::from_secs(3);

/// A stable identity the two peers of a pair compare the same way, to decide
/// which side dials (glare — see `docs/adr/0003`'s revision). This is the
/// *application's* to establish (node ids exchanged during its own
/// handshake); `PeerAddress` is opaque and unordered and cannot be the
/// basis. Only consulted in [`LinkRole::Symmetric`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinkId(pub String);

impl From<String> for LinkId {
    fn from(s: String) -> Self {
        LinkId(s)
    }
}
impl From<&str> for LinkId {
    fn from(s: &str) -> Self {
        LinkId(s.to_owned())
    }
}

/// Who dials, for a pair that can both see each other.
#[derive(Debug, Clone)]
pub enum LinkRole {
    /// Dials tracked peers *and* accepts inbound. For each peer, whichever
    /// side has the lower `LinkId` dials.
    Symmetric { local_id: LinkId },
    /// Only ever dials tracked peers.
    DialOnly,
    /// Only ever advertises and accepts; never dials.
    AcceptOnly,
}

/// Redial ladder for the dialer, and inbound-link deadline for the acceptor.
/// Both sides reach [`PeerStatus::GaveUp`] when the budget is spent.
#[derive(Debug, Clone)]
pub struct RetryBudget {
    /// Backoff before each successive redial. The last entry repeats.
    /// Empty = do not redial (an inbound link is still accepted).
    pub backoff: Vec<Duration>,
    /// Total time from the first failed attempt before the dialer gives up.
    pub give_up_after: Duration,
    /// How long the acceptor waits for an inbound link before giving up.
    pub acceptor_deadline: Duration,
}

impl Default for RetryBudget {
    fn default() -> Self {
        RetryBudget {
            backoff: vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(30),
            ],
            give_up_after: Duration::from_secs(60),
            acceptor_deadline: Duration::from_secs(60),
        }
    }
}

/// How many peers hold a live link at once. A BLE central has a hard
/// concurrent-link limit; tracking more peers than this queues the extra
/// ones as [`PeerStatus::Queued`].
#[derive(Debug, Clone, Copy)]
pub struct MaxLinks(pub usize);

impl Default for MaxLinks {
    fn default() -> Self {
        MaxLinks(4)
    }
}

/// Configure a [`PeerLink`]. See `docs/interface.md`.
pub struct PeerLinkConfig {
    /// The wire contract and datagram bounds. `datagram.service` /
    /// `datagram.characteristic` are the service and characteristic to use.
    pub datagram: DatagramConfig,
    pub role: LinkRole,
    pub retry_budget: RetryBudget,
    pub max_links: MaxLinks,
}

/// One tracked peer's coarse status — a projection of the internal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    Untracked,
    /// Radio off / unsupported.
    Unavailable,
    /// Tracked, but `max_links` is full — waiting for a slot.
    Queued,
    /// Trying — dialing, or waiting for an inbound link.
    Connecting,
    Connected,
    /// A previous attempt failed; backing off before the next.
    Waiting,
    /// The retry budget is spent. Left by `retry()` or an inbound link.
    GaveUp,
}

/// What the consumer reacts to. Delivered on [`PeerLink::events`], a
/// broadcast — each call is an independent subscription.
#[derive(Debug, Clone)]
pub enum PeerLinkEvent {
    /// A link to `peer` is up; the channel is valid until `Down` for this
    /// peer. A redial after a drop delivers a fresh `Up`.
    Up {
        peer: PeerAddress,
        max_message_len: usize,
        channel: Arc<PeerChannel>,
    },
    /// The link is gone; the channel from `Up` is dead.
    Down { peer: PeerAddress },
    /// Coarse status changed. No channel.
    Status { peer: PeerAddress, status: PeerStatus },
    /// The radio's usability changed.
    RadioChanged { status: RadioStatus },
}

/// Message-passing handle to one peer's live link. `send` / `recv` cross to
/// the driver thread, which does the GATT work. Contract (see
/// `docs/interface.md`): `send` returns the real `BleError`
/// (`GattBusy` stays distinguishable); `recv` → `None` on close, including
/// the driver being gone; a full send queue → `BleError::GattBusy`.
pub struct PeerChannel {
    peer: PeerAddress,
    max_message_len: usize,
    send_tx: mpsc::Sender<(Vec<u8>, oneshot::Sender<Result<()>>)>,
    recv_rx: AsyncMutex<mpsc::Receiver<Result<Vec<u8>>>>,
}

impl std::fmt::Debug for PeerChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerChannel")
            .field("peer", &self.peer)
            .field("max_message_len", &self.max_message_len)
            .finish_non_exhaustive()
    }
}

impl PeerChannel {
    pub fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    pub fn max_message_len(&self) -> usize {
        self.max_message_len
    }

    pub async fn send(&self, payload: Vec<u8>) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_tx
            .try_send((payload, reply_tx))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    BleError::GattBusy(format!("send queue full for {}", self.peer.0))
                }
                mpsc::error::TrySendError::Closed(_) => BleError::NotConnected(self.peer.0.clone()),
            })?;
        reply_rx
            .await
            .map_err(|_| BleError::NotConnected(self.peer.0.clone()))?
    }

    pub async fn recv(&self) -> Option<Result<Vec<u8>>> {
        self.recv_rx.lock().await.recv().await
    }
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

enum Command {
    Track { peer: PeerAddress, peer_id: LinkId },
    Untrack(PeerAddress),
    Retry(PeerAddress),
    Discover(oneshot::Sender<Result<Vec<PeerAddress>>>),
}

/// See the module docs and `docs/interface.md`.
pub struct PeerLink {
    cmd_tx: mpsc::UnboundedSender<Command>,
    events_tx: broadcast::Sender<PeerLinkEvent>,
    status: Arc<StdMutex<HashMap<PeerAddress, PeerStatus>>>,
    radio: Arc<StdMutex<RadioStatus>>,
    _driver: DriverHandle,
}

struct DriverHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for DriverHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl PeerLink {
    /// Build a `PeerLink` that constructs and owns the platform backend.
    /// Synchronous and infallible: the backend is acquired on the driver
    /// thread and surfaces as `radio()` reporting `Off` / `Unsupported`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new(config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(config, None)
    }

    /// Build a `PeerLink` over an already-constructed backend — for tests and
    /// the mock.
    pub fn with_backend(backend: Arc<dyn Backend>, config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(config, Some(backend))
    }

    fn spawn(config: PeerLinkConfig, backend: Option<Arc<dyn Backend>>) -> Arc<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let status = Arc::new(StdMutex::new(HashMap::new()));
        let radio = Arc::new(StdMutex::new(RadioStatus::Off));

        let driver = Driver {
            config,
            provided_backend: backend,
            cmd_rx,
            events_tx: events_tx.clone(),
            status: status.clone(),
            radio: radio.clone(),
        };
        let join = std::thread::Builder::new()
            .name("ble-gatt-peerlink".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("PeerLink driver runtime");
                rt.block_on(driver.run(shutdown_rx));
            })
            .expect("spawn PeerLink driver thread");

        Arc::new(PeerLink {
            cmd_tx,
            events_tx,
            status,
            radio,
            _driver: DriverHandle { shutdown: Some(shutdown_tx), join: Some(join) },
        })
    }

    /// Keep a link to this peer alive. `peer_id` is only consulted for
    /// [`LinkRole::Symmetric`].
    pub fn track(&self, peer: PeerAddress, peer_id: LinkId) {
        let _ = self.cmd_tx.send(Command::Track { peer, peer_id });
    }

    /// Stop: drop the channel, disconnect, forget the peer.
    pub fn untrack(&self, peer: PeerAddress) {
        let _ = self.cmd_tx.send(Command::Untrack(peer));
    }

    /// A tracked peer that gave up: try again now.
    pub fn retry(&self, peer: PeerAddress) {
        let _ = self.cmd_tx.send(Command::Retry(peer));
    }

    /// Peers advertising the configured service — a bounded snapshot.
    /// Untrusted metadata; the consumer decides which to `track`.
    pub async fn discover(&self) -> Result<Vec<PeerAddress>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Discover(tx))
            .map_err(|_| BleError::AdapterUnavailable("PeerLink driver is gone".into()))?;
        rx.await
            .map_err(|_| BleError::AdapterUnavailable("PeerLink driver is gone".into()))?
    }

    /// Subscribe to the event stream. Each call is an independent
    /// subscription.
    pub fn events(&self) -> BoxStream<PeerLinkEvent> {
        Box::pin(BroadcastStream::new(self.events_tx.subscribe()).filter_map(|item| item.ok()))
    }

    /// One peer's status without waiting for an event.
    pub fn status(&self, peer: &PeerAddress) -> PeerStatus {
        self.status
            .lock()
            .unwrap()
            .get(peer)
            .copied()
            .unwrap_or(PeerStatus::Untracked)
    }

    /// Radio usability without waiting for an event.
    pub fn radio(&self) -> RadioStatus {
        *self.radio.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// The driver — runs on the dedicated thread's runtime.
// ---------------------------------------------------------------------------

struct Driver {
    config: PeerLinkConfig,
    provided_backend: Option<Arc<dyn Backend>>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    events_tx: broadcast::Sender<PeerLinkEvent>,
    status: Arc<StdMutex<HashMap<PeerAddress, PeerStatus>>>,
    radio: Arc<StdMutex<RadioStatus>>,
}

/// Per-tracked-peer state the driver owns.
struct Peer {
    central: CentralLink,
    /// `true` when this side is the designated dialer for the pair.
    dials: bool,
    /// When the current run of attempts started, for the give-up clocks.
    trying_since: Option<Instant>,
    attempts: usize,
    /// Set while backing off: no dial before this instant.
    retry_at: Option<Instant>,
    gave_up: bool,
    link: Option<LiveLink>,
}

struct LiveLink {
    pump: tokio::task::AbortHandle,
    /// Held so the pump task's `teardown` receiver stays open; dropping this
    /// (on `tear_down`) tells the pump to drop the `DatagramChannel`.
    _teardown: oneshot::Sender<()>,
}

impl Driver {
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        let backend = match self.acquire_backend().await {
            Some(b) => b,
            None => {
                self.set_radio(RadioStatus::Unsupported);
                let _ = self
                    .events_tx
                    .send(PeerLinkEvent::RadioChanged { status: RadioStatus::Unsupported });
                self.idle_until_shutdown(shutdown).await;
                return;
            }
        };
        self.set_radio(RadioStatus::On);
        let _ = self
            .events_tx
            .send(PeerLinkEvent::RadioChanged { status: RadioStatus::On });

        let mut radio = RadioState::On;
        let mut peers: HashMap<PeerAddress, Peer> = HashMap::new();
        let mut events = backend.events();
        let (conn_tx, mut conn_rx) =
            mpsc::unbounded_channel::<(PeerAddress, Result<DatagramChannel>)>();
        let (linkgone_tx, mut linkgone_rx) = mpsc::unbounded_channel::<PeerAddress>();
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = &mut shutdown => break,

                cmd = self.cmd_rx.recv() => match cmd {
                    None => break,
                    Some(Command::Track { peer, peer_id }) => {
                        self.on_track(&mut peers, peer, peer_id, radio);
                    }
                    Some(Command::Untrack(peer)) => {
                        if let Some(mut p) = peers.remove(&peer) {
                            let had_link = p.link.is_some();
                            self.tear_down(&mut p);
                            if had_link {
                                let _ = self.events_tx.send(PeerLinkEvent::Down { peer: peer.clone() });
                            }
                        }
                        self.publish_status(&peer, PeerStatus::Untracked);
                    }
                    Some(Command::Retry(peer)) => {
                        if let Some(p) = peers.get_mut(&peer) {
                            p.gave_up = false;
                            p.attempts = 0;
                            p.retry_at = None;
                            p.trying_since = Some(Instant::now());
                        }
                    }
                    Some(Command::Discover(reply)) => {
                        let _ = reply.send(self.discover_now(&backend).await);
                    }
                },

                Some(ev) = events.next() => {
                    self.on_backend_event(&mut peers, &mut radio, ev);
                }

                Some((peer, result)) = conn_rx.recv() => {
                    self.on_dial_result(&mut peers, peer, result, &linkgone_tx);
                }

                Some(peer) = linkgone_rx.recv() => {
                    self.on_link_gone(&mut peers, peer);
                }

                _ = tick.tick() => {
                    self.on_tick(&mut peers, radio);
                }
            }
            self.pump_dials(&backend, &mut peers, radio, &conn_tx);
            self.refresh_all_status(&peers, radio);
        }

        for (_, mut p) in peers.drain() {
            self.tear_down(&mut p);
        }
    }

    async fn acquire_backend(&mut self) -> Option<Arc<dyn Backend>> {
        if let Some(b) = self.provided_backend.take() {
            return Some(b);
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            crate::backend::platform().await.ok()
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            None
        }
    }

    async fn idle_until_shutdown(&mut self, mut shutdown: oneshot::Receiver<()>) {
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                cmd = self.cmd_rx.recv() => match cmd {
                    None => return,
                    Some(Command::Discover(reply)) => {
                        let _ = reply.send(Err(BleError::AdapterUnavailable("no BLE adapter".into())));
                    }
                    Some(Command::Track { peer, .. }) => {
                        self.publish_status(&peer, PeerStatus::Unavailable);
                    }
                    Some(_) => {}
                },
            }
        }
    }

    async fn discover_now(&self, backend: &Arc<dyn Backend>) -> Result<Vec<PeerAddress>> {
        let mut stream = backend.scan(self.config.datagram.service).await?;
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + DISCOVER_WINDOW;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                item = stream.next() => match item {
                    Some(Ok(peer)) => out.push(peer.address),
                    Some(Err(err)) => return Err(err),
                    None => break,
                },
            }
        }
        Ok(out)
    }

    fn on_track(
        &self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress, peer_id: LinkId,
        radio: RadioState,
    ) {
        let dials = match &self.config.role {
            LinkRole::DialOnly => true,
            LinkRole::AcceptOnly => false,
            LinkRole::Symmetric { local_id } => *local_id < peer_id,
        };
        peers.entry(peer.clone()).or_insert_with(|| Peer {
            central: CentralLink::new(),
            dials,
            trying_since: Some(Instant::now()),
            attempts: 0,
            retry_at: None,
            gave_up: false,
            link: None,
        });
        self.refresh_status(peers, &peer, radio);
    }

    fn pump_dials(
        &self, backend: &Arc<dyn Backend>, peers: &mut HashMap<PeerAddress, Peer>,
        radio: RadioState,
        conn_tx: &mpsc::UnboundedSender<(PeerAddress, Result<DatagramChannel>)>,
    ) {
        if !radio.usable() {
            return;
        }
        let now = Instant::now();
        let live = peers.values().filter(|p| p.link.is_some()).count();
        let mut budget = self.config.max_links.0.saturating_sub(live);

        let mut candidates: Vec<PeerAddress> = peers.keys().cloned().collect();
        candidates.sort_by(|a, b| a.0.cmp(&b.0));

        let mut next_session: u64 = now.elapsed().as_nanos() as u64;
        for peer in candidates {
            let p = peers.get_mut(&peer).unwrap();
            if p.link.is_some() || p.gave_up || !p.dials || !p.central.may_dial() {
                continue;
            }
            if p.retry_at.is_some_and(|at| now < at) {
                continue;
            }
            if budget == 0 {
                continue; // status projected as Queued by `refresh_status`
            }
            budget -= 1;
            next_session = next_session.wrapping_add(1);
            let (next, _) = p.central.apply(CentralEvent::DialStarted { session: next_session }, now);
            p.central = next;
            p.attempts += 1;

            let backend = backend.clone();
            let cfg = self.config.datagram.clone();
            let conn_tx = conn_tx.clone();
            let peer2 = peer.clone();
            tokio::spawn(async move {
                let result = datagram::connect(backend, &peer2, &cfg).await;
                let _ = conn_tx.send((peer2, result));
            });
        }
    }

    fn on_dial_result(
        &self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress,
        result: Result<DatagramChannel>, linkgone_tx: &mpsc::UnboundedSender<PeerAddress>,
    ) {
        let Some(p) = peers.get_mut(&peer) else {
            return;
        };
        let now = Instant::now();
        match result {
            Ok(channel) => {
                let (next, _) = p.central.apply(CentralEvent::DialSucceeded, now);
                p.central = next;
                p.attempts = 0;
                p.trying_since = None;
                p.retry_at = None;
                let max_len = channel.max_message_len();
                let (link, handle) = self.start_pump(channel, peer.clone(), linkgone_tx.clone());
                p.link = Some(link);
                let _ = self.events_tx.send(PeerLinkEvent::Up {
                    peer,
                    max_message_len: max_len,
                    channel: handle,
                });
            }
            Err(_err) => {
                let (next, _) = p.central.apply(CentralEvent::DialFailed, now);
                p.central = next;
                self.apply_backoff(p, now);
            }
        }
    }

    fn on_link_gone(&self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress) {
        if let Some(p) = peers.get_mut(&peer) {
            if p.link.take().is_some() {
                let now = Instant::now();
                let (next, _) = p.central.apply(CentralEvent::LinkDropped, now);
                p.central = next;
                p.trying_since = Some(now);
                p.attempts = 0;
                p.retry_at = None;
                let _ = self.events_tx.send(PeerLinkEvent::Down { peer });
            }
        }
    }

    fn apply_backoff(&self, p: &mut Peer, now: Instant) {
        let since = *p.trying_since.get_or_insert(now);
        if now.duration_since(since) >= self.config.retry_budget.give_up_after {
            p.gave_up = true;
            p.retry_at = None;
            return;
        }
        let backoff = &self.config.retry_budget.backoff;
        if backoff.is_empty() {
            p.gave_up = true;
            return;
        }
        let idx = p.attempts.saturating_sub(1).min(backoff.len() - 1);
        p.retry_at = Some(now + backoff[idx]);
    }

    fn start_pump(
        &self, mut channel: DatagramChannel, peer: PeerAddress,
        linkgone_tx: mpsc::UnboundedSender<PeerAddress>,
    ) -> (LiveLink, Arc<PeerChannel>) {
        let max_message_len = channel.max_message_len();
        let (send_tx, mut send_rx) =
            mpsc::channel::<(Vec<u8>, oneshot::Sender<Result<()>>)>(CHANNEL_SEND_QUEUE);
        let (recv_tx, recv_rx) = mpsc::channel::<Result<Vec<u8>>>(CHANNEL_RECV_QUEUE);
        let (teardown_tx, mut teardown_rx) = oneshot::channel::<()>();

        let peer2 = peer.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut teardown_rx => break,
                    req = send_rx.recv() => match req {
                        None => break,
                        Some((bytes, reply)) => {
                            let _ = reply.send(channel.send(bytes).await);
                        }
                    },
                    item = channel.recv() => match item {
                        Some(msg) => {
                            if recv_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            drop(channel); // Drop disconnects the underlying link.
            let _ = linkgone_tx.send(peer2);
        });

        let handle = Arc::new(PeerChannel {
            peer,
            max_message_len,
            send_tx,
            recv_rx: AsyncMutex::new(recv_rx),
        });
        (LiveLink { pump: task.abort_handle(), _teardown: teardown_tx }, handle)
    }

    fn on_backend_event(
        &self, peers: &mut HashMap<PeerAddress, Peer>, radio: &mut RadioState, ev: GattEvent,
    ) {
        match ev {
            GattEvent::RadioChanged { status } => {
                let event = match status {
                    RadioStatus::On => RadioEvent::PoweredOn,
                    RadioStatus::Off => RadioEvent::PoweredOff,
                    RadioStatus::Unsupported => RadioEvent::NoAdapter,
                };
                let (next, effects) = radio.apply(event, Instant::now());
                *radio = next;
                self.set_radio(status);
                let _ = self.events_tx.send(PeerLinkEvent::RadioChanged { status });

                if !effects.is_empty() {
                    let now = Instant::now();
                    for (peer, p) in peers.iter_mut() {
                        let (c, _) = p.central.apply(CentralEvent::RadioLost, now);
                        p.central = c;
                        if p.link.take().is_some() {
                            let _ = self.events_tx.send(PeerLinkEvent::Down { peer: peer.clone() });
                        }
                        p.gave_up = false;
                        p.attempts = 0;
                        p.retry_at = None;
                        p.trying_since = Some(now);
                    }
                }
                if radio.usable() {
                    let now = Instant::now();
                    for p in peers.values_mut() {
                        let (c, _) = p.central.apply(CentralEvent::RadioBack, now);
                        p.central = c;
                    }
                }
            }
            GattEvent::Disconnected { peer, local_role: Role::Central, .. } => {
                if let Some(p) = peers.get_mut(&peer) {
                    if p.link.is_none() {
                        let now = Instant::now();
                        let (c, _) = p.central.apply(CentralEvent::LinkDropped, now);
                        p.central = c;
                    }
                }
            }
            _ => {}
        }
    }

    fn on_tick(&self, peers: &mut HashMap<PeerAddress, Peer>, _radio: RadioState) {
        let now = Instant::now();
        for p in peers.values_mut() {
            let (c, _) = p.central.apply(CentralEvent::Tick, now);
            p.central = c;

            if p.link.is_some() || p.gave_up {
                continue;
            }
            let deadline = if p.dials {
                self.config.retry_budget.give_up_after
            } else {
                self.config.retry_budget.acceptor_deadline
            };
            if let Some(since) = p.trying_since {
                if now.duration_since(since) >= deadline {
                    p.gave_up = true;
                    p.retry_at = None;
                }
            }
        }
    }

    fn tear_down(&self, p: &mut Peer) {
        if let Some(link) = p.link.take() {
            link.pump.abort();
            drop(link);
        }
        let now = Instant::now();
        // Force the machine back to a clean stop without touching the
        // platform beyond what the dropped channel already did.
        let (c, _) = p.central.apply(CentralEvent::RadioLost, now);
        p.central = c;
        let (c, _) = p.central.apply(CentralEvent::RadioBack, now);
        p.central = c;
    }

    // ---- status projection ------------------------------------------------

    fn refresh_all_status(&self, peers: &HashMap<PeerAddress, Peer>, radio: RadioState) {
        let live = peers.values().filter(|p| p.link.is_some()).count();
        let over_cap = live >= self.config.max_links.0;
        for (peer, p) in peers {
            self.publish_status(peer, project(p, radio, over_cap));
        }
    }

    fn refresh_status(
        &self, peers: &HashMap<PeerAddress, Peer>, peer: &PeerAddress, radio: RadioState,
    ) {
        if let Some(p) = peers.get(peer) {
            let live = peers.values().filter(|p| p.link.is_some()).count();
            let over_cap = live >= self.config.max_links.0;
            self.publish_status(peer, project(p, radio, over_cap));
        }
    }

    fn publish_status(&self, peer: &PeerAddress, status: PeerStatus) {
        let changed = {
            let mut map = self.status.lock().unwrap();
            match status {
                PeerStatus::Untracked => map.remove(peer).is_some(),
                _ => map.insert(peer.clone(), status) != Some(status),
            }
        };
        if changed {
            let _ = self
                .events_tx
                .send(PeerLinkEvent::Status { peer: peer.clone(), status });
        }
    }

    fn set_radio(&self, status: RadioStatus) {
        *self.radio.lock().unwrap() = status;
    }
}

fn project(p: &Peer, radio: RadioState, over_cap: bool) -> PeerStatus {
    if !radio.usable() {
        return PeerStatus::Unavailable;
    }
    if p.link.is_some() {
        return PeerStatus::Connected;
    }
    if p.gave_up {
        return PeerStatus::GaveUp;
    }
    if p.retry_at.is_some() {
        return PeerStatus::Waiting;
    }
    if over_cap && p.dials {
        return PeerStatus::Queued;
    }
    PeerStatus::Connecting
}
