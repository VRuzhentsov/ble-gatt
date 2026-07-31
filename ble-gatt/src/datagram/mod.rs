//! The datagram tier: an ordered, opaque-bytes channel to a peer.
//!
//! Raw GATT (`crate::backend`) is characteristic-oriented and caps every
//! write at the negotiated MTU. This module turns that into a message pipe —
//! hand it a `Vec<u8>` of any size, get exactly that `Vec<u8>` out the other
//! end — by fragmenting against the live connection's `max_write_len()` and
//! reassembling under strict bounds.
//!
//! **This tier carries bytes and does not interpret them.** Payloads may be
//! ciphertext, and nothing here inspects content, assumes UTF-8, or imposes
//! framing of its own. Layering encryption on top is the expected use; see
//! `docs/adr/0003`. Consumers that instead need to speak a *third party's*
//! GATT protocol (vendor sensor firmware with its own framing) should use
//! `crate::backend` directly and ignore this module entirely.
//!
//! ## Wire shape
//!
//! One service, one characteristic, both directions — the topology Bitchat
//! uses and the one `GattCharacteristicSpec` already produces:
//!
//! - central → peripheral: characteristic write
//! - peripheral → central: notify on that same characteristic
//!
//! ## Known limitation: the peripheral role serves one central at a time
//!
//! `Backend::notify` pushes to *every* subscribed central — the only
//! per-characteristic primitive both platform backends expose today. With a
//! single connected central this is exactly point-to-point, which is what
//! both current consumers need.
//!
//! With several it would be actively unsafe rather than merely wasteful:
//! each central receives the others' fragments, and since every channel
//! starts its `msg_id` at 0, the first message on two channels shares both
//! `msg_id` and usually `total`. The `InconsistentTotal` guard only fires
//! when `total` differs, so those fragments interleave in a third party's
//! reassembler and complete into a blend of two messages — delivered as
//! valid, with no error.
//!
//! `serve` therefore refuses additional centrals while one is active.
//! Lifting that needs a per-peer notify on the `Backend` port:
//! straightforward on Android (`notifyCharacteristicChanged` already takes a
//! device), unresolved on BlueZ where `bluer`'s notifier does not identify
//! its subscriber.

pub mod fragment;
pub mod reassembly;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::datagram::fragment::{split, FragmentHeader, FRAGMENT_HEADER_LEN, MAX_FRAGMENTS};
use crate::datagram::reassembly::{Accept, Reassembler, ReassemblyLimits};
use crate::error::{BleError, Result};
use crate::models::{
    CharacteristicUuid, GattCharacteristicSpec, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

pub const DEFAULT_MAX_MESSAGE_LEN: usize = 1024 * 1024;
pub const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CONCURRENT_REASSEMBLIES: usize = 4;
/// Completed messages buffered before the reassembler applies backpressure.
/// Bounded deliberately: an unbounded queue lets a peer sending valid
/// messages faster than the consumer reads them grow memory without limit.
pub const DEFAULT_INBOUND_QUEUE_DEPTH: usize = 32;
/// Raw fragments buffered per peer. Sized to comfortably hold one
/// maximum-fragment-count message in flight without being a memory lever.
pub const DEFAULT_FRAGMENT_QUEUE_DEPTH: usize = 256;

/// Default [`DatagramConfig::accept_queue_depth`]. Small on purpose: the
/// single-central rule means at most one channel is usefully live at a time,
/// so anything beyond a short burst allowance is a caller that has stopped
/// accepting.
pub const DEFAULT_ACCEPT_QUEUE_DEPTH: usize = 8;

/// How long `serve` will block on a full fragment queue before dropping a
/// fragment and reporting the loss.
///
/// Bounded rather than unbounded because the same loop carries disconnects:
/// waiting forever on a peer that has stopped draining would stall cleanup
/// for every peer, including that one.
const FRAGMENT_BACKPRESSURE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct DatagramConfig {
    pub service: ServiceUuid,
    pub characteristic: CharacteristicUuid,
    pub max_message_len: usize,
    pub reassembly_timeout: Duration,
    pub max_concurrent_reassemblies: usize,
    /// Defaults to `WithResponse`: ATT write
    /// requests are acknowledged. `WithoutResponse` trades that for
    /// throughput and will silently drop under load — opt in only where the
    /// layer above tolerates loss.
    pub write_type: WriteType,
    /// How many *completed* messages may sit unread before the reassembler
    /// stops accepting more. The reassembly limits bound only partial
    /// messages, so without this a slow or stalled consumer is a memory
    /// exhaustion vector.
    pub inbound_queue_depth: usize,
    /// Raw fragments buffered per peer before the oldest are dropped.
    /// Bounding the *completed* queue alone is not enough: once reassembly
    /// blocks on a full completed queue it stops draining fragments, and an
    /// unbounded fragment queue then grows without limit instead.
    pub fragment_queue_depth: usize,
    /// Accepted-but-not-yet-taken peripheral channels buffered by [`serve`]
    /// before further centrals are refused. Each queued channel pins a
    /// backend `Arc`, its reassembly buffers and a live task, so an
    /// unbounded queue lets a peer that connects and disconnects in a loop
    /// grow the process without limit while the caller is slow to accept.
    pub accept_queue_depth: usize,
}

impl DatagramConfig {
    pub fn new(service: ServiceUuid, characteristic: CharacteristicUuid) -> Self {
        Self {
            service,
            characteristic,
            max_message_len: DEFAULT_MAX_MESSAGE_LEN,
            reassembly_timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            max_concurrent_reassemblies: DEFAULT_MAX_CONCURRENT_REASSEMBLIES,
            write_type: WriteType::WithResponse,
            inbound_queue_depth: DEFAULT_INBOUND_QUEUE_DEPTH,
            fragment_queue_depth: DEFAULT_FRAGMENT_QUEUE_DEPTH,
            accept_queue_depth: DEFAULT_ACCEPT_QUEUE_DEPTH,
        }
    }

    /// `mpsc::channel(0)` panics, and `inbound_queue_depth` is a public
    /// field with no non-zero invariant — validate rather than letting a
    /// caller's zero crash `connect`, or worse, panic `serve`'s detached
    /// background task where nothing observes it.
    fn validate(&self) -> Result<()> {
        if self.inbound_queue_depth == 0 {
            return Err(BleError::Gatt(
                "inbound_queue_depth must be at least 1".to_string(),
            ));
        }
        if self.fragment_queue_depth == 0 {
            return Err(BleError::Gatt(
                "fragment_queue_depth must be at least 1".to_string(),
            ));
        }
        if self.accept_queue_depth == 0 {
            return Err(BleError::Gatt(
                "accept_queue_depth must be at least 1".to_string(),
            ));
        }
        Ok(())
    }

    fn limits(&self) -> ReassemblyLimits {
        ReassemblyLimits {
            max_message_len: self.max_message_len,
            reassembly_timeout: self.reassembly_timeout,
            max_concurrent_reassemblies: self.max_concurrent_reassemblies,
        }
    }

    /// The GATT service this tier expects on the wire. Both roles must agree,
    /// so both derive it from here rather than hand-rolling a spec.
    pub fn service_spec(&self) -> GattServiceSpec {
        GattServiceSpec {
            uuid: self.service,
            characteristics: vec![GattCharacteristicSpec {
                uuid: self.characteristic,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        }
    }
}

/// The configured ceiling, lowered to whatever the fragment budget can
/// actually carry. Without this the two limits disagree: a peripheral
/// budgets 14-byte fragments against the spec-minimum MTU, capping a
/// message at ~896 KiB, while `max_message_len` defaults to 1 MiB — so
/// messages in the gap passed the size check and then failed inside
/// `split()` one call later.
fn effective_max_message_len(configured: usize, fragment_budget: usize) -> usize {
    configured.min(fragment_budget.saturating_mul(MAX_FRAGMENTS))
}

/// How a channel pushes bytes at its peer — the only thing that differs
/// between the two roles once fragmentation is factored out.
enum Sink {
    /// Central: write to the remote characteristic.
    Connection {
        connection: Box<dyn GattConnection>,
        write_type: WriteType,
    },
    /// Peripheral: notify subscribed centrals. See the module-level note on
    /// this being a broadcast.
    Notify {
        backend: Arc<dyn Backend>,
        /// Cleared when this channel's peer disconnects.
        ///
        /// Because notify is a broadcast that cannot be addressed to one
        /// peer on BlueZ, a stale channel is not merely useless — it is
        /// dangerous. Once its peer has gone and a *different* central has
        /// been accepted, a `send` on the old channel would deliver this
        /// peer's fragments to the new one, interleaved into its stream,
        /// while `peer()` still reported the departed address.
        active: Arc<AtomicBool>,
    },
}

/// An ordered, opaque-bytes channel to one peer.
///
/// # Delivery guarantees — read before relying on `send`
///
/// `send` returning `Ok` means every fragment was accepted by the local
/// stack and, where the transport confirms it, acknowledged at the link
/// layer by the peer's controller. **It does not mean the peer's
/// application received the message.**
///
/// The gap is not an implementation shortcut, it is structural. A
/// `WithResponse` write is acknowledged by the receiving *stack* before this
/// library ever sees the payload, so when the receiver is not draining fast
/// enough there is no longer any way to fail the sender's write: the ack has
/// already gone out. `serve` applies backpressure for
/// [`FRAGMENT_BACKPRESSURE_TIMEOUT`] before giving up, and reports the loss
/// to the *receiving* side as an error item from [`Self::recv`] — the only
/// endpoint that can still be told.
///
/// So: this is a reliable *link*, not a reliable *protocol*. Consumers
/// needing end-to-end delivery guarantees must acknowledge at their own
/// layer, exactly as they would over UDP. That is consistent with this
/// tier's job — carrying bytes it does not interpret.
pub struct DatagramChannel {
    peer: PeerAddress,
    characteristic: CharacteristicUuid,
    sink: Mutex<Sink>,
    inbound: ReceiverStream<Result<Vec<u8>>>,
    /// Set when inbound data was dropped for this channel.
    ///
    /// A dedicated flag rather than an error pushed onto the message queue:
    /// overflow happens precisely when that queue is full, so the report
    /// would be the first thing discarded — the failure would silence its
    /// own alarm.
    overflow: Arc<AtomicBool>,
    next_msg_id: u16,
    max_message_len: usize,
    /// Fixed at construction from the negotiated MTU. BLE does not
    /// renegotiate mid-session in practice, so this is not refreshed.
    fragment_budget: usize,
    /// Background tasks owned by this channel, aborted on drop. Without
    /// this, dropping a channel without calling `close()` leaves the
    /// reassembly and link-loss tasks running until the peer or the OS
    /// happens to end the link.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for DatagramChannel {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl DatagramChannel {
    pub fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    /// Largest payload a single `send` will accept on *this* channel.
    ///
    /// May be lower than the configured `max_message_len`: a message also
    /// has to fit in `MAX_FRAGMENTS` fragments of `fragment_budget` bytes.
    /// The peripheral role budgets against the spec-minimum MTU, so its
    /// real ceiling is well under the 1 MiB default — reporting the
    /// configured value there would promise a size that `send` then
    /// refuses.
    pub fn max_message_len(&self) -> usize {
        self.max_message_len
    }

    /// Payload bytes carried per fragment on this channel. Exposed mainly so
    /// tests can assert a message genuinely fragmented rather than trusting
    /// that it did.
    pub fn fragment_budget(&self) -> usize {
        self.fragment_budget
    }

    pub async fn send(&mut self, payload: Vec<u8>) -> Result<()> {
        if payload.len() > self.max_message_len {
            return Err(BleError::Gatt(format!(
                "message of {} bytes exceeds the {}-byte limit for this channel",
                payload.len(),
                self.max_message_len
            )));
        }
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);

        let fragments = split(msg_id, &payload, self.fragment_budget)?;
        let characteristic = self.characteristic;
        let mut sink = self.sink.lock().await;
        for fragment in fragments {
            match &mut *sink {
                Sink::Connection {
                    connection,
                    write_type,
                } => {
                    connection
                        .write_with_type(characteristic, fragment, *write_type)
                        .await?;
                }
                Sink::Notify { backend, active } => {
                    if !active.load(Ordering::SeqCst) {
                        return Err(BleError::NotConnected(self.peer.0.clone()));
                    }
                    // Addressed, not broadcast. Refusing a second central is
                    // asynchronous — it subscribes, `serve` sees it, then
                    // disconnects it — and a broadcast in that window would
                    // hand it this peer's fragments. Addressing closes the
                    // window entirely rather than narrowing it.
                    backend.notify_peer(&self.peer, characteristic, fragment).await?;
                }
            }
        }
        Ok(())
    }

    /// Next complete message, or `None` once the channel is closed — which
    /// includes the peer vanishing without warning, not just an orderly
    /// `close()`. Without that, a caller mid-conversation would block
    /// forever on a dead link.
    pub async fn recv(&mut self) -> Option<Result<Vec<u8>>> {
        // Checked ahead of the queue so the report is prompt. It is
        // deliberately out of order with respect to messages still buffered:
        // "you have lost data" is more useful now than after draining
        // everything that survived.
        if self.overflow.swap(false, Ordering::SeqCst) {
            return Some(Err(BleError::Gatt(format!(
                "inbound overflow from {}: fragments were dropped and at least one \
                 message is lost",
                self.peer.0
            ))));
        }
        self.inbound.next().await
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut sink = self.sink.lock().await;
        match &mut *sink {
            Sink::Connection { connection, .. } => connection.disconnect().await,
            // The peripheral does not own the link; a central disconnecting
            // is what ends it. Stopping advertising here would tear down
            // every other peer's channel too.
            Sink::Notify { .. } => Ok(()),
        }
    }
}

/// Exclude a central this server has refused.
///
/// Every refusal path must go through this. Refusing to allocate state does
/// not stop the peer receiving traffic: it is still subscribed, and
/// `Backend::notify` is a broadcast that cannot be addressed to one peer on
/// BlueZ. Disconnecting is the only portable exclusion.
async fn disconnect_refused(backend: &dyn Backend, peer: &PeerAddress) {
    if let Err(err) = backend.disconnect_peer(peer).await {
        eprintln!(
            "[ble-gatt][datagram] could not disconnect refused central {}: {err} \
             — it may still receive broadcast notifications",
            peer.0
        );
    }
}

/// Feed inbound fragments through a reassembler and forward whole messages.
/// Shared by both roles — the only difference is where fragments come from.
///
/// Takes sole ownership of `out`. That is deliberate: the returned handle's
/// `abort()` is then sufficient to close the consumer's `recv()`, because
/// aborting drops the only sender. An earlier version handed a clone to the
/// link-loss watcher as well, so dropping the watcher's copy closed nothing
/// and `recv()` hung forever on a dead link.
fn spawn_reassembly<S>(
    mut fragments: S, limits: ReassemblyLimits, out: mpsc::Sender<Result<Vec<u8>>>,
    overflow: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()>
where
    S: tokio_stream::Stream<Item = Result<Vec<u8>>> + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut reassembler = Reassembler::new(limits);
        // `Reassembler::expire` only runs from inside `accept`, so a peer
        // that sends half a message and then goes quiet would otherwise pin
        // its partial sets for the life of the link — `reassembly_timeout`
        // would silently never fire. Ticking here is what makes the
        // configured timeout real on an idle connection.
        let tick = (limits.reassembly_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                incoming = fragments.next() => {
                    let Some(raw) = incoming else {
                        // Fragment source ended: the link is gone. Dropping
                        // `out` here is what makes `recv()` return `None`.
                        return;
                    };
                    // A gap reported by the backend — payloads the peer was
                    // told were delivered but that never reached us. Surface
                    // it rather than letting the affected message expire
                    // unexplained, and discard partial sets, since any of
                    // them may be missing the fragment that was lost.
                    let raw = match raw {
                        Ok(raw) => raw,
                        Err(_) => {
                            overflow.store(true, Ordering::SeqCst);
                            reassembler = Reassembler::new(limits);
                            continue;
                        }
                    };
                    let Some((header, payload)) = FragmentHeader::parse(&raw) else {
                        // Too short to be ours — a foreign write to our
                        // characteristic, not a reason to kill the channel.
                        continue;
                    };
                    match reassembler.accept(header, payload, Instant::now()) {
                        Accept::Complete(message) => {
                            // Bounded, and awaited rather than dropped: an
                            // unbounded queue let a peer sending valid
                            // single-fragment messages faster than the
                            // consumer reads grow memory without limit.
                            // Blocking here pushes back through the BLE
                            // stack, which is where backpressure belongs.
                            if out.send(Ok(message)).await.is_err() {
                                return; // receiver dropped; stop reassembling
                            }
                        }
                        Accept::Pending => {}
                        Accept::Rejected(_) => {
                            // Dropped by policy (bounds breach or malformed).
                            // The channel stays usable: one bad message must
                            // not deny service for the rest of the session.
                        }
                    }
                }
                _ = tokio::time::sleep(tick) => {
                    reassembler.expire(Instant::now());
                }
            }
        }
    })
}

/// Central role: connect to `peer` and bring up a channel over `config`'s
/// service.
pub async fn connect(
    backend: Arc<dyn Backend>, peer: &PeerAddress, config: &DatagramConfig,
) -> Result<DatagramChannel> {
    config.validate()?;
    // Subscribe before connecting, for the same reason `serve` subscribes
    // before advertising: `events()` is a broadcast, so a disconnect landing
    // between `connect`/`subscribe` succeeding and this call is lost. The
    // backend contract explicitly does not promise that a notification
    // stream closes with the link, so losing it leaves `recv()` pending
    // forever with nothing left to wake it.
    let events = backend.events();
    let mut connection = backend.connect(peer).await?;
    let notifications = connection.subscribe(config.characteristic).await?;

    let budget = connection
        .max_write_len()
        .checked_sub(FRAGMENT_HEADER_LEN)
        .filter(|budget| *budget > 0)
        .ok_or_else(|| {
            BleError::Gatt(format!(
                "negotiated MTU leaves no room for payload after the \
                 {FRAGMENT_HEADER_LEN}-byte fragment header"
            ))
        })?;

    let (tx, rx) = mpsc::channel(config.inbound_queue_depth);
    let overflow = Arc::new(AtomicBool::new(false));
    let reassembly =
        spawn_reassembly(notifications, config.limits(), tx, overflow.clone());
    // The watcher gets an *AbortHandle*, not the JoinHandle. Handing over
    // the JoinHandle meant aborting the watcher merely dropped it — and
    // dropping a Tokio JoinHandle detaches its task rather than cancelling
    // it, so reassembly outlived the channel that owned it.
    let watcher = spawn_link_loss_watch(events, peer.clone(), reassembly.abort_handle());

    Ok(DatagramChannel {
        peer: peer.clone(),
        characteristic: config.characteristic,
        sink: Mutex::new(Sink::Connection {
            connection,
            write_type: config.write_type,
        }),
        inbound: ReceiverStream::new(rx),
        // Set when the backend reports it dropped notifications — the
        // central-side equivalent of `serve`'s fragment-queue overflow.
        overflow,
        next_msg_id: 0,
        max_message_len: effective_max_message_len(config.max_message_len, budget),
        fragment_budget: budget,
        tasks: vec![reassembly, watcher],
    })
}

/// Turn link loss into `recv() -> None` by aborting the reassembly task,
/// which drops the only sender on the inbound channel.
///
/// No error is delivered — "the peer walked away" is not a fault of the
/// caller, and a closed channel already says everything it needs to. The
/// notification stream itself cannot be relied on to end here: on a real
/// backend a subscription outlives the link, so waiting for it to close
/// would be waiting forever.
fn spawn_link_loss_watch(
    mut events: BoxStream<GattEvent>, peer: PeerAddress,
    reassembly: tokio::task::AbortHandle,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if matches!(
                &event,
                GattEvent::Disconnected { peer: lost, local_role: Role::Central } if *lost == peer
            ) {
                reassembly.abort();
                return;
            }
        }
        // Event stream ended: nothing more can arrive, so the reassembly
        // task would otherwise linger for the life of the process.
        reassembly.abort();
    })
}

/// Peripheral role: advertise `config`'s service and serve **one** central
/// at a time.
///
/// # Why one, and not many
///
/// `Backend::notify` broadcasts to every subscribed central — it is the only
/// per-characteristic primitive both platforms expose. That makes serving
/// two centrals concurrently actively unsafe, not merely wasteful:
///
/// - every central receives every other central's fragments, and
/// - each `DatagramChannel` starts its `msg_id` counter at 0, so the very
///   first message on two channels shares both `msg_id` and often `total`.
///   The `InconsistentTotal` guard only fires when `total` differs, so those
///   fragments interleave inside a third party's reassembler and complete
///   into a message that is a blend of two — delivered to `recv()` as valid,
///   with no error.
///
/// Silent corruption is worse than a missing feature, so additional centrals
/// are refused while one is active rather than served incorrectly. Lifting
/// this needs a per-peer notify on the `Backend` port: straightforward on
/// Android (`notifyCharacteristicChanged` already takes a device),
/// unresolved on BlueZ.
pub async fn serve(
    backend: Arc<dyn Backend>, config: &DatagramConfig,
) -> Result<BoxStream<DatagramChannel>> {
    config.validate()?;
    // Subscribe *before* advertising. `events()` is a broadcast subscription,
    // so anything emitted before this call is lost — and both backends can
    // emit a peripheral-role `Connected` during `advertise`: Android can
    // accept a central before its advertise callback returns, and Linux
    // starts its inbound watcher inside `advertise`. With a central that
    // waits for the server to speak first there is no later write to
    // recreate the event, so losing it hangs `serve` forever.
    let mut events = backend.events();
    backend.advertise(config.service_spec()).await?;

    let (channels_tx, channels_rx) = mpsc::channel(config.accept_queue_depth);
    let config = config.clone();
    // The peripheral has no `GattConnection` to ask for a negotiated MTU, so
    // it budgets against the spec-minimum. Conservative on purpose:
    // undersized fragments always fit, oversized ones would be truncated by
    // the stack with no error.
    let peripheral_budget = crate::backend::DEFAULT_ATT_MTU as usize
        - crate::backend::ATT_HEADER_LEN
        - FRAGMENT_HEADER_LEN;

    tokio::spawn(async move {
        // At most one entry: see the doc comment on why concurrent centrals
        // are refused rather than served.
        // The fragment sender plus the liveness flag handed to that peer's
    // channel, so a disconnect can invalidate the channel the caller holds.
    #[allow(clippy::type_complexity)]
    let mut inbound: HashMap<
        PeerAddress,
        (mpsc::Sender<Vec<u8>>, Arc<AtomicBool>, Arc<AtomicBool>),
    > = HashMap::new();

        // Dropping the accept stream means "no more *new* peers" — it does
        // not abandon the ones already handed over, which still need this
        // loop to deliver their inbound fragments. So the task ends only
        // once the stream is closed *and* no channel is still being served;
        // until then it would otherwise sit on `events.next()` forever,
        // holding the backend `Arc` and the event subscription alive with
        // nothing able to wake it (`stop_advertising` does not close the
        // event stream).
        let mut accept_closed = false;

        loop {
            let event = if accept_closed {
                match events.next().await {
                    Some(event) => event,
                    None => return,
                }
            } else {
                tokio::select! {
                    _ = channels_tx.closed() => {
                        accept_closed = true;
                        if inbound.is_empty() {
                            return;
                        }
                        continue;
                    }
                    event = events.next() => match event {
                        Some(event) => event,
                        None => return,
                    },
                }
            };
            match event {
                // Only inbound connections belong to the peripheral role.
                // Without the role check, this backend's own *outbound*
                // `connect` calls also surface here — yielding a phantom
                // channel for a remote peripheral and, worse, occupying the
                // single-central slot so the genuine inbound central is
                // refused.
                GattEvent::Connected {
                    peer,
                    local_role: Role::Peripheral,
                } => {
                    // `Connected` is NOT once-per-connection. Both real
                    // backends re-emit it ahead of every characteristic
                    // write, because neither has a true server-side
                    // connection signal to key off (see `Backend::events`).
                    // Rebuilding the channel each time would drop the
                    // previous fragment sender, killing the channel already
                    // handed to the caller and making any multi-fragment
                    // message impossible to reassemble.
                    if inbound.contains_key(&peer) {
                        continue;
                    }
                    // Shutting down: service what we still hold, but take on
                    // nothing new — and in particular do not refuse-and-
                    // disconnect, which would evict a peer belonging to a
                    // newer generation on this same backend.
                    if accept_closed {
                        continue;
                    }
                    if let Some(active) = inbound.keys().next() {
                        eprintln!(
                            "[ble-gatt][datagram] refusing central {} — already serving {} \
                             and notify is a broadcast, so serving both would corrupt \
                             each other's messages",
                            peer.0, active.0
                        );
                        // Refusing to allocate state is NOT refusing service.
                        // The peer subscribed without needing our consent, so
                        // until it is disconnected every `Sink::Notify`
                        // broadcast still reaches it — handing it the served
                        // peer's fragments, interleaved into its own stream.
                        // Disconnecting is the only portable exclusion: BlueZ
                        // cannot address a notification to one peer at all.
                        disconnect_refused(backend.as_ref(), &peer).await;
                        continue;
                    }
                    let active = Arc::new(AtomicBool::new(true));
                    let (frag_tx, frag_rx) = mpsc::channel(config.fragment_queue_depth);
                    let (msg_tx, msg_rx) = mpsc::channel(config.inbound_queue_depth);
                    let overflow = Arc::new(AtomicBool::new(false));
                    let reassembly = spawn_reassembly(
                        ReceiverStream::new(frag_rx).map(Ok),
                        config.limits(),
                        msg_tx,
                        overflow.clone(),
                    );

                    let channel = DatagramChannel {
                        peer: peer.clone(),
                        characteristic: config.characteristic,
                        sink: Mutex::new(Sink::Notify {
                            backend: backend.clone(),
                            active: active.clone(),
                        }),
                        inbound: ReceiverStream::new(msg_rx),
                        overflow: overflow.clone(),
                        next_msg_id: 0,
                        max_message_len: effective_max_message_len(
                            config.max_message_len,
                            peripheral_budget,
                        ),
                        fragment_budget: peripheral_budget,
                        tasks: vec![reassembly],
                    };
                    // try_send, not send: awaiting here would stall the one
                    // loop that also delivers writes and disconnects, so a
                    // caller slow to accept would freeze the peers it has
                    // already accepted. `inbound` is only populated once the
                    // channel is safely handed over — a refused central must
                    // not occupy the single-central slot.
                    match channels_tx.try_send(channel) {
                        Ok(()) => {
                            inbound.insert(peer.clone(), (frag_tx, active, overflow.clone()));
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            eprintln!(
                                "[ble-gatt][datagram] accept queue full, refusing central {} \
                                 — the caller is not draining `serve`'s stream",
                                peer.0
                            );
                            // Same exclusion as the already-serving branch,
                            // and for the same reason: leaving this peer
                            // connected leaves it *subscribed*, so once the
                            // queue drains and another central is accepted,
                            // every broadcast notification reaches this one
                            // too.
                            disconnect_refused(backend.as_ref(), &peer).await;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // Do NOT disconnect here. A generation that is
                            // shutting down must not touch peers it is not
                            // serving: the same backend may already have a
                            // *newer* `serve` that legitimately owns this
                            // peer, and disconnecting it would kill the new
                            // session on behalf of a dead one.
                            accept_closed = true;
                            if inbound.is_empty() {
                                return;
                            }
                        }
                    }
                }
                GattEvent::CharacteristicWritten {
                    peer,
                    characteristic,
                    value,
                } => {
                    if characteristic != config.characteristic {
                        continue;
                    }
                    if let Some((tx, _, overflow)) = inbound.get(&peer) {
                        // Try first, then apply real backpressure before
                        // considering a drop. The sender's ATT write has
                        // already been acknowledged by the stack by the time
                        // this event exists, so a dropped fragment is loss
                        // that the *sender* cannot be told about — worth
                        // waiting to avoid.
                        //
                        // The wait is bounded because this loop also carries
                        // disconnects: blocking it indefinitely on a peer
                        // that has stopped draining would stall cleanup for
                        // everyone, including the peer that is stuck.
                        match tx.try_send(value) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {}
                            Err(mpsc::error::TrySendError::Full(value)) => {
                                let queued = tokio::time::timeout(
                                    FRAGMENT_BACKPRESSURE_TIMEOUT,
                                    tx.send(value),
                                )
                                .await;
                                if queued.is_err() {
                                    // Report the loss to the receiver rather
                                    // than letting the message quietly time
                                    // out. It is the only endpoint that can
                                    // still be told.
                                    overflow.store(true, Ordering::SeqCst);
                                    eprintln!(
                                        "[ble-gatt][datagram] fragment queue full for {} after \
                                         backpressure; dropping fragment and reporting loss",
                                        peer.0
                                    );
                                }
                            }
                        }
                    }
                }
                GattEvent::Disconnected {
                    peer,
                    local_role: Role::Peripheral,
                } => {
                    // Dropping the fragment sender ends that peer's
                    // reassembly task, which closes its channel's `recv()`.
                    // Clearing the flag closes the *send* half: without it
                    // the caller could still push fragments into a broadcast
                    // that the next accepted central would receive.
                    if let Some((_, active, _)) = inbound.remove(&peer) {
                        active.store(false, Ordering::SeqCst);
                    }
                    if accept_closed && inbound.is_empty() {
                        return;
                    }
                }
                // Central-role lifecycle belongs to `connect`, not here.
                GattEvent::Connected { .. } | GattEvent::Disconnected { .. } => {}
            }
        }
    });

    Ok(Box::pin(ReceiverStream::new(channels_rx)))
}
