//! Owned state machines for a peer's link, one per concern (ADR-0005).
//!
//! **Load-bearing. Read `docs/adr/0005-owned-link-state-machine.md` before
//! changing anything here.** Whether a dial happens, whether a stale record
//! blocks one, whether a consumer is told a link died — all of it is
//! downstream of these three tables.
//!
//! Three properties are deliberate and easy to erode by accident, each noted
//! where it matters: transitions are **pure** (no clock read, no lock, no
//! I/O — `now` is an argument), effects are a **bare minimum** rather than a
//! command channel, and unhandled `(state, event)` pairs are **no-ops**
//! because events race transitions that already moved past them.
//!
//! The machines do not decide policy they cannot see. Whether this device is
//! the designated dialer for a pair, how many redials to attempt, when to
//! give up — all of that lives in `PeerLink` above, which reports outcomes
//! back as events. A machine here owns what the *state* is, never who acts.
//!
//! Nothing here is exposed to consumers; `PeerLink` projects a `PeerStatus`.

// The backend wiring and `PeerLink` land in the same change; until every
// phase of that is in, parts of this surface have only their tests as
// callers. Remove this once `android.rs` / `linux.rs` / `peer_link.rs` are
// wired.
#![allow(dead_code)]

use std::time::{Duration, Instant};

/// Identifies successive connections to the same peer, so a lifecycle event
/// queued from a previous one is recognisable as stale. Minted by the caller
/// (`PeerLink`), carried by the machine so effects can name it.
pub(crate) type Session = u64;

/// How long a dial may run before it is abandoned. Seeded from the Linux
/// backend's former `CONNECT_TIMEOUT`; tuned on hardware evidence.
pub(crate) const DIAL_WINDOW: Duration = Duration::from_secs(20);

/// How long to wait for the platform to confirm a `disconnect()` before
/// forcing the teardown. Seeded from Android's former `DISCONNECT_TIMEOUT`.
pub(crate) const DISCONNECT_WINDOW: Duration = Duration::from_secs(5);

/// How long one abandoned-dial cleanup attempt may run before it is retried.
/// Seeded from the Linux backend's former `CLEANUP_DISCONNECT_TIMEOUT`.
pub(crate) const DRAIN_WINDOW: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// RadioState — the universal power-state seam, one per backend.
// ---------------------------------------------------------------------------

/// Whether the platform BLE radio is usable.
///
/// A new backend implements exactly one thing — feeding this machine
/// `PoweredOff` / `PoweredOn` / `NoAdapter` — and inherits correct teardown
/// of every link for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RadioState {
    /// A toggle: the adapter exists but is off. A later `PoweredOn` recovers.
    Off,
    On,
    /// No usable adapter for the wanted role, permanently. Distinct from
    /// `Off` because a consumer renders it differently — a dead grey row with
    /// no affordance, not a temporary one.
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RadioEvent {
    PoweredOn,
    PoweredOff,
    NoAdapter,
}

/// What the backend must do when the radio stops being usable: submit
/// `CentralEvent::RadioLost` / `PeripheralEvent::RadioLost` to every per-peer
/// machine it holds, then run their effects. Not a command channel — this is
/// the one thing `RadioState` must cause to keep the per-peer invariants true.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RadioEffect {
    InvalidateAllLinks,
}

impl RadioState {
    pub(crate) fn usable(&self) -> bool {
        matches!(self, RadioState::On)
    }

    /// Pure transition. `_now` is unused (this machine has no deadlines) but
    /// kept in the signature so every machine dispatches the same way.
    pub(crate) fn apply(&self, event: RadioEvent, _now: Instant) -> (RadioState, Vec<RadioEffect>) {
        use RadioEvent as E;
        use RadioState as S;
        match (self, event) {
            (S::On, E::PoweredOff) => (S::Off, vec![RadioEffect::InvalidateAllLinks]),
            (S::On, E::NoAdapter) => (S::Unsupported, vec![RadioEffect::InvalidateAllLinks]),
            (S::Off, E::PoweredOn) => (S::On, vec![]),
            (S::Off, E::NoAdapter) => (S::Unsupported, vec![]),
            // Once permanently unsupported, a spurious PoweredOn is ignored:
            // the adapter that is missing cannot power on. Only a fresh
            // `NoAdapter -> ...` path (a new backend instance) leaves it.
            (S::Unsupported, _) => (S::Unsupported, vec![]),
            (state, _) => (*state, vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// CentralLink — central-role connection lifecycle, one per peer address.
// ---------------------------------------------------------------------------

/// One peer's central-role link.
///
/// > **Invariant:** a platform GATT connection to this peer exists **iff**
/// > the state is `Connected` or `Disconnecting`.
///
/// `Dialing` is not connected yet. `Draining` is a *previous* attempt's
/// teardown. Every "already open, refuses forever" bug is a violation of that
/// sentence — asserted by `has_connection` and property-tested below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CentralLink {
    /// No link, nothing in flight. A dial may proceed.
    Idle,
    /// An outbound `connect()` is issued; the platform has not confirmed.
    Dialing { since: Instant, session: Session },
    /// The link is up; a `GattConnection` is (or is about to be) in the
    /// caller's hands.
    Connected { session: Session },
    /// A `disconnect()` is issued; the platform has not confirmed.
    Disconnecting { since: Instant, session: Session },
    /// An abandoned dial's cleanup disconnect is running (the Linux
    /// `pending_cleanup` quarantine). Blocks a fresh dial until it clears.
    /// On Android this state is simply never entered.
    Draining { since: Instant },
    /// The radio is down; no link can exist. Left only by `RadioBack`.
    RadioOff,
}

/// Something that happened to a central-role link. Submitted by whichever
/// component observed it (a platform callback, `PeerLink`, the tick loop);
/// the machine decides what it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CentralEvent {
    /// The caller decided to dial and has issued the platform call.
    DialStarted { session: Session },
    /// The platform confirmed the link.
    DialSucceeded,
    /// The platform reported failure, or the link died before completing.
    DialFailed,
    /// The caller called `disconnect()`.
    DisconnectRequested,
    /// An unsolicited drop of a `Connected` link.
    LinkDropped,
    /// The platform confirmed a teardown the caller asked for.
    DisconnectConfirmed,
    /// (Linux) an abandoned dial's guard began its cleanup disconnect.
    CleanupStarted,
    /// (Linux) that cleanup disconnect completed.
    CleanupConfirmed,
    /// The radio stopped being usable. Fanned in from `RadioState`; valid
    /// from every state.
    RadioLost,
    /// The radio is usable again. Fanned in; recovers `RadioOff` only.
    RadioBack,
    /// Time passed. Drives every deadline; nothing else here reads a clock.
    Tick,
}

/// What must happen as a consequence of a transition.
///
/// Deliberately one variant. Effects exist only for what a machine *must*
/// cause to keep its invariant true — not a command channel. Dialing, for
/// instance, is not an effect: the caller owns that decision and reports it
/// as `DialStarted`, so the state never claims an attempt nobody made.
///
/// Close / forget the platform link handle for this peer (idempotent).
/// `emit_disconnected` additionally pushes a `Disconnected` lifecycle event —
/// `true` for an unsolicited or radio-forced loss the consumer must learn
/// about, `false` for a teardown the caller asked for (its own call already
/// told it) or a resource the consumer never saw as a live link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TearDown {
    pub(crate) emit_disconnected: bool,
}

impl CentralLink {
    /// The starting point for a peer whose radio is up.
    pub(crate) fn new() -> Self {
        CentralLink::Idle
    }

    /// The invariant, in code. `PeerLink` and `ensure_current` answer "is
    /// there a live link" from this instead of a separate flag.
    pub(crate) fn has_connection(&self) -> bool {
        matches!(
            self,
            CentralLink::Connected { .. } | CentralLink::Disconnecting { .. }
        )
    }

    /// Whether any platform resource (a half-open dial, a cleanup disconnect,
    /// a live link) needs closing. Broader than `has_connection`: it also
    /// covers `Dialing` and `Draining`, whose resources are not links a
    /// consumer knows about but still must be torn down on radio loss.
    fn holds_resource(&self) -> bool {
        !matches!(self, CentralLink::Idle | CentralLink::RadioOff)
    }

    /// Whether a dial may start now.
    pub(crate) fn may_dial(&self) -> bool {
        matches!(self, CentralLink::Idle)
    }

    pub(crate) fn session(&self) -> Option<Session> {
        match self {
            CentralLink::Dialing { session, .. }
            | CentralLink::Connected { session }
            | CentralLink::Disconnecting { session, .. } => Some(*session),
            _ => None,
        }
    }

    /// The pure transition. Returns the next state and any effects.
    ///
    /// Unhandled `(state, event)` pairs are no-ops by design: a late
    /// `LinkDropped` for a peer that already disconnected, a `Tick` for a
    /// state with no deadline, a `DialSucceeded` that lost a race with
    /// `RadioLost` — none should panic or move a settled link.
    pub(crate) fn apply(&self, event: CentralEvent, now: Instant) -> (CentralLink, Vec<TearDown>) {
        use CentralEvent as E;
        use CentralLink as S;

        match (self, event) {
            // Radio loss is valid from every state and matched first. The
            // effect is decided from whether a platform resource was held,
            // and whether the consumer knew it as a live link.
            (state, E::RadioLost) => {
                let effects = if state.has_connection() {
                    vec![TearDown { emit_disconnected: true }]
                } else if state.holds_resource() {
                    vec![TearDown { emit_disconnected: false }]
                } else {
                    vec![]
                };
                (S::RadioOff, effects)
            }
            (S::RadioOff, E::RadioBack) => (S::Idle, vec![]),

            (S::Idle, E::DialStarted { session }) => (S::Dialing { since: now, session }, vec![]),

            (S::Dialing { session, .. }, E::DialSucceeded) => {
                (S::Connected { session: *session }, vec![])
            }
            // A dial that failed or timed out: close the half-open attempt,
            // but no `Disconnected` event — the caller learns via its
            // `connect()` return, not the lifecycle stream.
            (S::Dialing { .. }, E::DialFailed) => {
                (S::Idle, vec![TearDown { emit_disconnected: false }])
            }
            (S::Dialing { since, .. }, E::Tick) if now.duration_since(*since) >= DIAL_WINDOW => {
                (S::Idle, vec![TearDown { emit_disconnected: false }])
            }

            (S::Connected { session }, E::DisconnectRequested) => {
                (S::Disconnecting { since: now, session: *session }, vec![])
            }
            (S::Connected { .. }, E::LinkDropped) => {
                (S::Idle, vec![TearDown { emit_disconnected: true }])
            }

            // The caller asked for this and its `disconnect()` returned — it
            // already knows, so no event. The one legitimate silent loss of
            // a connection state.
            (S::Disconnecting { .. }, E::DisconnectConfirmed) => (S::Idle, vec![]),
            (S::Disconnecting { since, .. }, E::Tick)
                if now.duration_since(*since) >= DISCONNECT_WINDOW =>
            {
                (S::Idle, vec![TearDown { emit_disconnected: true }])
            }

            (S::Idle | S::Dialing { .. }, E::CleanupStarted) => {
                (S::Draining { since: now }, vec![])
            }
            (S::Draining { .. }, E::CleanupConfirmed) => (S::Idle, vec![]),
            // The cleanup attempt timed out; retry it (stay `Draining`,
            // re-arm the window). Matches the former `pending_cleanup` retry
            // loop: the address stays quarantined until a disconnect
            // genuinely completes.
            (S::Draining { since }, E::Tick) if now.duration_since(*since) >= DRAIN_WINDOW => {
                (S::Draining { since: now }, vec![TearDown { emit_disconnected: false }])
            }

            (state, _) => (*state, vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// PeripheralLink — a remote central attached to our GATT server, per peer.
// ---------------------------------------------------------------------------

/// One remote central's attachment to our GATT server.
///
/// > **Invariant:** our GATT server holds a connection slot for this peer
/// > **iff** the state is `Accepted`, `Serving`, or `Disconnecting`.
///
/// Thinner than `CentralLink` (no dial), same discipline. Replaces the
/// ad-hoc `served_peers` / `server_sessions` maps and the disconnect
/// watchers' generation checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeripheralLink {
    /// No central at this address.
    Absent,
    /// A central connected but has not subscribed (written the CCCD) yet.
    Accepted { session: Session },
    /// Subscribed; `notify_peer` can reach it.
    Serving { session: Session },
    /// We called `disconnect_peer`; the platform has not confirmed.
    Disconnecting { since: Instant, session: Session },
    /// The radio is down. Left only by `RadioBack`.
    RadioOff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeripheralEvent {
    /// A central connected to our server.
    CentralConnected { session: Session },
    /// It wrote the CCCD — a notify route now exists.
    CentralSubscribed,
    /// It disabled the CCCD while staying connected — gone, from the
    /// server's point of view.
    CentralUnsubscribed,
    /// It dropped the connection.
    CentralDropped,
    /// The caller asked to drop it.
    DisconnectRequested,
    /// The platform confirmed the drop.
    DisconnectConfirmed,
    RadioLost,
    RadioBack,
    Tick,
}

impl PeripheralLink {
    pub(crate) fn new() -> Self {
        PeripheralLink::Absent
    }

    pub(crate) fn holds_slot(&self) -> bool {
        matches!(
            self,
            PeripheralLink::Accepted { .. }
                | PeripheralLink::Serving { .. }
                | PeripheralLink::Disconnecting { .. }
        )
    }

    pub(crate) fn session(&self) -> Option<Session> {
        match self {
            PeripheralLink::Accepted { session }
            | PeripheralLink::Serving { session }
            | PeripheralLink::Disconnecting { session, .. } => Some(*session),
            _ => None,
        }
    }

    pub(crate) fn apply(
        &self, event: PeripheralEvent, now: Instant,
    ) -> (PeripheralLink, Vec<TearDown>) {
        use PeripheralEvent as E;
        use PeripheralLink as S;

        match (self, event) {
            (state, E::RadioLost) => {
                let effects = if state.holds_slot() {
                    vec![TearDown { emit_disconnected: true }]
                } else {
                    vec![]
                };
                (S::RadioOff, effects)
            }
            (S::RadioOff, E::RadioBack) => (S::Absent, vec![]),

            // A fresh `CentralConnected` for an address already `Serving`
            // supersedes it — the map now holds the newer session, and the
            // old session's `notify_peer` is refused because it no longer
            // matches (the former `superseded` guard).
            (S::Absent | S::Accepted { .. } | S::Serving { .. }, E::CentralConnected { session }) => {
                (S::Accepted { session }, vec![])
            }
            (S::Accepted { session }, E::CentralSubscribed) => {
                (S::Serving { session: *session }, vec![])
            }
            (S::Serving { .. } | S::Accepted { .. }, E::CentralUnsubscribed | E::CentralDropped) => {
                (S::Absent, vec![TearDown { emit_disconnected: true }])
            }

            (S::Accepted { session } | S::Serving { session }, E::DisconnectRequested) => {
                (S::Disconnecting { since: now, session: *session }, vec![])
            }
            (S::Disconnecting { .. }, E::DisconnectConfirmed) => (S::Absent, vec![]),
            (S::Disconnecting { .. }, E::CentralDropped) => {
                (S::Absent, vec![TearDown { emit_disconnected: true }])
            }
            (S::Disconnecting { since, .. }, E::Tick)
                if now.duration_since(*since) >= DISCONNECT_WINDOW =>
            {
                (S::Absent, vec![TearDown { emit_disconnected: true }])
            }

            (state, _) => (*state, vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    // ---- RadioState ----------------------------------------------------

    #[test]
    fn powering_off_invalidates_links() {
        let (state, effects) = RadioState::On.apply(RadioEvent::PoweredOff, t0());
        assert_eq!(state, RadioState::Off);
        assert_eq!(effects, vec![RadioEffect::InvalidateAllLinks]);
    }

    #[test]
    fn powering_back_on_recovers_without_an_effect() {
        let (state, effects) = RadioState::Off.apply(RadioEvent::PoweredOn, t0());
        assert_eq!(state, RadioState::On);
        assert!(effects.is_empty());
    }

    #[test]
    fn unsupported_is_terminal() {
        let (state, _) = RadioState::Unsupported.apply(RadioEvent::PoweredOn, t0());
        assert_eq!(state, RadioState::Unsupported);
    }

    // ---- CentralLink -------------------------------------------------------

    #[test]
    fn a_full_dial_connect_disconnect_cycle() {
        let start = t0();
        let s = CentralLink::new();
        assert!(s.may_dial());

        let (s, e) = s.apply(CentralEvent::DialStarted { session: 7 }, start);
        assert_eq!(s, CentralLink::Dialing { since: start, session: 7 });
        assert!(e.is_empty());
        assert!(!s.may_dial(), "a dial is already in flight");

        let (s, e) = s.apply(CentralEvent::DialSucceeded, start);
        assert_eq!(s, CentralLink::Connected { session: 7 });
        assert!(e.is_empty());
        assert!(s.has_connection());

        let (s, e) = s.apply(CentralEvent::DisconnectRequested, start);
        assert_eq!(s, CentralLink::Disconnecting { since: start, session: 7 });
        assert!(e.is_empty());
        assert!(s.has_connection(), "still connected until the platform confirms");

        let (s, e) = s.apply(CentralEvent::DisconnectConfirmed, start);
        assert_eq!(s, CentralLink::Idle);
        assert!(e.is_empty(), "the caller asked and already knows — no event");
        assert!(!s.has_connection());
    }

    #[test]
    fn an_unsolicited_drop_tells_the_consumer() {
        let (s, e) = CentralLink::Connected { session: 1 }.apply(CentralEvent::LinkDropped, t0());
        assert_eq!(s, CentralLink::Idle);
        assert_eq!(e, vec![TearDown { emit_disconnected: true }]);
    }

    #[test]
    fn a_dial_that_never_completes_is_abandoned_without_an_event() {
        let start = t0();
        let dialing = CentralLink::Dialing { since: start, session: 1 };
        let (s, e) = dialing.apply(CentralEvent::Tick, start + DIAL_WINDOW);
        assert_eq!(s, CentralLink::Idle);
        assert_eq!(
            e,
            vec![TearDown { emit_disconnected: false }],
            "close the half-open dial, but the caller learns via connect()'s return"
        );
    }

    #[test]
    fn radio_loss_from_connected_tears_down_and_emits() {
        let (s, e) = CentralLink::Connected { session: 1 }.apply(CentralEvent::RadioLost, t0());
        assert_eq!(s, CentralLink::RadioOff);
        assert_eq!(e, vec![TearDown { emit_disconnected: true }]);

        let (s, e) = s.apply(CentralEvent::RadioBack, t0());
        assert_eq!(s, CentralLink::Idle);
        assert!(e.is_empty());
    }

    #[test]
    fn radio_loss_while_dialing_closes_the_attempt_silently() {
        let (s, e) = CentralLink::Dialing { since: t0(), session: 1 }
            .apply(CentralEvent::RadioLost, t0());
        assert_eq!(s, CentralLink::RadioOff);
        assert_eq!(e, vec![TearDown { emit_disconnected: false }]);
    }

    #[test]
    fn radio_loss_while_idle_does_nothing_but_change_state() {
        let (s, e) = CentralLink::Idle.apply(CentralEvent::RadioLost, t0());
        assert_eq!(s, CentralLink::RadioOff);
        assert!(e.is_empty());
    }

    #[test]
    fn draining_retries_until_cleanup_confirms() {
        let start = t0();
        let (s, e) = CentralLink::Idle.apply(CentralEvent::CleanupStarted, start);
        assert_eq!(s, CentralLink::Draining { since: start });
        assert!(e.is_empty());

        // Timed out: retry, re-arm the window, still quarantined.
        let (s, e) = s.apply(CentralEvent::Tick, start + DRAIN_WINDOW);
        assert_eq!(s, CentralLink::Draining { since: start + DRAIN_WINDOW });
        assert_eq!(e, vec![TearDown { emit_disconnected: false }]);

        let (s, e) = s.apply(CentralEvent::CleanupConfirmed, start + DRAIN_WINDOW);
        assert_eq!(s, CentralLink::Idle);
        assert!(e.is_empty());
    }

    #[test]
    fn a_quarantined_peer_cannot_be_dialled() {
        assert!(!CentralLink::Draining { since: t0() }.may_dial());
        assert!(!CentralLink::RadioOff.may_dial());
        assert!(!CentralLink::Connected { session: 1 }.may_dial());
    }

    #[test]
    fn central_late_events_are_ignored() {
        let start = t0();
        for state in [
            CentralLink::Idle,
            CentralLink::RadioOff,
            CentralLink::Connected { session: 1 },
        ] {
            let (next, effects) = state.apply(CentralEvent::DialSucceeded, start);
            assert_eq!(next, state, "a stale DialSucceeded must not move a settled link");
            assert!(effects.is_empty());
        }
    }

    /// The invariant, over every `(state, event)` pair: a `Disconnected`
    /// event is emitted exactly when a connection the consumer knew about
    /// stops existing, and such a connection never stops existing silently —
    /// the one exception being `DisconnectConfirmed`, which reports a
    /// teardown the caller asked for and already knows about.
    #[test]
    fn central_disconnect_is_emitted_exactly_when_a_known_link_ends() {
        let start = t0();
        let states = [
            CentralLink::Idle,
            CentralLink::Dialing { since: start, session: 1 },
            CentralLink::Connected { session: 1 },
            CentralLink::Disconnecting { since: start, session: 1 },
            CentralLink::Draining { since: start },
            CentralLink::RadioOff,
        ];
        let events = [
            CentralEvent::DialStarted { session: 2 },
            CentralEvent::DialSucceeded,
            CentralEvent::DialFailed,
            CentralEvent::DisconnectRequested,
            CentralEvent::LinkDropped,
            CentralEvent::DisconnectConfirmed,
            CentralEvent::CleanupStarted,
            CentralEvent::CleanupConfirmed,
            CentralEvent::RadioLost,
            CentralEvent::RadioBack,
            CentralEvent::Tick,
        ];
        // Far enough ahead that every deadline has expired.
        let now = start + DIAL_WINDOW + DISCONNECT_WINDOW + DRAIN_WINDOW;

        for state in &states {
            for event in &events {
                let (next, effects) = state.apply(*event, now);
                let emitted = effects.iter().any(|t| t.emit_disconnected);
                let known_link_ended = state.has_connection() && !next.has_connection();

                if emitted {
                    assert!(
                        known_link_ended,
                        "emitted Disconnected without a known link ending: \
                         {state:?} + {event:?} -> {next:?}"
                    );
                }
                if known_link_ended && !emitted {
                    assert_eq!(
                        *event,
                        CentralEvent::DisconnectConfirmed,
                        "a known link ended with no Disconnected event: \
                         {state:?} + {event:?} -> {next:?}"
                    );
                }
            }
        }
    }

    /// No `(state, event)` pair leaves a resource-holding state for a
    /// resource-free one without a `TearDown` — except the two events that
    /// report the platform has already resolved it.
    #[test]
    fn central_never_leaks_a_platform_resource() {
        let start = t0();
        let states = [
            CentralLink::Idle,
            CentralLink::Dialing { since: start, session: 1 },
            CentralLink::Connected { session: 1 },
            CentralLink::Disconnecting { since: start, session: 1 },
            CentralLink::Draining { since: start },
            CentralLink::RadioOff,
        ];
        let events = [
            CentralEvent::DialStarted { session: 2 },
            CentralEvent::DialSucceeded,
            CentralEvent::DialFailed,
            CentralEvent::DisconnectRequested,
            CentralEvent::LinkDropped,
            CentralEvent::DisconnectConfirmed,
            CentralEvent::CleanupStarted,
            CentralEvent::CleanupConfirmed,
            CentralEvent::RadioLost,
            CentralEvent::RadioBack,
            CentralEvent::Tick,
        ];
        let now = start + DIAL_WINDOW + DISCONNECT_WINDOW + DRAIN_WINDOW;

        for state in &states {
            for event in &events {
                let (next, effects) = state.apply(*event, now);
                let freed = state.holds_resource() && !next.holds_resource();
                let tore_down = !effects.is_empty();
                let platform_already_resolved = matches!(
                    event,
                    CentralEvent::DisconnectConfirmed | CentralEvent::CleanupConfirmed
                );
                if freed && !tore_down {
                    assert!(
                        platform_already_resolved,
                        "left a resource-holding state with no teardown: \
                         {state:?} + {event:?} -> {next:?}"
                    );
                }
            }
        }
    }

    // ---- PeripheralLink --------------------------------------------------

    #[test]
    fn a_central_connects_subscribes_and_leaves() {
        let start = t0();
        let (s, e) = PeripheralLink::new().apply(PeripheralEvent::CentralConnected { session: 3 }, start);
        assert_eq!(s, PeripheralLink::Accepted { session: 3 });
        assert!(e.is_empty());

        let (s, e) = s.apply(PeripheralEvent::CentralSubscribed, start);
        assert_eq!(s, PeripheralLink::Serving { session: 3 });
        assert!(e.is_empty());
        assert!(s.holds_slot());

        let (s, e) = s.apply(PeripheralEvent::CentralDropped, start);
        assert_eq!(s, PeripheralLink::Absent);
        assert_eq!(e, vec![TearDown { emit_disconnected: true }]);
        assert!(!s.holds_slot());
    }

    #[test]
    fn a_reconnect_supersedes_the_old_session() {
        let (s, e) = PeripheralLink::Serving { session: 3 }
            .apply(PeripheralEvent::CentralConnected { session: 4 }, t0());
        assert_eq!(s, PeripheralLink::Accepted { session: 4 });
        assert!(e.is_empty(), "the map now holds session 4; session 3's notify is refused by mismatch");
    }

    #[test]
    fn peripheral_disconnect_request_is_silent_on_confirm() {
        let start = t0();
        let (s, e) = PeripheralLink::Serving { session: 1 }
            .apply(PeripheralEvent::DisconnectRequested, start);
        assert_eq!(s, PeripheralLink::Disconnecting { since: start, session: 1 });
        assert!(e.is_empty());
        let (s, e) = s.apply(PeripheralEvent::DisconnectConfirmed, start);
        assert_eq!(s, PeripheralLink::Absent);
        assert!(e.is_empty());
    }

    #[test]
    fn radio_loss_drops_a_serving_central() {
        let (s, e) = PeripheralLink::Serving { session: 1 }.apply(PeripheralEvent::RadioLost, t0());
        assert_eq!(s, PeripheralLink::RadioOff);
        assert_eq!(e, vec![TearDown { emit_disconnected: true }]);
    }

    #[test]
    fn peripheral_slot_is_freed_exactly_when_told_or_torn_down() {
        let start = t0();
        let states = [
            PeripheralLink::Absent,
            PeripheralLink::Accepted { session: 1 },
            PeripheralLink::Serving { session: 1 },
            PeripheralLink::Disconnecting { since: start, session: 1 },
            PeripheralLink::RadioOff,
        ];
        let events = [
            PeripheralEvent::CentralConnected { session: 2 },
            PeripheralEvent::CentralSubscribed,
            PeripheralEvent::CentralUnsubscribed,
            PeripheralEvent::CentralDropped,
            PeripheralEvent::DisconnectRequested,
            PeripheralEvent::DisconnectConfirmed,
            PeripheralEvent::RadioLost,
            PeripheralEvent::RadioBack,
            PeripheralEvent::Tick,
        ];
        let now = start + DISCONNECT_WINDOW * 2;

        for state in &states {
            for event in &events {
                let (next, effects) = state.apply(*event, now);
                let emitted = effects.iter().any(|t| t.emit_disconnected);
                let slot_freed = state.holds_slot() && !next.holds_slot();

                if emitted {
                    assert!(
                        slot_freed,
                        "emitted Disconnected without freeing a slot: {state:?} + {event:?} -> {next:?}"
                    );
                }
                if slot_freed && !emitted {
                    assert_eq!(
                        *event,
                        PeripheralEvent::DisconnectConfirmed,
                        "a slot was freed with no Disconnected event: {state:?} + {event:?} -> {next:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn peripheral_late_events_are_ignored() {
        let (next, effects) =
            PeripheralLink::Absent.apply(PeripheralEvent::CentralDropped, t0());
        assert_eq!(next, PeripheralLink::Absent);
        assert!(effects.is_empty());
    }
}
