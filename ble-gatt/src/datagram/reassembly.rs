//! Bounded fragment reassembly.
//!
//! This runs on bytes from a peer that whatever sits above has *not yet*
//! authenticated — a BLE connection is open to anyone in radio range. Every
//! bound here is load-bearing: without them a peer can pin unbounded memory
//! by opening many partial messages, declaring an enormous `total`, or
//! starting messages it never finishes. Modelled on Bitchat's
//! `FragmentManager`, which enforces the same class of limits.
//!
//! Deliberately pure: no I/O, no clock of its own (`now` is passed in), so
//! every bound including the timeout is deterministically testable.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::datagram::fragment::FragmentHeader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReassemblyLimits {
    pub max_message_len: usize,
    pub reassembly_timeout: Duration,
    pub max_concurrent_reassemblies: usize,
}

/// Why a fragment was dropped. Returned rather than logged so the caller
/// decides whether a misbehaving peer is worth reporting — the library
/// stays quiet by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// `total == 0`: a message with no fragments is meaningless.
    EmptyMessage,
    /// `index >= total`: the fragment claims a slot outside its own message.
    IndexOutOfRange,
    /// `total` disagrees with the in-flight value for this `msg_id`.
    InconsistentTotal,
    /// Accepting this fragment would push the message past `max_message_len`.
    MessageTooLarge,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Accept {
    /// All fragments present; here is the reassembled message.
    Complete(Vec<u8>),
    /// Fragment stored, message still incomplete.
    Pending,
    /// Fragment dropped.
    Rejected(RejectReason),
}

struct PartialMessage {
    total: u16,
    fragments: BTreeMap<u16, Vec<u8>>,
    cumulative_len: usize,
    started_at: Instant,
}

/// Reassembles one peer's fragment stream. Hold one per peer — `msg_id` is
/// only unique within a single sender's channel.
pub struct Reassembler {
    limits: ReassemblyLimits,
    partial: BTreeMap<u32, PartialMessage>,
}

impl Reassembler {
    pub fn new(limits: ReassemblyLimits) -> Self {
        Self {
            limits,
            partial: BTreeMap::new(),
        }
    }

    /// Number of in-flight partial messages. Exposed for tests and for a
    /// caller that wants to surface pressure metrics.
    pub fn pending_count(&self) -> usize {
        self.partial.len()
    }

    /// Drop partial messages that have been open longer than the timeout.
    /// Called automatically by `accept`; public so an idle channel can also
    /// release memory without waiting for the next fragment.
    pub fn expire(&mut self, now: Instant) {
        let timeout = self.limits.reassembly_timeout;
        self.partial
            .retain(|_, message| now.duration_since(message.started_at) < timeout);
    }

    pub fn accept(&mut self, header: FragmentHeader, payload: &[u8], now: Instant) -> Accept {
        if header.total == 0 {
            return Accept::Rejected(RejectReason::EmptyMessage);
        }
        if header.index >= header.total {
            return Accept::Rejected(RejectReason::IndexOutOfRange);
        }
        // A single fragment can't exceed the whole-message budget either.
        if payload.len() > self.limits.max_message_len {
            return Accept::Rejected(RejectReason::MessageTooLarge);
        }

        self.expire(now);

        // Fast path: a message that arrives whole in one fragment never
        // touches the partial-message table, so a stream of small messages
        // can't consume reassembly slots at all.
        if header.total == 1 {
            self.partial.remove(&header.msg_id);
            return Accept::Complete(payload.to_vec());
        }

        if let Some(existing) = self.partial.get(&header.msg_id) {
            if existing.total != header.total {
                // Either a peer contradicting itself or a stale msg_id being
                // reused after a wrap. Both make the buffered fragments
                // unusable, so discard rather than mix them.
                self.partial.remove(&header.msg_id);
                return Accept::Rejected(RejectReason::InconsistentTotal);
            }
        } else {
            // Declared size is checkable before storing anything: reject an
            // over-large message on its first fragment rather than after
            // buffering most of it.
            let declared_min = (header.total as usize - 1).saturating_mul(payload.len().max(1));
            if declared_min > self.limits.max_message_len {
                return Accept::Rejected(RejectReason::MessageTooLarge);
            }
            self.evict_if_at_capacity();
            self.partial.insert(
                header.msg_id,
                PartialMessage {
                    total: header.total,
                    fragments: BTreeMap::new(),
                    cumulative_len: 0,
                    started_at: now,
                },
            );
        }

        let limit = self.limits.max_message_len;
        let message = self
            .partial
            .get_mut(&header.msg_id)
            .expect("inserted above when absent");

        // Duplicate index. Keeping the first copy is what stops a peer
        // growing `cumulative_len` without bound using one index — but only
        // when the two copies are genuinely the same fragment.
        //
        // A *differing* payload at an index we already hold means this
        // `msg_id` has been reused while the previous message was still
        // incomplete: `next_msg_id` is a `u16`, so a sender using
        // `WithoutResponse` can wrap through 65,536 messages well inside the
        // reassembly timeout. Merging then blends two messages into one that
        // completes and passes every check — corrupt data delivered as
        // valid, which is the worst outcome this layer can produce. The
        // differing total case above already catches reuse when the sizes
        // disagree; this catches it when they happen to match.
        if let Some(existing) = message.fragments.get(&header.index) {
            if existing.as_slice() == payload {
                return Accept::Pending;
            }
            // Start over from this fragment: the buffered set belongs to a
            // message that will never arrive.
            let restarted = PartialMessage {
                total: header.total,
                fragments: BTreeMap::from([(header.index, payload.to_vec())]),
                cumulative_len: payload.len(),
                started_at: now,
            };
            self.partial.insert(header.msg_id, restarted);
            return Accept::Pending;
        }
        if message.cumulative_len + payload.len() > limit {
            self.partial.remove(&header.msg_id);
            return Accept::Rejected(RejectReason::MessageTooLarge);
        }

        message.cumulative_len += payload.len();
        message.fragments.insert(header.index, payload.to_vec());

        if message.fragments.len() as u16 != message.total {
            return Accept::Pending;
        }

        let completed = self
            .partial
            .remove(&header.msg_id)
            .expect("present immediately above");
        // BTreeMap iterates in key order, so fragments rejoin by index
        // regardless of the order they arrived in.
        let mut out = Vec::with_capacity(completed.cumulative_len);
        for chunk in completed.fragments.values() {
            out.extend_from_slice(chunk);
        }
        Accept::Complete(out)
    }

    fn evict_if_at_capacity(&mut self) {
        if self.partial.len() < self.limits.max_concurrent_reassemblies {
            return;
        }
        // Evict the least recently started, so a peer trickling junk can't
        // starve a message that is genuinely in progress.
        if let Some(oldest) = self
            .partial
            .iter()
            .min_by_key(|(_, message)| message.started_at)
            .map(|(id, _)| *id)
        {
            self.partial.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ReassemblyLimits {
        ReassemblyLimits {
            max_message_len: 1024,
            reassembly_timeout: Duration::from_secs(30),
            max_concurrent_reassemblies: 4,
        }
    }

    fn header(msg_id: u32, index: u16, total: u16) -> FragmentHeader {
        FragmentHeader {
            msg_id,
            index,
            total,
        }
    }

    #[test]
    fn a_single_fragment_message_completes_immediately() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(
            r.accept(header(1, 0, 1), b"hello", now),
            Accept::Complete(b"hello".to_vec())
        );
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn multiple_fragments_reassemble_in_index_order() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 0, 3), b"aaa", now), Accept::Pending);
        assert_eq!(r.accept(header(1, 1, 3), b"bbb", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 2, 3), b"ccc", now),
            Accept::Complete(b"aaabbbccc".to_vec())
        );
    }

    #[test]
    fn fragments_arriving_out_of_order_still_reassemble_correctly() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 2, 3), b"ccc", now), Accept::Pending);
        assert_eq!(r.accept(header(1, 0, 3), b"aaa", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 1, 3), b"bbb", now),
            Accept::Complete(b"aaabbbccc".to_vec())
        );
    }

    #[test]
    fn an_identical_duplicate_index_is_ignored() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        // A genuine retransmission. Ignoring it is what stops a peer growing
        // `cumulative_len` without bound by resending one index.
        assert_eq!(r.accept(header(1, 0, 2), b"first", now), Accept::Pending);
        assert_eq!(r.accept(header(1, 0, 2), b"first", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 1, 2), b"!", now),
            Accept::Complete(b"first!".to_vec())
        );
    }

    #[test]
    fn a_differing_duplicate_index_restarts_rather_than_keeping_the_first_copy() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 0, 2), b"first", now), Accept::Pending);
        // Different bytes at an index already held means this `msg_id` has
        // been reused while the previous message was still incomplete.
        // Keeping the first copy — which this reassembler used to do — is
        // what lets the two messages blend into one that completes and looks
        // valid.
        assert_eq!(r.accept(header(1, 0, 2), b"second", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 1, 2), b"!", now),
            Accept::Complete(b"second!".to_vec()),
            "the completed message must be the new one, not a splice of both"
        );
    }

    #[test]
    fn an_index_outside_the_message_is_rejected() {
        let mut r = Reassembler::new(limits());
        assert_eq!(
            r.accept(header(1, 3, 3), b"x", Instant::now()),
            Accept::Rejected(RejectReason::IndexOutOfRange)
        );
    }

    #[test]
    fn a_zero_fragment_message_is_rejected() {
        let mut r = Reassembler::new(limits());
        assert_eq!(
            r.accept(header(1, 0, 0), b"x", Instant::now()),
            Accept::Rejected(RejectReason::EmptyMessage)
        );
    }

    #[test]
    fn a_total_that_changes_mid_message_discards_the_set() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 0, 3), b"aaa", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 1, 5), b"bbb", now),
            Accept::Rejected(RejectReason::InconsistentTotal)
        );
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn a_message_exceeding_the_size_cap_is_rejected() {
        let mut r = Reassembler::new(ReassemblyLimits {
            max_message_len: 10,
            ..limits()
        });
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 0, 2), b"12345", now), Accept::Pending);
        assert_eq!(
            r.accept(header(1, 1, 2), b"678901", now),
            Accept::Rejected(RejectReason::MessageTooLarge)
        );
        assert_eq!(r.pending_count(), 0, "the partial set must be freed too");
    }

    #[test]
    fn a_single_oversized_fragment_is_rejected_before_being_stored() {
        let mut r = Reassembler::new(ReassemblyLimits {
            max_message_len: 4,
            ..limits()
        });
        assert_eq!(
            r.accept(header(1, 0, 2), b"far too long", Instant::now()),
            Accept::Rejected(RejectReason::MessageTooLarge)
        );
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn exceeding_the_concurrency_cap_evicts_the_oldest_partial_message() {
        let mut r = Reassembler::new(ReassemblyLimits {
            max_concurrent_reassemblies: 2,
            ..limits()
        });
        let base = Instant::now();
        r.accept(header(1, 0, 2), b"a", base, );
        r.accept(header(2, 0, 2), b"b", base + Duration::from_millis(1));
        assert_eq!(r.pending_count(), 2);

        // Third distinct message evicts msg_id 1, the oldest.
        r.accept(header(3, 0, 2), b"c", base + Duration::from_millis(2));
        assert_eq!(r.pending_count(), 2);

        // msg_id 1's buffered fragment is gone: completing it now behaves as
        // a fresh message rather than silently resurrecting stale bytes.
        assert_eq!(
            r.accept(header(1, 1, 2), b"z", base + Duration::from_millis(3)),
            Accept::Pending
        );
    }

    #[test]
    fn a_partial_message_is_dropped_once_it_exceeds_the_timeout() {
        let mut r = Reassembler::new(ReassemblyLimits {
            reassembly_timeout: Duration::from_secs(30),
            ..limits()
        });
        let base = Instant::now();
        assert_eq!(r.accept(header(1, 0, 2), b"aaa", base), Accept::Pending);
        assert_eq!(r.pending_count(), 1);

        r.expire(base + Duration::from_secs(31));
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn interleaved_messages_from_one_peer_do_not_corrupt_each_other() {
        let mut r = Reassembler::new(limits());
        let now = Instant::now();
        assert_eq!(r.accept(header(1, 0, 2), b"one-", now), Accept::Pending);
        assert_eq!(r.accept(header(2, 0, 2), b"two-", now), Accept::Pending);
        assert_eq!(
            r.accept(header(2, 1, 2), b"second", now),
            Accept::Complete(b"two-second".to_vec())
        );
        assert_eq!(
            r.accept(header(1, 1, 2), b"first", now),
            Accept::Complete(b"one-first".to_vec())
        );
    }
    #[test]
    fn a_reused_message_id_starts_a_new_message_rather_than_blending_two() {
        let mut reassembler = Reassembler::new(limits());
        let now = Instant::now();

        // Message A: three fragments, but the middle one is lost.
        assert!(matches!(
            reassembler.accept(header(7, 0, 3), b"AAAA", now),
            Accept::Pending
        ));
        assert!(matches!(
            reassembler.accept(header(7, 2, 3), b"CCCC", now),
            Accept::Pending
        ));

        // `next_msg_id` is a u16, so a sender using unacknowledged writes can
        // wrap all the way round well inside the reassembly timeout and
        // reuse id 7 while A is still incomplete. This message happens to
        // have the same fragment count, so the inconsistent-total check does
        // not catch it.
        assert!(matches!(
            reassembler.accept(header(7, 0, 3), b"xxxx", now),
            Accept::Pending
        ));
        assert!(matches!(
            reassembler.accept(header(7, 1, 3), b"yyyy", now),
            Accept::Pending
        ));

        // The fragment that completes the set must complete the *new*
        // message. Merging would deliver A's surviving fragment inside B —
        // a byte-for-byte blend of two messages, passing every check.
        match reassembler.accept(header(7, 2, 3), b"zzzz", now) {
            Accept::Complete(message) => assert_eq!(
                message, b"xxxxyyyyzzzz",
                "a reused id must not splice the previous message's fragments into this one"
            ),
            other => panic!("expected the new message to complete, got {other:?}"),
        }
    }

}
