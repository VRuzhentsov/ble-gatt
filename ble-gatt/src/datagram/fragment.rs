//! Fragment header encoding and payload splitting.
//!
//! Every fragment — including a message small enough to fit in one — carries
//! the same 6-byte header. A single uniform path is worth the 6 bytes: a
//! "small messages skip the header" optimisation would mean the receiver has
//! to guess which format it is looking at, and that guess is exactly where
//! framing bugs live.
//!
//! ```text
//! offset 0..2   msg_id   u16 little-endian   rolling per-channel counter, wraps
//! offset 2..4   index    u16 little-endian   0-based fragment index
//! offset 4..6   total    u16 little-endian   fragment count for this message
//! offset 6..    payload
//! ```

use crate::error::{BleError, Result};

/// Size of the fragment header in bytes. Subtract from a connection's
/// `max_write_len()` to get the per-fragment payload budget.
pub const FRAGMENT_HEADER_LEN: usize = 6;

/// Largest number of fragments one message can be split into, bounded by the
/// `u16` `total` field.
pub const MAX_FRAGMENTS: usize = u16::MAX as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    pub msg_id: u16,
    pub index: u16,
    pub total: u16,
}

impl FragmentHeader {
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.msg_id.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.total.to_le_bytes());
    }

    /// Split a received fragment into its header and payload. `None` when the
    /// buffer is too short to contain a header — a truncated or foreign write
    /// to our characteristic, which must be dropped rather than panicked on.
    pub fn parse(bytes: &[u8]) -> Option<(Self, &[u8])> {
        if bytes.len() < FRAGMENT_HEADER_LEN {
            return None;
        }
        let header = Self {
            msg_id: u16::from_le_bytes([bytes[0], bytes[1]]),
            index: u16::from_le_bytes([bytes[2], bytes[3]]),
            total: u16::from_le_bytes([bytes[4], bytes[5]]),
        };
        Some((header, &bytes[FRAGMENT_HEADER_LEN..]))
    }
}

/// Split `payload` into wire-ready fragments, each at most
/// `max_fragment_payload` bytes of payload plus the header.
///
/// An empty payload still produces exactly one fragment (`total = 1`, empty
/// body) so that "send nothing" round-trips as "receive nothing" rather than
/// silently vanishing.
pub fn split(msg_id: u16, payload: &[u8], max_fragment_payload: usize) -> Result<Vec<Vec<u8>>> {
    if max_fragment_payload == 0 {
        return Err(BleError::Gatt(
            "fragment payload budget is zero — negotiated MTU is too small to carry data"
                .to_string(),
        ));
    }

    let total = if payload.is_empty() {
        1
    } else {
        payload.len().div_ceil(max_fragment_payload)
    };
    if total > MAX_FRAGMENTS {
        return Err(BleError::Gatt(format!(
            "message needs {total} fragments, exceeding the {MAX_FRAGMENTS}-fragment limit"
        )));
    }

    let mut fragments = Vec::with_capacity(total);
    for (index, chunk) in payload
        .chunks(max_fragment_payload)
        .chain(payload.is_empty().then_some(&[][..]))
        .enumerate()
    {
        let mut fragment = Vec::with_capacity(FRAGMENT_HEADER_LEN + chunk.len());
        FragmentHeader {
            msg_id,
            index: index as u16,
            total: total as u16,
        }
        .write_to(&mut fragment);
        fragment.extend_from_slice(chunk);
        fragments.push(fragment);
    }
    Ok(fragments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_through_the_wire_format() {
        let header = FragmentHeader {
            msg_id: 0xBEEF,
            index: 3,
            total: 9,
        };
        let mut buffer = Vec::new();
        header.write_to(&mut buffer);
        buffer.extend_from_slice(b"body");

        let (parsed, payload) = FragmentHeader::parse(&buffer).expect("parse");
        assert_eq!(parsed, header);
        assert_eq!(payload, b"body");
    }

    #[test]
    fn a_buffer_too_short_for_a_header_is_rejected_not_panicked_on() {
        for len in 0..FRAGMENT_HEADER_LEN {
            assert!(FragmentHeader::parse(&vec![0u8; len]).is_none());
        }
    }

    #[test]
    fn an_empty_payload_still_produces_one_fragment() {
        let fragments = split(1, b"", 100).expect("split");
        assert_eq!(fragments.len(), 1);
        let (header, payload) = FragmentHeader::parse(&fragments[0]).expect("parse");
        assert_eq!(header.total, 1);
        assert_eq!(header.index, 0);
        assert!(payload.is_empty());
    }

    #[test]
    fn a_payload_that_fits_exactly_does_not_gain_a_spurious_fragment() {
        // The classic off-by-one: len exactly == budget must stay one
        // fragment, and budget+1 must become two.
        let exact = split(1, &[0u8; 10], 10).expect("split");
        assert_eq!(exact.len(), 1);

        let over = split(1, &[0u8; 11], 10).expect("split");
        assert_eq!(over.len(), 2);
    }

    #[test]
    fn fragments_carry_sequential_indices_and_a_consistent_total() {
        let fragments = split(7, &[0u8; 25], 10).expect("split");
        assert_eq!(fragments.len(), 3);
        for (i, fragment) in fragments.iter().enumerate() {
            let (header, _) = FragmentHeader::parse(fragment).expect("parse");
            assert_eq!(header.msg_id, 7);
            assert_eq!(header.index, i as u16);
            assert_eq!(header.total, 3);
        }
    }

    #[test]
    fn reassembling_the_split_payload_reproduces_it_byte_for_byte() {
        let original: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let fragments = split(1, &original, 64).expect("split");
        let rejoined: Vec<u8> = fragments
            .iter()
            .flat_map(|f| FragmentHeader::parse(f).expect("parse").1.to_vec())
            .collect();
        assert_eq!(rejoined, original);
    }

    #[test]
    fn a_zero_byte_budget_is_an_error_rather_than_an_infinite_split() {
        assert!(split(1, b"data", 0).is_err());
    }
}
