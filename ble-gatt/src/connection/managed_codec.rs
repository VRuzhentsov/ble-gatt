//! Complete managed records. Datagram fragmentation remains a separate layer.
use super::peer_link::EndpointId;
use crate::{BleError, Result};

pub(super) const VERSION: u8 = 1;
pub(super) const CAPABILITY: u8 = 1;
pub(super) const DATA_HEADER: usize = 9;

/// The managed profile header and the endpoint it advertises.
pub(super) struct ManagedMetadata {
    pub endpoint: EndpointId,
}

impl ManagedMetadata {
    pub(super) fn complete_peer(
        advertisement: crate::advertisement::GattAdvertisement,
        service: crate::ServiceUuid,
        mut peer: crate::DiscoveredPeer,
    ) -> Result<(EndpointId, crate::DiscoveredPeer)> {
        let bytes = advertisement
            .service_data
            .get(&service)
            .ok_or_else(|| BleError::Protocol("missing managed profile".into()))?;
        let endpoint = Self::decode(bytes)?.endpoint;
        // The readable managed value is complete; removed fields must not survive in a radio cache.
        peer.manufacturer_data = advertisement.manufacturer_data;
        peer.service_data = advertisement.service_data;
        peer.name = advertisement.local_name;
        Ok((endpoint, peer))
    }
    /// The native spec of the managed profile: the readable value (`BG` v1
    /// plus the caller's fields), the advertisement header under the managed
    /// service, and the fast advertisement when the config names a company
    /// for it (ADR-0012 §2, §5).
    pub(super) fn service_spec(
        &self,
        config: &crate::datagram::DatagramConfig,
    ) -> Result<crate::GattServiceSpec> {
        use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
        if config.advertised_service_data.contains_key(&config.service) {
            return Err(BleError::Protocol(
                "managed service data is reserved".into(),
            ));
        }
        let mut spec = config.service_spec();
        let mut complete = GattAdvertisement::of(&spec);
        complete.service_data.insert(config.service, self.encode());
        let bytes = complete.encode();
        if GattAdvertisement::decode(&bytes)? != complete {
            return Err(BleError::Protocol(
                "managed advertisement exceeds readable limits".into(),
            ));
        }
        let hash = RadioRecord::metadata_hash(&bytes);
        spec.characteristics
            .iter_mut()
            .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .expect("datagram metadata characteristic")
            .initial_value = bytes;
        // The name stays readable only. The radio carries the compact records.
        spec.local_name = None;
        spec.service_data.insert(
            config.service,
            RadioRecord::Header {
                hash,
                tag: self.endpoint.tag(),
            }
            .encode(),
        );
        if let Some(company) = config.fast_advertisement_company {
            let tail = config
                .advertised_manufacturer_data
                .get(&company)
                .cloned()
                .unwrap_or_default();
            if tail.len() <= FAST_TAIL_MAX_LEN {
                spec.fast_advertisement = Some(crate::FastAdvertisement {
                    company_id: company,
                    data: RadioRecord::Fast {
                        hash,
                        endpoint: self.endpoint,
                        tail,
                    }
                    .encode(),
                });
            } else {
                log::warn!(
                    "advertise: no fast advertisement: manufacturer data {company:#06x} has {} bytes, at most {FAST_TAIL_MAX_LEN} fit",
                    tail.len()
                );
            }
        }
        spec.validate()?;
        Ok(spec)
    }

    /// The readable value a peer advertising `peer`'s radio fields and
    /// `endpoint` would serve, for comparing with a fast advertisement's hash.
    pub(super) fn radio_hash(
        endpoint: EndpointId,
        service: crate::ServiceUuid,
        peer: &crate::DiscoveredPeer,
    ) -> u32 {
        let mut value = crate::advertisement::GattAdvertisement {
            manufacturer_data: peer.manufacturer_data.clone(),
            service_data: peer.service_data.clone(),
            local_name: peer.name.clone(),
        };
        value
            .service_data
            .insert(service, Self { endpoint }.encode());
        RadioRecord::metadata_hash(&value.encode())
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![b'B', b'G', VERSION, CAPABILITY];
        bytes.extend_from_slice(&self.endpoint.0);
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 20 || bytes[..4] != [b'B', b'G', VERSION, CAPABILITY] {
            return Err(BleError::Protocol("unsupported managed profile".into()));
        }
        Ok(Self {
            endpoint: EndpointId(bytes[4..20].try_into().map_err(|_| Record::invalid())?),
        })
    }
}

impl EndpointId {
    /// The endpoint tag: the endpoint's first four bytes, as the
    /// advertisement header carries them.
    pub(super) fn tag(&self) -> [u8; 4] {
        [self.0[0], self.0[1], self.0[2], self.0[3]]
    }
}

/// Tag byte `1 1 1 1 1 K V V`: `K` the kind, `VV` the version less one.
/// Never valid UTF-8, so a record is never read as a name.
const RADIO_TAG_BASE: u8 = 0xF8;
const RADIO_KIND_FAST: u8 = 0b100;
const RADIO_VERSION: u8 = 1;
pub(super) const HEADER_LEN: usize = 9;
const FAST_FIXED_LEN: usize = 21;
/// Most caller bytes a fast advertisement carries after its fixed part.
pub(super) const FAST_TAIL_MAX_LEN: usize = crate::FAST_ADVERTISEMENT_MAX_LEN - FAST_FIXED_LEN;

/// The managed profile's compact radio records (ADR-0012 §5), versioned
/// apart from the readable `BG` v1 value. Every field is big-endian.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RadioRecord {
    /// `[0xF8][hash 4][endpoint tag 4]`: under the managed service as
    /// service data. Tells a scanner which readable value to read once.
    Header { hash: u32, tag: [u8; 4] },
    /// `[0xFC][hash 4][endpoint 16][tail 0..=6]`: the endpoint itself, and
    /// the caller's manufacturer data under the fast company as the tail.
    Fast {
        hash: u32,
        endpoint: EndpointId,
        tail: Vec<u8>,
    },
}

impl RadioRecord {
    /// FNV-1a, 32-bit (offset basis `0x811C9DC5`, prime `0x01000193`): a
    /// cache key for one readable value, not a security boundary
    /// (ADR-0003). Google Nearby truncates SHA-256 for the same field.
    pub(super) fn metadata_hash(bytes: &[u8]) -> u32 {
        bytes.iter().fold(0x811C_9DC5u32, |hash, byte| {
            (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
        })
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Header { hash, tag } => {
                let mut bytes = vec![RADIO_TAG_BASE | (RADIO_VERSION - 1)];
                bytes.extend_from_slice(&hash.to_be_bytes());
                bytes.extend_from_slice(tag);
                bytes
            }
            Self::Fast {
                hash,
                endpoint,
                tail,
            } => {
                let mut bytes = vec![RADIO_TAG_BASE | RADIO_KIND_FAST | (RADIO_VERSION - 1)];
                bytes.extend_from_slice(&hash.to_be_bytes());
                bytes.extend_from_slice(&endpoint.0);
                bytes.extend_from_slice(tail);
                bytes
            }
        }
    }

    /// `None` for anything but a well-formed record of a known version, so
    /// a scanner falls back to the readable value.
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let (&tag_byte, rest) = bytes.split_first()?;
        if tag_byte & RADIO_TAG_BASE != RADIO_TAG_BASE || (tag_byte & 0b11) + 1 != RADIO_VERSION {
            return None;
        }
        let hash = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
        if tag_byte & RADIO_KIND_FAST == 0 {
            (bytes.len() == HEADER_LEN).then(|| Self::Header {
                hash,
                tag: rest[4..8].try_into().expect("length checked"),
            })
        } else {
            (bytes.len() >= FAST_FIXED_LEN && bytes.len() <= crate::FAST_ADVERTISEMENT_MAX_LEN)
                .then(|| Self::Fast {
                    hash,
                    endpoint: EndpointId(rest[4..20].try_into().expect("length checked")),
                    tail: bytes[FAST_FIXED_LEN..].to_vec(),
                })
        }
    }

    /// The record a scan result carries for the managed `service`: a fast
    /// advertisement under `company` first, then service data under
    /// `service` (a fast one, for extended advertisements, or a header).
    pub(super) fn find(
        peer: &crate::DiscoveredPeer,
        service: crate::ServiceUuid,
        company: Option<u16>,
    ) -> Option<Self> {
        company
            .and_then(|company| peer.manufacturer_data.get(&company))
            .and_then(|bytes| Self::decode(bytes))
            .filter(|record| matches!(record, Self::Fast { .. }))
            .or_else(|| {
                peer.service_data
                    .get(&service)
                    .and_then(|bytes| Self::decode(bytes))
            })
    }

    /// `peer` as its advertiser described it: the record removed, and a fast
    /// advertisement's tail back as `manufacturer_data[company]` (removed
    /// when empty).
    pub(super) fn radio_fields(
        &self,
        mut peer: crate::DiscoveredPeer,
        service: crate::ServiceUuid,
        company: Option<u16>,
    ) -> crate::DiscoveredPeer {
        peer.service_data.remove(&service);
        if let (Self::Fast { tail, .. }, Some(company)) = (self, company) {
            if tail.is_empty() {
                peer.manufacturer_data.remove(&company);
            } else {
                peer.manufacturer_data.insert(company, tail.clone());
            }
        }
        peer
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Record {
    Request {
        sender: EndpointId,
        target: EndpointId,
        attempt: u64,
        nonce: i32,
    },
    Response {
        sender: EndpointId,
        target: EndpointId,
        attempt: u64,
    },
    Decision {
        attempt: u64,
        action: u8,
    },
    Data {
        attempt: u64,
        payload: Vec<u8>,
    },
    Disconnect {
        attempt: u64,
    },
    Ping {
        attempt: u64,
    },
    Pong {
        attempt: u64,
    },
}

impl Record {
    pub(super) fn invalid() -> BleError {
        BleError::Protocol("invalid managed record".into())
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Self::Request {
                sender,
                target,
                attempt,
                nonce,
            } => {
                bytes.extend_from_slice(&[1, VERSION, CAPABILITY]);
                bytes.extend_from_slice(&sender.0);
                bytes.extend_from_slice(&target.0);
                bytes.extend_from_slice(&attempt.to_be_bytes());
                bytes.extend_from_slice(&nonce.to_be_bytes());
            }
            Self::Response {
                sender,
                target,
                attempt,
            } => {
                bytes.push(2);
                bytes.extend_from_slice(&sender.0);
                bytes.extend_from_slice(&target.0);
                bytes.extend_from_slice(&attempt.to_be_bytes());
            }
            Self::Decision { attempt, action } => {
                bytes.push(3);
                bytes.extend_from_slice(&attempt.to_be_bytes());
                bytes.push(*action);
            }
            Self::Data { attempt, payload } => {
                bytes.push(4);
                bytes.extend_from_slice(&attempt.to_be_bytes());
                bytes.extend_from_slice(payload);
            }
            Self::Disconnect { attempt } | Self::Ping { attempt } | Self::Pong { attempt } => {
                bytes.push(match self {
                    Self::Disconnect { .. } => 5,
                    Self::Ping { .. } => 6,
                    _ => 7,
                });
                bytes.extend_from_slice(&attempt.to_be_bytes());
            }
        }
        bytes
    }

    pub(super) fn decode(bytes: &[u8], max_data: usize) -> Result<Self> {
        let read_attempt = |offset| -> Result<u64> {
            Ok(u64::from_be_bytes(
                bytes
                    .get(offset..offset + 8)
                    .ok_or_else(Self::invalid)?
                    .try_into()
                    .map_err(|_| Self::invalid())?,
            ))
        };
        match bytes.first() {
            Some(1) if bytes.len() == 47 && bytes[1..3] == [VERSION, CAPABILITY] => {
                Ok(Self::Request {
                    sender: EndpointId(bytes[3..19].try_into().map_err(|_| Self::invalid())?),
                    target: EndpointId(bytes[19..35].try_into().map_err(|_| Self::invalid())?),
                    attempt: read_attempt(35)?,
                    nonce: i32::from_be_bytes(
                        bytes[43..47].try_into().map_err(|_| Self::invalid())?,
                    ),
                })
            }
            Some(2) if bytes.len() == 41 => Ok(Self::Response {
                sender: EndpointId(bytes[1..17].try_into().map_err(|_| Self::invalid())?),
                target: EndpointId(bytes[17..33].try_into().map_err(|_| Self::invalid())?),
                attempt: read_attempt(33)?,
            }),
            Some(3) if bytes.len() == 10 && bytes[9] <= 2 => Ok(Self::Decision {
                attempt: read_attempt(1)?,
                action: bytes[9],
            }),
            Some(4) if bytes.len() >= DATA_HEADER && bytes.len() - DATA_HEADER <= max_data => {
                Ok(Self::Data {
                    attempt: read_attempt(1)?,
                    payload: bytes[DATA_HEADER..].to_vec(),
                })
            }
            Some(5) if bytes.len() == 9 => Ok(Self::Disconnect {
                attempt: read_attempt(1)?,
            }),
            Some(6) if bytes.len() == 9 => Ok(Self::Ping {
                attempt: read_attempt(1)?,
            }),
            Some(7) if bytes.len() == 9 => Ok(Self::Pong {
                attempt: read_attempt(1)?,
            }),
            _ => Err(Self::invalid()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datagram::DatagramConfig;
    use crate::{CharacteristicUuid, DiscoveredPeer, PeerAddress, ServiceUuid};

    const FINI: u16 = 0xFFFF;

    fn config() -> DatagramConfig {
        let mut config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(11)),
            CharacteristicUuid(uuid::Uuid::from_u128(12)),
        );
        config
            .advertised_manufacturer_data
            .insert(FINI, vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]);
        config.advertised_local_name = Some("Pixel".into());
        config.fast_advertisement_company = Some(FINI);
        config
    }

    fn metadata() -> ManagedMetadata {
        ManagedMetadata {
            endpoint: EndpointId([7; 16]),
        }
    }

    fn header_hash(spec: &crate::GattServiceSpec) -> u32 {
        match RadioRecord::decode(&spec.service_data[&spec.uuid]) {
            Some(RadioRecord::Header { hash, .. }) => hash,
            other => panic!("not a header: {other:?}"),
        }
    }

    #[test]
    fn the_metadata_hash_is_fnv_1a_32() {
        assert_eq!(RadioRecord::metadata_hash(b""), 0x811C_9DC5);
        assert_eq!(RadioRecord::metadata_hash(b"a"), 0xE40C_292C);
        assert_eq!(RadioRecord::metadata_hash(b"foobar"), 0xBF9C_F968);
    }

    #[test]
    fn header_and_fast_records_round_trip() {
        let header = RadioRecord::Header {
            hash: 0x0102_0304,
            tag: [9, 8, 7, 6],
        };
        let bytes = header.encode();
        assert_eq!(bytes, [0xF8, 1, 2, 3, 4, 9, 8, 7, 6]);
        assert_eq!(RadioRecord::decode(&bytes), Some(header));
        let fast = RadioRecord::Fast {
            hash: 0xA0B0_C0D0,
            endpoint: EndpointId([5; 16]),
            tail: vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD],
        };
        let bytes = fast.encode();
        assert_eq!(bytes.len(), 26);
        assert_eq!(bytes[0], 0xFC);
        assert_eq!(RadioRecord::decode(&bytes), Some(fast));
        let empty = RadioRecord::Fast {
            hash: 1,
            endpoint: EndpointId([5; 16]),
            tail: Vec::new(),
        };
        assert_eq!(RadioRecord::decode(&empty.encode()), Some(empty));
    }

    #[test]
    fn a_record_is_never_a_name() {
        for record in [
            RadioRecord::Header {
                hash: 0x4142_4344,
                tag: *b"Name",
            },
            RadioRecord::Fast {
                hash: 0x4142_4344,
                endpoint: EndpointId(*b"abcdefghijklmnop"),
                tail: b"ok".to_vec(),
            },
        ] {
            assert!(String::from_utf8(record.encode()).is_err());
        }
    }

    #[test]
    fn unknown_versions_and_bad_lengths_are_ignored() {
        let mut header = RadioRecord::Header {
            hash: 1,
            tag: [0; 4],
        }
        .encode();
        header[0] = 0xF9; // version 2
        assert_eq!(RadioRecord::decode(&header), None);
        header[0] = 0xF8;
        assert_eq!(RadioRecord::decode(&header[..8]), None);
        let mut fast = RadioRecord::Fast {
            hash: 1,
            endpoint: EndpointId([0; 16]),
            tail: vec![0; FAST_TAIL_MAX_LEN],
        }
        .encode();
        assert!(RadioRecord::decode(&fast).is_some());
        fast.push(0);
        assert_eq!(RadioRecord::decode(&fast), None, "a tail over 6 bytes");
        assert_eq!(RadioRecord::decode(b"BG\x01\x01"), None);
    }

    #[test]
    fn a_tail_over_six_bytes_gives_no_fast_record() {
        let mut config = config();
        config
            .advertised_manufacturer_data
            .insert(FINI, vec![0; FAST_TAIL_MAX_LEN + 1]);
        let spec = metadata().service_spec(&config).unwrap();
        assert!(spec.fast_advertisement.is_none());
        assert_eq!(spec.manufacturer_data[&FINI].len(), 7, "kept as it was");
        assert_eq!(spec.service_data[&spec.uuid].len(), HEADER_LEN);
    }

    #[test]
    fn the_spec_carries_header_and_fast_records_of_the_readable_value() {
        let config = config();
        let spec = metadata().service_spec(&config).unwrap();
        let readable = &spec
            .characteristics
            .iter()
            .find(|c| c.uuid == crate::advertisement::GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value;
        let hash = RadioRecord::metadata_hash(readable);
        assert_eq!(header_hash(&spec), hash);
        let fast = spec.fast_advertisement.as_ref().unwrap();
        assert_eq!(fast.company_id, FINI);
        assert_eq!(
            RadioRecord::decode(&fast.data),
            Some(RadioRecord::Fast {
                hash,
                endpoint: metadata().endpoint,
                tail: vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD],
            })
        );
        assert_eq!(spec.local_name, None, "the name stays readable");
        // A scanner rebuilds the readable value from the radio and the name.
        let radio = DiscoveredPeer {
            address: PeerAddress("a".into()),
            name: Some("Pixel".into()),
            services: vec![config.service],
            manufacturer_data: config.advertised_manufacturer_data.clone(),
            service_data: Default::default(),
            rssi: Some(-40),
        };
        assert_eq!(
            ManagedMetadata::radio_hash(metadata().endpoint, config.service, &radio),
            hash
        );
        let nameless = DiscoveredPeer {
            name: None,
            ..radio
        };
        assert_ne!(
            ManagedMetadata::radio_hash(metadata().endpoint, config.service, &nameless),
            hash,
            "without the name the radio is incomplete"
        );
    }

    #[test]
    fn the_hash_changes_with_the_name_or_a_flag() {
        let base = header_hash(&metadata().service_spec(&config()).unwrap());
        let mut renamed = config();
        renamed.advertised_local_name = Some("Pixel 9".into());
        assert_ne!(
            header_hash(&metadata().service_spec(&renamed).unwrap()),
            base
        );
        let mut flagged = config();
        flagged
            .advertised_manufacturer_data
            .insert(FINI, vec![0x00, 0xAA, 0xBB, 0xCC, 0xDD]);
        assert_ne!(
            header_hash(&metadata().service_spec(&flagged).unwrap()),
            base
        );
        assert_eq!(
            header_hash(&metadata().service_spec(&config()).unwrap()),
            base,
            "the same value, the same hash"
        );
    }

    #[test]
    fn a_scanner_restores_the_folded_manufacturer_data() {
        let spec = metadata().service_spec(&config()).unwrap();
        let fast = spec.fast_advertisement.unwrap();
        let mut peer = DiscoveredPeer {
            address: PeerAddress("a".into()),
            name: None,
            services: vec![spec.uuid],
            manufacturer_data: [(FINI, fast.data)].into(),
            service_data: Default::default(),
            rssi: None,
        };
        let record = RadioRecord::find(&peer, spec.uuid, Some(FINI)).unwrap();
        peer = record.radio_fields(peer, spec.uuid, Some(FINI));
        assert_eq!(
            peer.manufacturer_data[&FINI],
            vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]
        );
        // Without the company configured the scanner looks at service data only.
        assert_eq!(
            RadioRecord::find(&peer, spec.uuid, None),
            None,
            "the restored entry is no record"
        );
    }
}
