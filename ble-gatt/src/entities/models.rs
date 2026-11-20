use std::collections::BTreeMap;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct ServiceUuid(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct CharacteristicUuid(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum Role {
    Central,
    Peripheral,
}

/// What a `Driver` can actually do on this device, discovered at runtime
/// (not assumed from the target OS alone) — e.g. an Android device whose
/// chipset/driver doesn't expose GATT-server APIs still reports
/// `peripheral: false` here rather than failing opaquely later. Consumers
/// use this to pick a `Role` before committing to it (see the plan's
/// deterministic-role-assignment-with-fallback decision).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapabilityReport {
    pub central: bool,
    pub peripheral: bool,
}

/// Opaque, driver-specific peer identifier (a BlueZ D-Bus device address on
/// Linux, a Bluetooth device address on Android, etc). Callers must not
/// parse or format it themselves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct PeerAddress(pub String);

#[derive(Debug, Clone)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct DiscoveredPeer {
    pub address: PeerAddress,
    /// The peer's advertised name: its [`GattServiceSpec::local_name`] when
    /// it set one (read from the scan response on every platform; see
    /// [`advertised_name`]), otherwise the name the platform reports. A
    /// scan reports a peer again when this changes, since the scan
    /// response can arrive after the first advertisement.
    pub name: Option<String>,
    pub services: Vec<ServiceUuid>,
    /// Manufacturer-specific advertisement data, keyed by the Bluetooth SIG
    /// assigned company identifier. Vendor devices routinely carry their
    /// real identity here (a serial number, a device EUI) rather than in the
    /// BLE name, so this is often the only way to tell two units of the same
    /// product apart before connecting.
    pub manufacturer_data: BTreeMap<u16, Vec<u8>>,
    /// Service-specific advertisement data, keyed by service UUID. The
    /// conventional place to publish a small identity payload alongside your
    /// own service UUID — it lets a scanner recognise a *specific* peer
    /// without paying for a connection first.
    pub service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    /// Received signal strength in dBm, when the driver reports it. Useful
    /// as a proximity gate (ignore peers below a threshold) — `None` when
    /// the platform doesn't surface it.
    pub rssi: Option<i16>,
}

impl DiscoveredPeer {
    pub(crate) fn advertised_name(
        service: ServiceUuid,
        service_data: &BTreeMap<ServiceUuid, Vec<u8>>,
        platform_name: Option<String>,
    ) -> Option<String> {
        service_data
            .get(&service)
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .or(platform_name)
    }
}

/// How a characteristic write should be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum WriteType {
    /// ATT Write Request: the peer acknowledges each write. Slower, but you
    /// learn about failures. The right default for command/response
    /// protocols.
    #[default]
    WithResponse,
    /// ATT Write Command: fire-and-forget, no acknowledgement. Substantially
    /// higher throughput — the reason bulk transfers (firmware upload) use
    /// it — at the cost of the peer silently dropping writes it can't keep
    /// up with. Callers taking this path are responsible for their own
    /// pacing and integrity checking.
    WithoutResponse,
}

/// The latency/power trade-off a central asks the platform for on one live
/// link — see [`GattConnection::request_connection_priority`].
///
/// Android only (`BluetoothGatt.requestConnectionPriority`); BlueZ exposes
/// no per-connection interval control over D-Bus, so the Linux driver
/// returns [`BleError::Unsupported`](crate::BleError::Unsupported).
///
/// [`GattConnection::request_connection_priority`]: crate::GattConnection::request_connection_priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum ConnectionPriority {
    /// Long connection interval: fewer radio wakeups and less battery, at
    /// the cost of latency and throughput. For an idle link kept open only
    /// to stay reachable.
    LowPower,
    /// The platform default.
    #[default]
    Balanced,
    /// Short connection interval: lowest latency and highest throughput,
    /// at the cost of battery. For bursts of traffic or while a user is
    /// actively watching; switch back when that ends.
    High,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum GattEvent {
    Connected {
        peer: PeerAddress,
        /// The role *this* device played. `Central` means we dialled out;
        /// `Peripheral` means a central connected to our GATT server.
        /// Without it the two are indistinguishable, so a driver used in
        /// both roles at once cannot tell an outbound connection from an
        /// inbound one.
        local_role: Role,
        /// Identifies *which* connection to `peer` this is.
        ///
        /// An address names a peer, not a session, so a peer that drops and
        /// reconnects quickly produces events that are indistinguishable by
        /// address alone — and a lifecycle event queued from the old
        /// connection would otherwise be applied to the new one, tearing
        /// down a link that is working. `None` from drivers that cannot
        /// distinguish sessions; consumers then fall back to address
        /// matching.
        session: Option<u64>,
    },
    Disconnected {
        peer: PeerAddress,
        local_role: Role,
        /// See [`GattEvent::Connected::session`]. Matching on this is what
        /// stops a stale loss event terminating its own replacement.
        session: Option<u64>,
    },
    CharacteristicWritten {
        peer: PeerAddress,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    },
    /// This subscriber fell behind and `dropped` events were discarded
    /// before it could read them.
    ///
    /// Reported rather than swallowed because the lost events are not all
    /// harmless: an acknowledged `CharacteristicWritten` disappearing here
    /// removes a fragment the sender was told had arrived, so a consumer
    /// treating the remaining events as a complete record would wait
    /// forever for a message that can no longer be assembled. Which peers
    /// were affected is unknowable — the events are simply gone — so a
    /// consumer must treat every session it is tracking as suspect.
    Lagged { dropped: u64 },
    /// The platform BLE radio's usability changed.
    ///
    /// Emitted when the adapter is toggled (`RadioStatus::Off` / `On`) or
    /// found to be absent for this device's needs (`Unsupported`). A
    /// transition to anything other than `On` means every link this driver
    /// held is gone — the driver also emits the per-link `Disconnected`
    /// events, but a consumer watching connectivity as a whole keys off
    /// this. `PeerLink` turns it into its per-peer `Unavailable` status and
    /// stops a consumer polling `capabilities()` to guess.
    RadioChanged { status: RadioStatus },
}

/// One connection's state, published by `connection::Connection::state`
/// (ADR-0007 D5). A `Connection` exists only once connected, so there is no
/// "connecting" state here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    /// The peer went away, the radio stopped, or this side disconnected.
    /// Final: a new connection is a new `Connection`.
    Disconnected,
}

impl ConnectionState {
    pub fn is_connected(self) -> bool {
        self == Self::Connected
    }

    pub fn is_disconnected(self) -> bool {
        self == Self::Disconnected
    }
}

/// Whether the platform BLE radio is usable. See
/// [`GattEvent::RadioChanged`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum RadioStatus {
    /// Usable.
    On,
    /// The adapter exists but is switched off. A later `On` recovers every
    /// link to a tracked peer without the consumer doing anything.
    Off,
    /// No usable adapter for this device's role, and not expected to
    /// appear. Distinct from `Off` so a consumer renders it as a permanent
    /// state rather than a temporary one.
    Unsupported,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct GattCharacteristicSpec {
    pub uuid: CharacteristicUuid,
    pub readable: bool,
    pub writable: bool,
    pub notifiable: bool,
    /// Only meaningful when `readable` is true: the value returned to a
    /// central's read request. Peripheral role is a single local GATT
    /// server, so this is static state, not per-peer.
    pub initial_value: Vec<u8>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct GattServiceSpec {
    pub uuid: ServiceUuid,
    pub characteristics: Vec<GattCharacteristicSpec>,
    /// Manufacturer-specific advertisement data, keyed by the Bluetooth SIG
    /// assigned company identifier — the outbound mirror of
    /// `DiscoveredPeer::manufacturer_data`. Consumers use this for small,
    /// pre-connection identity payloads (e.g. a discoverability flag) a
    /// scanner can read without paying for a connection first. Empty by
    /// default (via `GattServiceSpec::new`) — legacy advertisements are
    /// capped at 31 bytes total, most of it already spent on `uuid` itself,
    /// so callers should keep this small.
    pub manufacturer_data: BTreeMap<u16, Vec<u8>>,
    /// Service-specific advertisement data, keyed by service UUID — the
    /// outbound mirror of `DiscoveredPeer::service_data`. Same size
    /// constraints as `manufacturer_data`.
    pub service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    /// A short name (see `docs/glossary.md`: local name) sent in the scan
    /// response, never in the advertising packet, so it costs none of the
    /// 31 bytes `uuid` and `manufacturer_data` share. Scanners read it into
    /// `DiscoveredPeer::name` without connecting.
    ///
    /// Truncated (on a UTF-8 boundary) to what the platform's scan response
    /// holds: [`NAME_SERVICE_DATA_MAX_LEN`] bytes on Android, which sends it
    /// as service data under `uuid` because it cannot set a name per
    /// advertisement; [`LOCAL_NAME_MAX_LEN`] on Linux, which sends BlueZ's
    /// `LocalName`. Keep it within the smaller to look the same everywhere.
    /// A spec with a name may not also put `service_data` under its own
    /// `uuid`: that is where the name travels.
    pub local_name: Option<String>,
    /// A record for drivers that can put manufacturer data in a packet of its
    /// own (the Android scan response; the mock). Placed where it fits;
    /// where placed it replaces `manufacturer_data[company_id]`, and a
    /// scanner restores that entry from it. `None` by default. The managed
    /// profile fills it (ADR-0012 §2).
    #[cfg_attr(feature = "mock-broker", serde(default))]
    pub fast_advertisement: Option<FastAdvertisement>,
    /// How often the advertisement is sent: the latency/power trade-off of
    /// discovery by others. `Balanced` by default.
    #[cfg_attr(feature = "mock-broker", serde(default))]
    pub advertising_mode: AdvertisingMode,
}

/// Most bytes of [`FastAdvertisement::data`]: one manufacturer data AD
/// structure alone in a legacy packet holds 31, less the 2-byte AD header
/// and the 2-byte company identifier. Google Nearby's fast advertisement
/// has the same limit (`ble_advertisement.h`, "leaves 27").
pub const FAST_ADVERTISEMENT_MAX_LEN: usize = 27;

/// Manufacturer data a driver places in a packet of its own when it can
/// (see [`GattServiceSpec::fast_advertisement`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub struct FastAdvertisement {
    /// The company identifier the record travels under.
    pub company_id: u16,
    /// At most [`FAST_ADVERTISEMENT_MAX_LEN`] bytes.
    pub data: Vec<u8>,
}

/// How often a peripheral advertises. The names mirror Android's
/// `AdvertiseSettings` modes, as [`ConnectionPriority`] mirrors
/// `requestConnectionPriority`.
///
/// Android sets the mode; BlueZ gets the matching interval (LowLatency
/// 100 ms, Balanced 250 ms, LowPower 1000 ms); Windows has no API for it
/// and ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum AdvertisingMode {
    /// About once a second: least battery, slowest to be found.
    LowPower,
    /// About four times a second. The platform default on Android.
    #[default]
    Balanced,
    /// About ten times a second: fastest to be found, most battery. For a
    /// short time while a user is waiting, for example an open Add Device.
    LowLatency,
}

impl AdvertisingMode {
    /// The advertising interval BlueZ is asked for.
    pub fn interval(self) -> std::time::Duration {
        std::time::Duration::from_millis(match self {
            Self::LowPower => 1000,
            Self::Balanced => 250,
            Self::LowLatency => 100,
        })
    }
}

/// Whether `manufacturer_data` holds a managed fast advertisement (first
/// byte `0b1111_11VV`, 21 to 27 bytes; ADR-0012 §5). Scanners let such a
/// result through even without the service UUID: a Windows peer publishes it
/// in an advertisement of its own, next to its GATT provider's.
pub fn carries_fast_advertisement<'a>(
    mut manufacturer_data: impl Iterator<Item = &'a Vec<u8>>,
) -> bool {
    manufacturer_data.any(|data| {
        (21..=FAST_ADVERTISEMENT_MAX_LEN).contains(&data.len()) && data[0] & 0xFC == 0xFC
    })
}

/// Bytes of one legacy advertising packet (Core Specification Vol 6 Part B
/// 2.3.1.1: 31 bytes of advertising data).
pub const LEGACY_ADVERTISING_DATA_LEN: usize = 31;

/// Where a legacy advertiser with a scan response puts each part of a spec:
/// the Android placement (ADR-0012 §2). The mock uses it too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPlacement {
    /// Manufacturer data in the advertising packet.
    pub manufacturer_data: BTreeMap<u16, Vec<u8>>,
    /// Service data in the advertising packet: every entry except the one
    /// under the spec's own `uuid`.
    pub service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    /// What the scan response carries.
    pub scan_response: ScanResponse,
}

/// The one thing a legacy scan response carries (31 bytes hold one of them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanResponse {
    /// The fast advertisement, as manufacturer data.
    Fast(FastAdvertisement),
    /// Service data under the spec's `uuid`: the caller's (the managed
    /// advertisement header) or the local name.
    ServiceData(Vec<u8>),
    None,
}

impl LegacyPlacement {
    /// Bytes of the advertising packet: see
    /// [`GattServiceSpec::legacy_advertising_len`].
    pub fn advertising_len(&self) -> usize {
        GattServiceSpec::legacy_len(&self.manufacturer_data, &self.service_data)
    }

    /// Bytes of the scan response.
    pub fn scan_response_len(&self) -> usize {
        match &self.scan_response {
            ScanResponse::Fast(fast) => 2 + 2 + fast.data.len(),
            ScanResponse::ServiceData(data) => 2 + 16 + data.len(),
            ScanResponse::None => 0,
        }
    }
}

impl GattServiceSpec {
    /// Constructs a spec with no advertisement data beyond the service UUID
    /// itself — the shape every caller needed before `manufacturer_data`/
    /// `service_data` existed. Prefer this over the struct literal unless
    /// you actually need those fields, both to stay terse and so a future
    /// field addition here doesn't require touching every call site again.
    pub fn new(uuid: ServiceUuid, characteristics: Vec<GattCharacteristicSpec>) -> Self {
        Self {
            uuid,
            characteristics,
            manufacturer_data: BTreeMap::new(),
            service_data: BTreeMap::new(),
            local_name: None,
            fast_advertisement: None,
            advertising_mode: AdvertisingMode::Balanced,
        }
    }

    /// Refuses a spec whose name would collide with its own service data:
    /// both would travel as service data under `uuid`. Refuses a fast
    /// advertisement longer than [`FAST_ADVERTISEMENT_MAX_LEN`].
    pub fn validate(&self) -> crate::Result<()> {
        if self.local_name.is_some() && self.service_data.contains_key(&self.uuid) {
            return Err(crate::BleError::Gatt(format!(
                "local_name and service_data under the advertised service {} cannot both be set: \
                 the name is sent as that service data",
                self.uuid.0
            )));
        }
        if let Some(fast) = &self.fast_advertisement {
            if fast.data.len() > FAST_ADVERTISEMENT_MAX_LEN {
                return Err(crate::BleError::Gatt(format!(
                    "fast advertisement of {} bytes exceeds {FAST_ADVERTISEMENT_MAX_LEN}",
                    fast.data.len()
                )));
            }
        }
        Ok(())
    }

    /// Bytes this spec's advertisement data takes in one legacy advertising
    /// packet: flags (3), the 128-bit service UUID list (18), each
    /// manufacturer data AD structure (4 + data) and each 128-bit service
    /// data AD structure (18 + data). The name and the fast advertisement
    /// are not counted: they travel in the scan response. At most
    /// [`LEGACY_ADVERTISING_DATA_LEN`] fits.
    pub fn legacy_advertising_len(&self) -> usize {
        Self::legacy_len(&self.manufacturer_data, &self.service_data)
    }

    fn legacy_len(
        manufacturer_data: &BTreeMap<u16, Vec<u8>>,
        service_data: &BTreeMap<ServiceUuid, Vec<u8>>,
    ) -> usize {
        3 + 18
            + manufacturer_data
                .values()
                .map(|value| 4 + value.len())
                .sum::<usize>()
            + service_data
                .values()
                .map(|value| 18 + value.len())
                .sum::<usize>()
    }

    /// The Android placement of this spec (ADR-0012 §2): the fast
    /// advertisement, if any, goes in the scan response and replaces
    /// `manufacturer_data[company_id]`; otherwise the scan response carries
    /// the service data under `uuid`, otherwise the local name (truncated to
    /// [`NAME_SERVICE_DATA_MAX_LEN`]). `with_fast: false` is the placement
    /// without the fast advertisement, for an advertiser that refused it.
    pub fn legacy_placement(&self, with_fast: bool) -> LegacyPlacement {
        let mut manufacturer_data = self.manufacturer_data.clone();
        let mut service_data = self.service_data.clone();
        let own = service_data.remove(&self.uuid);
        let fast = self
            .fast_advertisement
            .as_ref()
            .filter(|fast| with_fast && fast.data.len() <= FAST_ADVERTISEMENT_MAX_LEN);
        let scan_response = if let Some(fast) = fast {
            manufacturer_data.remove(&fast.company_id);
            ScanResponse::Fast(fast.clone())
        } else if let Some(own) = own {
            ScanResponse::ServiceData(own)
        } else if let Some(name) = self.local_name.as_deref().filter(|name| !name.is_empty()) {
            ScanResponse::ServiceData(
                truncate_name(name, NAME_SERVICE_DATA_MAX_LEN)
                    .as_bytes()
                    .to_vec(),
            )
        } else {
            ScanResponse::None
        };
        LegacyPlacement {
            manufacturer_data,
            service_data,
            scan_response,
        }
    }

    /// Compare service UUIDs and characteristic properties in their listed order, as `Driver::update_advertisement` requires.
    /// Ignore advertisement data and characteristic values, which can change while serving (`Driver::set_characteristic_value`, ADR-0012).
    pub(crate) fn same_service(&self, other: &GattServiceSpec) -> bool {
        self.uuid == other.uuid
            && self.characteristics.len() == other.characteristics.len()
            && self
                .characteristics
                .iter()
                .zip(&other.characteristics)
                .all(|(current, proposed)| {
                    current.uuid == proposed.uuid
                        && current.readable == proposed.readable
                        && current.writable == proposed.writable
                        && current.notifiable == proposed.notifiable
                })
    }
    pub(crate) fn truncate_name(name: &str, max: usize) -> &str {
        if name.len() <= max {
            return name;
        }
        let mut end = max;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        &name[..end]
    }
}

/// Bytes of name a scan response holds as service data under a 128-bit
/// service UUID: 31, less the 2-byte AD header and the 16-byte UUID.
pub const NAME_SERVICE_DATA_MAX_LEN: usize = 13;

/// Bytes of name a scan response holds as a Complete Local Name AD
/// structure: 31, less the 2-byte AD header.
pub const LOCAL_NAME_MAX_LEN: usize = 29;

/// `name` cut to at most `max` bytes without splitting a UTF-8 character.
pub fn truncate_name(name: &str, max: usize) -> &str {
    GattServiceSpec::truncate_name(name, max)
}

/// The name a scanner reports for a peer advertising `service`: the
/// service data under `service`, when it is non-empty UTF-8 (how an Android
/// peer sends its [`GattServiceSpec::local_name`]), otherwise the name the
/// platform read from the advertisement.
///
/// The service data wins because the platform's name can be stale: BlueZ
/// keeps the name it read over GATT the last time it connected to the
/// device (a phone's system name), and reports that until the scan
/// response arrives.
pub fn advertised_name(
    service: ServiceUuid,
    service_data: &BTreeMap<ServiceUuid, Vec<u8>>,
    platform_name: Option<String>,
) -> Option<String> {
    DiscoveredPeer::advertised_name(service, service_data, platform_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_truncated_on_a_character_boundary() {
        assert_eq!(truncate_name("short", NAME_SERVICE_DATA_MAX_LEN), "short");
        assert_eq!(
            truncate_name("Living room desktop", NAME_SERVICE_DATA_MAX_LEN),
            "Living room d"
        );
        // "é" is two bytes; cutting at 13 would split the one at 12..14.
        assert_eq!(
            truncate_name("aaaaaaaaaaaaé", NAME_SERVICE_DATA_MAX_LEN),
            "aaaaaaaaaaaa"
        );
    }

    fn fini_shaped_spec() -> GattServiceSpec {
        let mut spec = GattServiceSpec::new(ServiceUuid(uuid::Uuid::from_u128(1)), vec![]);
        spec.manufacturer_data
            .insert(0xFFFF, vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]);
        spec
    }

    #[test]
    fn the_legacy_length_counts_flags_uuid_and_each_structure() {
        let mut spec = fini_shaped_spec();
        // flags 3 + UUID list 18 + manufacturer data 2 + 2 + 5.
        assert_eq!(spec.legacy_advertising_len(), 30);
        spec.service_data.insert(spec.uuid, vec![0xF8; 9]);
        // + service data 2 + 16 + 9: the header does not fit next to it.
        assert_eq!(spec.legacy_advertising_len(), 57);
        assert!(spec.legacy_advertising_len() > LEGACY_ADVERTISING_DATA_LEN);
    }

    #[test]
    fn the_android_placement_moves_the_fast_record_to_the_scan_response() {
        let mut spec = fini_shaped_spec();
        spec.service_data.insert(spec.uuid, vec![0xF8; 9]);
        spec.fast_advertisement = Some(FastAdvertisement {
            company_id: 0xFFFF,
            data: vec![0xFC; 26],
        });
        let placement = spec.legacy_placement(true);
        assert!(
            placement.manufacturer_data.is_empty(),
            "folded into the record"
        );
        assert!(placement.service_data.is_empty(), "superseded header");
        assert_eq!(placement.advertising_len(), 21);
        assert_eq!(placement.scan_response_len(), 30);
        assert!(placement.advertising_len() <= LEGACY_ADVERTISING_DATA_LEN);

        let fallback = spec.legacy_placement(false);
        assert_eq!(fallback.advertising_len(), 30);
        assert_eq!(
            fallback.scan_response,
            ScanResponse::ServiceData(vec![0xF8; 9])
        );
        assert_eq!(fallback.scan_response_len(), 27);
    }

    #[test]
    fn an_oversized_fast_advertisement_is_refused() {
        let mut spec = fini_shaped_spec();
        spec.fast_advertisement = Some(FastAdvertisement {
            company_id: 0xFFFF,
            data: vec![0; FAST_ADVERTISEMENT_MAX_LEN + 1],
        });
        assert!(spec.validate().is_err());
        spec.fast_advertisement.as_mut().unwrap().data.pop();
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn the_service_data_name_wins_over_the_platform_name() {
        let service = ServiceUuid(uuid::Uuid::from_u128(1));
        let mut data = BTreeMap::new();
        assert_eq!(
            advertised_name(service, &data, Some("cached".into())),
            Some("cached".into())
        );
        data.insert(service, b"Phone".to_vec());
        assert_eq!(
            advertised_name(service, &data, Some("cached".into())),
            Some("Phone".into())
        );
        data.insert(service, vec![0xFF, 0xFE]);
        assert_eq!(
            advertised_name(service, &data, None),
            None,
            "not UTF-8: not a name"
        );
    }
}
