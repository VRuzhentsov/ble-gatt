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
        }
    }

    /// Refuses a spec whose name would collide with its own service data:
    /// both would travel as service data under `uuid`.
    pub fn validate(&self) -> crate::Result<()> {
        if self.local_name.is_some() && self.service_data.contains_key(&self.uuid) {
            return Err(crate::BleError::Gatt(format!(
                "local_name and service_data under the advertised service {} cannot both be set: \
                 the name is sent as that service data",
                self.uuid.0
            )));
        }
        Ok(())
    }

    /// Whether `other` describes the same GATT service: the same UUID and
    /// characteristics, whatever its advertisement data. What
    /// `Driver::update_advertisement` requires of its spec.
    pub(crate) fn same_service(&self, other: &GattServiceSpec) -> bool {
        self.uuid == other.uuid
            && self.characteristics.len() == other.characteristics.len()
            && self.characteristics.iter().zip(&other.characteristics).all(|(a, b)| {
                a.uuid == b.uuid
                    && a.readable == b.readable
                    && a.writable == b.writable
                    && a.notifiable == b.notifiable
                    && a.initial_value == b.initial_value
            })
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
    if name.len() <= max {
        return name;
    }
    let mut end = max;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
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
    service: ServiceUuid, service_data: &BTreeMap<ServiceUuid, Vec<u8>>, platform_name: Option<String>,
) -> Option<String> {
    service_data
        .get(&service)
        .filter(|bytes| !bytes.is_empty())
        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
        .or(platform_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_truncated_on_a_character_boundary() {
        assert_eq!(truncate_name("short", NAME_SERVICE_DATA_MAX_LEN), "short");
        assert_eq!(truncate_name("Living room desktop", NAME_SERVICE_DATA_MAX_LEN), "Living room d");
        // "é" is two bytes; cutting at 13 would split the one at 12..14.
        assert_eq!(truncate_name("aaaaaaaaaaaaé", NAME_SERVICE_DATA_MAX_LEN), "aaaaaaaaaaaa");
    }

    #[test]
    fn the_service_data_name_wins_over_the_platform_name() {
        let service = ServiceUuid(uuid::Uuid::from_u128(1));
        let mut data = BTreeMap::new();
        assert_eq!(advertised_name(service, &data, Some("cached".into())), Some("cached".into()));
        data.insert(service, b"Pixel".to_vec());
        assert_eq!(advertised_name(service, &data, Some("cached".into())), Some("Pixel".into()));
        data.insert(service, vec![0xFF, 0xFE]);
        assert_eq!(advertised_name(service, &data, None), None, "not UTF-8: not a name");
    }
}
