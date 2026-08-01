use std::collections::BTreeMap;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceUuid(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CharacteristicUuid(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Central,
    Peripheral,
}

/// What a `Backend` can actually do on this device, discovered at runtime
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

/// Opaque, backend-specific peer identifier (a BlueZ D-Bus device address on
/// Linux, a Bluetooth device address on Android, etc). Callers must not
/// parse or format it themselves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerAddress(pub String);

#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    pub address: PeerAddress,
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
    /// Received signal strength in dBm, when the backend reports it. Useful
    /// as a proximity gate (ignore peers below a threshold) — `None` when
    /// the platform doesn't surface it.
    pub rssi: Option<i16>,
}

/// How a characteristic write should be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

#[derive(Debug, Clone)]
pub enum GattEvent {
    Connected {
        peer: PeerAddress,
        /// The role *this* device played. `Central` means we dialled out;
        /// `Peripheral` means a central connected to our GATT server.
        /// Without it the two are indistinguishable, so a backend used in
        /// both roles at once cannot tell an outbound connection from an
        /// inbound one.
        local_role: Role,
        /// Identifies *which* connection to `peer` this is.
        ///
        /// An address names a peer, not a session, so a peer that drops and
        /// reconnects quickly produces events that are indistinguishable by
        /// address alone — and a lifecycle event queued from the old
        /// connection would otherwise be applied to the new one, tearing
        /// down a link that is working. `None` from backends that cannot
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
}

#[derive(Debug, Clone)]
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
pub struct GattServiceSpec {
    pub uuid: ServiceUuid,
    pub characteristics: Vec<GattCharacteristicSpec>,
}
