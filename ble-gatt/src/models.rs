use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
}

#[derive(Debug, Clone)]
pub enum GattEvent {
    Connected {
        peer: PeerAddress,
    },
    Disconnected {
        peer: PeerAddress,
    },
    CharacteristicWritten {
        peer: PeerAddress,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    },
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
