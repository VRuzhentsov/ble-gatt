#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum BleError {
    #[error("managed connection attempt: {0}")]
    Attempt(#[from] AttemptError),
    /// Managed profile validation or establishment failed. Raw paths do not emit this error.
    #[error("managed peer protocol: {0}")]
    Protocol(String),

    #[error("BLE adapter unavailable: {0}")]
    AdapterUnavailable(String),

    /// The adapter exists and is usable, but refused an adapter-level
    /// operation (e.g. starting discovery) because one is already in
    /// progress — distinct from `AdapterUnavailable` so a caller does not
    /// treat a busy radio as a missing one. Like `GattBusy`, the driver
    /// has already done whatever recovery it can; retrying is the caller's
    /// decision.
    #[error("BLE adapter busy: {0}")]
    AdapterBusy(String),

    #[error("peripheral (GATT server) mode not supported on this driver")]
    PeripheralUnsupported,

    /// The operation exists in the API but this driver, platform or role
    /// cannot perform it — e.g. requesting a connection priority on Linux,
    /// where BlueZ offers no such control. Not an error in the link itself:
    /// a caller written for several platforms can treat it as a no-op.
    #[error("not supported: {0}")]
    Unsupported(String),

    #[error("connect to {peer} failed: {reason}")]
    ConnectFailed { peer: String, reason: String },

    #[error("GATT operation failed: {0}")]
    Gatt(String),

    /// Known inbound payload loss. The channel remains usable; this is not
    /// evidence of a native failure or a missing lifecycle event.
    #[error("inbound receive gap: {0}")]
    ReceiveGap(String),

    /// A GATT operation was rejected in a way the driver has reason to
    /// believe is transient (e.g. BlueZ's generic "briefly not ready"
    /// rejection right after a fresh connection) — distinct from `Gatt` so
    /// a caller can retry specifically on this variant, sized to its own
    /// timeout budget, without guessing whether a given rejection is worth
    /// retrying at all. The driver performs no retry of its own: no fixed
    /// internal budget fits every caller (see `is_transient_gatt_error`'s
    /// doc comment in the Linux driver for the real-hardware case that
    /// motivated this split), so retrying — if any — is entirely the
    /// caller's decision.
    #[error("GATT operation rejected, possibly transient: {0}")]
    GattBusy(String),

    #[error("not connected to {0}")]
    NotConnected(String),

    /// A `mock-broker` connection itself failed — distinct from `Gatt` so a
    /// caller checking for a specific protocol error (e.g. `NotConnected`)
    /// isn't fooled by a transport hiccup, and vice versa. Always present
    /// (not cfg-gated) so `BleError`'s shape doesn't differ by feature flag;
    /// only its serde derive above is feature-gated.
    #[error("mock broker transport error: {0}")]
    Transport(String),
}

/// Terminal managed attempt outcomes. Native and malformed-wire errors remain separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum AttemptError {
    #[error("peer rejected the request")]
    Rejected,
    #[error("request cancelled")]
    Cancelled,
    #[error("another attempt won")]
    Superseded,
    #[error("equal attempt nonces")]
    EqualNonce,
    #[error("establishment deadline exceeded")]
    TimedOut,
    #[error("peer lost")]
    PeerLost,
    #[error("explicit disconnect")]
    Disconnected,
    #[error("peer disconnected")]
    PeerDisconnected,
    #[error("last use released")]
    Released,
    #[error("bounded queue exhausted")]
    QueueFull,
    #[error("worker closed")]
    Closed,
    #[error("invalid managed record")]
    InvalidRecord,
}

pub type Result<T> = std::result::Result<T, BleError>;
