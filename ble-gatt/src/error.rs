#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "mock-broker", derive(serde::Serialize, serde::Deserialize))]
pub enum BleError {
    #[error("BLE adapter unavailable: {0}")]
    AdapterUnavailable(String),

    #[error("peripheral (GATT server) mode not supported on this backend")]
    PeripheralUnsupported,

    #[error("connect to {peer} failed: {reason}")]
    ConnectFailed { peer: String, reason: String },

    #[error("GATT operation failed: {0}")]
    Gatt(String),

    /// A GATT operation was rejected in a way the backend has reason to
    /// believe is transient (e.g. BlueZ's generic "briefly not ready"
    /// rejection right after a fresh connection) — distinct from `Gatt` so
    /// a caller can retry specifically on this variant, sized to its own
    /// timeout budget, without guessing whether a given rejection is worth
    /// retrying at all. The backend performs no retry of its own: no fixed
    /// internal budget fits every caller (see `is_transient_gatt_error`'s
    /// doc comment in the Linux backend for the real-hardware case that
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

pub type Result<T> = std::result::Result<T, BleError>;
