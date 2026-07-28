#[derive(Debug, thiserror::Error)]
pub enum BleError {
    #[error("BLE adapter unavailable: {0}")]
    AdapterUnavailable(String),

    #[error("peripheral (GATT server) mode not supported on this backend")]
    PeripheralUnsupported,

    #[error("connect to {peer} failed: {reason}")]
    ConnectFailed { peer: String, reason: String },

    #[error("GATT operation failed: {0}")]
    Gatt(String),

    #[error("not connected to {0}")]
    NotConnected(String),
}

pub type Result<T> = std::result::Result<T, BleError>;
