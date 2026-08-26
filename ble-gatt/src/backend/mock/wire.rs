//! The `mock-broker` wire protocol: one `Envelope` per frame, length-prefixed
//! JSON over the socket. `correlation_id` ties a `Resp` back to its `Req`;
//! `0` marks a fire-and-forget request (no response is awaited) or an
//! unsolicited `Push`. See docs/adr/0004-mock-broker-for-cross-process-e2e.md
//! for why JSON over hand-rolled binary framing, and why `Push` multiplexes
//! `events()`/`subscribe()` over the same connection as request/response.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{BleError, Result};
use crate::models::{
    CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress, ServiceUuid,
    WriteType,
};

#[derive(Serialize, Deserialize)]
pub(super) struct Envelope {
    pub(super) correlation_id: u64,
    pub(super) frame: Frame,
}

#[derive(Serialize, Deserialize)]
pub(super) enum Frame {
    Req(Request),
    Resp(Response),
    Push(Push),
}

#[derive(Serialize, Deserialize)]
pub(super) enum Request {
    /// Fire-and-forget (sent with `correlation_id: 0`): tells the broker to
    /// route future `Push::Event`s addressed to `address` onto this
    /// connection. Always sent before any other request from the same
    /// `RemoteClient` — the shared per-connection outbox queue preserves
    /// that ordering on the wire without an explicit ack, and every
    /// operation that could cause a push to `address` is itself gated
    /// behind `address` having already taken some prior action (advertise
    /// to be discoverable, subscribe to become a notify target), which can
    /// only happen after this has already been sent. See docs/adr/0004.
    RegisterAddress { address: PeerAddress },
    Scan { requester: PeerAddress, service: ServiceUuid },
    Connect { central: PeerAddress, peer: PeerAddress },
    Advertise { address: PeerAddress, service: GattServiceSpec },
    StopAdvertising { address: PeerAddress },
    Notify { address: PeerAddress, characteristic: CharacteristicUuid, value: Vec<u8> },
    NotifyPeer {
        address: PeerAddress, peer: PeerAddress, session: Option<u64>,
        characteristic: CharacteristicUuid, value: Vec<u8>,
    },
    DisconnectPeer { address: PeerAddress, peer: PeerAddress, session: Option<u64> },
    Read { session: u64, central: PeerAddress, peripheral: PeerAddress, characteristic: CharacteristicUuid },
    WriteWithType {
        session: u64, central: PeerAddress, peripheral: PeerAddress,
        characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    },
    /// `subscription_id` is **client-allocated**, not broker-allocated —
    /// deliberately, so the client can register its local delivery channel
    /// under that id *before* sending this request, closing the only
    /// otherwise-possible race in this protocol: a `Push::NotifyItem`
    /// arriving before the client has anywhere to route it. See
    /// docs/adr/0004.
    Subscribe {
        session: u64, central: PeerAddress, peripheral: PeerAddress,
        characteristic: CharacteristicUuid, subscription_id: u64,
    },
    Disconnect { session: u64, central: PeerAddress, peripheral: PeerAddress },
}

#[derive(Serialize, Deserialize)]
pub(super) enum Response {
    Unit,
    ScanSnapshot { peers: Vec<DiscoveredPeer>, armed_failure: Option<String> },
    Session(u64),
    Bytes(Vec<u8>),
    Err(BleError),
}

#[derive(Serialize, Deserialize)]
pub(super) enum Push {
    Event { address: PeerAddress, event: GattEvent },
    NotifyItem { subscription_id: u64, item: std::result::Result<Vec<u8>, BleError> },
}

/// `u32` BE length prefix + `serde_json`. Test-only, non-perf-sensitive
/// protocol — see docs/adr/0004 for why this was chosen over hand-rolled
/// binary framing.
pub(super) async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, env: &Envelope) -> Result<()> {
    let bytes = serde_json::to_vec(env).map_err(|e| BleError::Transport(e.to_string()))?;
    w.write_u32(bytes.len() as u32).await.map_err(|e| BleError::Transport(e.to_string()))?;
    w.write_all(&bytes).await.map_err(|e| BleError::Transport(e.to_string()))
}

pub(super) async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Envelope> {
    let len = r.read_u32().await.map_err(|e| BleError::Transport(e.to_string()))?;
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await.map_err(|e| BleError::Transport(e.to_string()))?;
    serde_json::from_slice(&buf).map_err(|e| BleError::Transport(e.to_string()))
}
