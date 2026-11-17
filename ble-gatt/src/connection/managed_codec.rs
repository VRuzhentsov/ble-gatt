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
