//! `mock-broker` server: accepts client connections and dispatches their
//! requests onto a single `LocalRadio` — the same struct and methods
//! `Radio::Local` uses in-process, so wire parity is structural rather than
//! a second implementation kept in sync by hand. See
//! docs/adr/0004-mock-broker-for-cross-process-e2e.md.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio::io::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::error::{BleError, Result};
use crate::models::{GattEvent, PeerAddress};

use super::local::LocalRadio;
use super::wire::{read_frame, write_frame, Envelope, Frame, Push, Request, Response};

struct ConnHandle {
    outbox: mpsc::UnboundedSender<Envelope>,
    /// Addresses this connection has `RegisterAddress`'d — swept on
    /// disconnect so a killed process doesn't leave the survivor's
    /// `events()` stream waiting forever for a `Disconnected` that will
    /// never come.
    owned_addresses: Mutex<HashSet<PeerAddress>>,
    /// Event/notify forwarder tasks spawned for this connection — aborted on
    /// disconnect rather than left to notice a failed send on their own
    /// next wakeup, which could otherwise be an arbitrarily long time away.
    forwarders: Mutex<Vec<JoinHandle<()>>>,
}

pub(super) async fn serve(listener: TcpListener) -> Result<()> {
    let radio = Arc::new(LocalRadio::default());
    loop {
        let (socket, _) = listener.accept().await.map_err(|e| BleError::Transport(e.to_string()))?;
        tokio::spawn(handle_connection(socket, radio.clone()));
    }
}

async fn handle_connection(socket: TcpStream, radio: Arc<LocalRadio>) {
    let (mut read_half, write_half) = tokio::io::split(socket);
    let (outbox_tx, mut outbox_rx) = mpsc::unbounded_channel::<Envelope>();
    let conn = Arc::new(ConnHandle {
        outbox: outbox_tx,
        owned_addresses: Mutex::new(HashSet::new()),
        forwarders: Mutex::new(Vec::new()),
    });

    // Single writer, fed by both this loop's responses and forwarder tasks'
    // pushes — a single mpsc queue is what keeps a response strictly ordered
    // after everything enqueued before it (see `Request::RegisterAddress`'s
    // doc comment).
    let writer = tokio::spawn(async move {
        let mut write_half: WriteHalf<TcpStream> = write_half;
        while let Some(env) = outbox_rx.recv().await {
            if write_frame(&mut write_half, &env).await.is_err() {
                break;
            }
        }
    });

    loop {
        let env = match read_frame(&mut read_half).await {
            Ok(env) => env,
            Err(_) => break,
        };
        let request = match env.frame {
            Frame::Req(request) => request,
            // A well-behaved client only ever sends `Req` frames.
            _ => continue,
        };
        let response = dispatch(request, &radio, &conn).await;
        if env.correlation_id != 0 {
            let _ = conn.outbox.send(Envelope { correlation_id: env.correlation_id, frame: Frame::Resp(response) });
        }
    }

    for handle in conn.forwarders.lock().unwrap().drain(..) {
        handle.abort();
    }
    writer.abort();

    // The process that owned this connection is gone (killed, crashed, or
    // closed cleanly without calling stop_advertising/disconnect itself) —
    // synthesize what that teardown would have done, or a surviving peer's
    // events() stream never learns this address is gone.
    let addresses: Vec<PeerAddress> = conn.owned_addresses.lock().unwrap().iter().cloned().collect();
    for address in addresses {
        radio.disconnect_all_for(&address).await;
    }
}

async fn dispatch(request: Request, radio: &Arc<LocalRadio>, conn: &Arc<ConnHandle>) -> Response {
    match request {
        Request::RegisterAddress { address } => {
            conn.owned_addresses.lock().unwrap().insert(address.clone());
            let (tx, mut rx) = tokio::sync::broadcast::channel(64);
            radio.register_events_sender(address.clone(), tx);
            let outbox = conn.outbox.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let env = Envelope {
                                correlation_id: 0,
                                frame: Frame::Push(Push::Event { address: address.clone(), event }),
                            };
                            if outbox.send(env).is_err() {
                                break;
                            }
                        }
                        // Forward the gap as `GattEvent::Lagged`, exactly as
                        // the local backend's own `events()` converts a
                        // `BroadcastStreamRecvError::Lagged` — not swallow
                        // it. The datagram layer's `serve`/`connect` treat
                        // `Lagged` as terminal specifically because the
                        // discarded events may include this peer's own
                        // `CharacteristicWritten`/`Disconnected`, and
                        // silently continuing left a receiver with no way to
                        // learn a fragment vanished: the channel looked
                        // perfectly healthy (no overflow signal, no forced
                        // reconnect) while a message that will now never
                        // arrive sat missing a piece. Confirmed as the root
                        // cause of a real "clean session, one message never
                        // arrives" bug found running fini's actors-ble e2e
                        // under bursty resend traffic.
                        Err(RecvError::Lagged(dropped)) => {
                            let env = Envelope {
                                correlation_id: 0,
                                frame: Frame::Push(Push::Event {
                                    address: address.clone(),
                                    event: GattEvent::Lagged { dropped },
                                }),
                            };
                            if outbox.send(env).is_err() {
                                break;
                            }
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });
            conn.forwarders.lock().unwrap().push(handle);
            Response::Unit
        }
        Request::Scan { requester, service } => match radio.scan(&requester, service).await {
            Ok((peers, armed_failure)) => Response::ScanSnapshot { peers, armed_failure },
            Err(e) => Response::Err(e),
        },
        Request::Connect { central, peer } => match radio.connect(&central, &peer).await {
            Ok(session) => Response::Session(session),
            Err(e) => Response::Err(e),
        },
        Request::Advertise { address, service } => match radio.advertise(&address, service).await {
            Ok(()) => Response::Unit,
            Err(e) => Response::Err(e),
        },
        Request::StopAdvertising { address } => match radio.stop_advertising(&address).await {
            Ok(()) => Response::Unit,
            Err(e) => Response::Err(e),
        },
        Request::Notify { address, characteristic, value } => {
            match radio.notify(&address, characteristic, value).await {
                Ok(()) => Response::Unit,
                Err(e) => Response::Err(e),
            }
        }
        Request::NotifyPeer { address, peer, session, characteristic, value } => {
            match radio.notify_peer(&address, &peer, session, characteristic, value).await {
                Ok(()) => Response::Unit,
                Err(e) => Response::Err(e),
            }
        }
        Request::DisconnectPeer { address, peer, session } => {
            match radio.disconnect_peer(&address, &peer, session).await {
                Ok(()) => Response::Unit,
                Err(e) => Response::Err(e),
            }
        }
        Request::Read { session, central, peripheral, characteristic } => {
            match radio.read(session, &central, &peripheral, characteristic).await {
                Ok(bytes) => Response::Bytes(bytes),
                Err(e) => Response::Err(e),
            }
        }
        Request::WriteWithType { session, central, peripheral, characteristic, value, write_type } => {
            match radio.write_with_type(session, &central, &peripheral, characteristic, value, write_type).await {
                Ok(()) => Response::Unit,
                Err(e) => Response::Err(e),
            }
        }
        Request::Subscribe { session, central, peripheral, characteristic, subscription_id } => {
            match radio.subscribe(session, &central, &peripheral, characteristic).await {
                Ok(mut stream) => {
                    let outbox = conn.outbox.clone();
                    let handle = tokio::spawn(async move {
                        while let Some(item) = stream.next().await {
                            let env = Envelope {
                                correlation_id: 0,
                                frame: Frame::Push(Push::NotifyItem { subscription_id, item }),
                            };
                            if outbox.send(env).is_err() {
                                break;
                            }
                        }
                    });
                    conn.forwarders.lock().unwrap().push(handle);
                    Response::Unit
                }
                Err(e) => Response::Err(e),
            }
        }
        Request::Disconnect { session, central, peripheral } => {
            match radio.disconnect(session, &central, &peripheral).await {
                Ok(()) => Response::Unit,
                Err(e) => Response::Err(e),
            }
        }
    }
}
