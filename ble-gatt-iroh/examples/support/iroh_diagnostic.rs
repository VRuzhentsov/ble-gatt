//! Application-only diagnostic shared by the Linux and Android examples.
//! iroh owns QUIC connections. The core worker owns BLE establishment and cleanup.
use ble_gatt::{
    datagram::DatagramConfig, Adapter, CharacteristicUuid, GattEvent, PeerAddress, PeerLink,
    PeerLinkConfig, Role, ServiceUuid,
};
use ble_gatt_iroh::{dial_with, BleGattTransport, Dialer};
use iroh::{
    endpoint::{presets, Connection},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointId, RelayMode, SecretKey,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_stream::StreamExt;

const ALPN: &[u8] = b"ble-gatt/example/iroh-diagnostic/1";
const COMPANY: u16 = 0xffff;

pub fn mark(line: impl std::fmt::Display) {
    Options::mark(line)
}

pub fn payload(sender: EndpointId, round: u8) -> Vec<u8> {
    Echo::payload(sender, round)
}

pub fn mode(side: &str, mode: &str) -> Result<(bool, bool), String> {
    Options::mode(side, mode)
}

/// Bounded run, including shutdown on failure. A PASS requires last-user cleanup
/// without force-close; emergency close_links is only used after failure.
pub async fn run(adapter: Adapter, options: Options) -> Result<(), String> {
    options
        .run_inner(
            adapter,
            #[cfg(test)]
            false,
        )
        .await
}

#[derive(Debug, Clone)]
struct Echo {
    peer: EndpointId,
    received: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
}
impl Echo {
    fn payload(sender: EndpointId, round: u8) -> Vec<u8> {
        let mut bytes: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        bytes[..32].copy_from_slice(sender.as_bytes());
        bytes[32] = round;
        bytes
    }

    fn acknowledged(
        &self,
        round: u8,
        ack: Result<Option<iroh::endpoint::VarInt>, impl std::fmt::Display>,
    ) -> Result<(), String> {
        let result = match ack {
            Ok(None) if (1..=3).contains(&round) => {
                let bit = 1 << (round - 1);
                if self.received.fetch_or(bit, Ordering::SeqCst) & bit != 0 {
                    Err(format!("duplicate acknowledged round {round}"))
                } else {
                    Ok(())
                }
            }
            Ok(None) => Err(format!("invalid acknowledged round {round}")),
            Ok(Some(code)) => Err(format!("echo stopped by peer: {code}")),
            Err(error) => Err(format!("echo acknowledgement failed: {error}")),
        };
        if let Err(error) = &result {
            *self.failure.lock().expect("diagnostic failure mutex") = Some(error.clone());
        }
        result
    }

    fn complete(&self) -> bool {
        self.received.load(Ordering::SeqCst) == 0b111
    }

    fn check(&self) -> Result<(), String> {
        match self
            .failure
            .lock()
            .expect("diagnostic failure mutex")
            .clone()
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    async fn probe(connection: &Connection, local: EndpointId, round: u8) -> Result<(), String> {
        let bytes = payload(local, round);
        let (mut send, mut recv) = connection.open_bi().await.map_err(|e| e.to_string())?;
        send.write_all(&bytes).await.map_err(|e| e.to_string())?;
        send.finish().map_err(|e| e.to_string())?;
        let echoed = recv.read_to_end(4096).await.map_err(|e| e.to_string())?;
        if echoed != bytes {
            return Err("synthetic payload mismatch".into());
        }
        mark(format!(
            "PAYLOAD matched peer={} handle={} round={round} bytes={}",
            connection.remote_id(),
            connection.stable_id(),
            bytes.len()
        ));
        Ok(())
    }
}

impl ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if connection.remote_id() != self.peer {
            connection.close(1u32.into(), b"unexpected diagnostic peer");
            return Ok(());
        }
        mark(format!(
            "QUIC incoming peer={} handle={} paths={:?}",
            self.peer,
            connection.stable_id(),
            connection.paths()
        ));
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let bytes = recv
                .read_to_end(4096)
                .await
                .map_err(std::io::Error::other)?;
            let round = bytes.get(32).copied().unwrap_or(0);
            if !(1..=3).contains(&round) || bytes != payload(self.peer, round) {
                connection.close(2u32.into(), b"invalid synthetic probe");
                return Ok(());
            }
            send.write_all(&bytes)
                .await
                .map_err(std::io::Error::other)?;
            send.finish()?;
            // Do not report delivery until the echo stream is acknowledged.
            self.acknowledged(round, send.stopped().await)
                .map_err(std::io::Error::other)?;
            let rounds = self.received.load(Ordering::SeqCst);
            mark(format!(
                "PAYLOAD received peer={} handle={} round={round} bytes={} echo_ack=true rounds_mask={rounds:03b}",
                self.peer,
                connection.stable_id(),
                bytes.len()
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CarrierTasks {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl CarrierTasks {
    async fn wait_for_cleanup(
        &self,
        transport: &BleGattTransport,
        timeout: Duration,
    ) -> Result<usize, String> {
        tokio::time::timeout(timeout, async {
        loop {
            let finished = {
                let mut handles = self.handles.lock().expect("diagnostic carriers mutex");
                if transport.linked_peers().is_empty() && handles.iter().all(|h| h.is_finished()) {
                    Some(std::mem::take(&mut *handles))
                } else {
                    None
                }
            };
            if let Some(handles) = finished {
                let count = handles.len();
                for handle in handles {
                    handle.await.map_err(|e| format!("incoming carrier task failed: {e}"))?;
                }
                return Ok(count);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.map_err(|_| format!(
        "last-user cleanup timed out before shutdown: address_queues={:?} incoming_carriers_pending={}",
        transport.linked_peers(),
        self.handles.lock().expect("diagnostic carriers mutex").iter().filter(|h| !h.is_finished()).count(),
    ))?
    }
}

/// Driver lifecycle evidence, not a claim about every OS Bluetooth object.
/// Subscribe before serving/dialling; a lost event makes the evidence invalid.
#[derive(Default)]
struct NativeSessions {
    active: Vec<(PeerAddress, Role, Option<u64>)>,
    observed: usize,
    failure: Option<String>,
}

impl NativeSessions {
    fn observe(&mut self, event: GattEvent) {
        match event {
            GattEvent::Connected {
                peer,
                local_role,
                session,
            } => {
                let key = (peer, local_role, session);
                if !self.active.contains(&key) {
                    self.observed += 1;
                    self.active.push(key);
                }
            }
            GattEvent::Disconnected {
                peer,
                local_role,
                session,
            } => {
                // Drivers without disconnect session IDs document address
                // fallback. A concrete old session must not evict a newer one.
                self.active.retain(|(address, role, id)| {
                    address != &peer || role != &local_role || (session.is_some() && id != &session)
                });
            }
            GattEvent::Lagged { dropped } => {
                self.failure = Some(format!("native lifecycle evidence lost {dropped} events"));
            }
            _ => {}
        }
    }
    async fn wait_for_native_cleanup(
        sessions: &Mutex<NativeSessions>,
        timeout: Duration,
    ) -> Result<usize, String> {
        tokio::time::timeout(timeout, async {
            loop {
                {
                    let state = sessions.lock().expect("diagnostic native sessions mutex");
                    if let Some(error) = &state.failure {
                        return Err(error.clone());
                    }
                    if state.observed > 0 && state.active.is_empty() {
                        return Ok(state.observed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| {
            let state = sessions.lock().expect("diagnostic native sessions mutex");
            format!(
                "native session cleanup timed out before shutdown: observed={} active={:?}",
                state.observed, state.active
            )
        })?
    }
    // Shared production finalization seam: carrier cleanup may yield while the
    // independent native observer continues to update its evidence.
    async fn finalize_cleanup<F, Fut, C>(
        sessions: &Mutex<NativeSessions>,
        timeout: Duration,
        mut cleanup: F,
        carriers_clean: C,
    ) -> Result<(usize, usize), String>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<usize, String>>,
        C: Fn() -> bool,
    {
        tokio::time::timeout(timeout, async {
            let mut joined = cleanup().await?;
            loop {
                NativeSessions::wait_for_native_cleanup(sessions, timeout).await?;
                // Late QUIC close traffic may have produced another carrier
                // while the native disconnect was in flight. Joining it can yield
                // and invalidate the native checkpoint: never reuse that snapshot.
                joined += cleanup().await?;
                {
                    let state = sessions.lock().expect("diagnostic native sessions mutex");
                    if let Some(error) = &state.failure {
                        return Err(error.clone());
                    }
                    if state.observed > 0 && state.active.is_empty() && carriers_clean() {
                        return Ok((joined, state.observed));
                    }
                }
                // Retry both checkpoints, bounded by one overall deadline.
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| {
            "final native/carrier cleanup checkpoint timed out before shutdown".to_string()
        })?
    }
}

pub struct Options {
    pub key: SecretKey,
    pub peer: EndpointId,
    pub initiate: bool,
    pub expect_incoming: bool,
    /// Optional common wall-clock start in Unix seconds, after discovery.
    pub start_at: u64,
    pub timeout: Duration,
}

impl Options {
    fn scheduled_delay(start_at: u64, now: Duration) -> Duration {
        // Keep the fractional part: truncating `now` schedules each peer at
        // start_at plus its own discovery-time fraction, up to one second apart.
        Duration::from_secs(start_at).saturating_sub(now)
    }

    fn mark(line: impl std::fmt::Display) {
        log::info!("IROHDIAG: {line}");
    }

    // Five bytes fit alongside flags + 128-bit service UUID in legacy advertising.
    // A discovery hint only: TLS authenticates the full out-of-band peer key.
    fn hint(id: EndpointId) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&id.as_bytes()[..4]);
        bytes
    }

    fn config(id: EndpointId) -> DatagramConfig {
        let mut config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::from_u128(
                0xb1e6a110_f101_4000_8000_00805f9b34fb,
            )),
            CharacteristicUuid(uuid::Uuid::from_u128(
                0xb1e6a111_f101_4000_8000_00805f9b34fb,
            )),
        );
        config
            .advertised_manufacturer_data
            .insert(COMPANY, Options::hint(id));
        config
    }

    fn mode(side: &str, mode: &str) -> Result<(bool, bool), String> {
        if !matches!(side, "phone" | "linux") {
            return Err("side must be phone or linux".into());
        }
        match mode {
            "both" => Ok((true, true)),
            "phone-initiate" => Ok((side == "phone", side != "phone")),
            "linux-initiate" => Ok((side == "linux", side != "linux")),
            _ => Err("mode must be phone-initiate, linux-initiate or both".into()),
        }
    }

    async fn run_inner(
        self,
        adapter: Adapter,
        #[cfg(test)] acquire_before_survivor: bool,
    ) -> Result<(), String> {
        let id = self.key.public();
        if id == self.peer {
            return Err("local and remote keys must differ".into());
        }
        let caps = adapter.capabilities().await;
        mark(format!("START local={id} peer={} establishment=core initiate={} expect_incoming={} ip=disabled relay=disabled central={} peripheral={}", self.peer, self.initiate, self.expect_incoming, caps.central, caps.peripheral));
        if !caps.central || !caps.peripheral {
            return Err(
                "diagnostic requires both individual roles (not proof of concurrency)".into(),
            );
        }
        let config = Options::config(id);
        let mut native_events = adapter.driver().events();
        let peer_link =
            PeerLink::with_driver(adapter.driver(), PeerLinkConfig::new(config.clone()));
        let base_dial = dial_with(peer_link.clone());
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = attempts.clone();
        let dialer: Dialer = Arc::new(move |peer| {
            let base = base_dial.clone();
            let n = count.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                mark(format!("ACQUIRE request={n} advertised_address={}", peer.0));
                let result = base(peer.clone()).await;
                match &result {
                    Ok(_) => mark(format!("ACQUIRED request={n} address={}", peer.0)),
                    Err(e) => mark(format!("FAILED request={n} address={} error={e}", peer.0)),
                }
                result
            })
        });
        let transport = BleGattTransport::builder()
            .dialer(dialer.clone())
            .link_idle_timeout(Duration::from_secs(2))
            .build();
        let mut incoming = peer_link
            .incoming()
            .map_err(|e| format!("incoming requests: {e}"))?;
        peer_link
            .set_advertising(true)
            .await
            .map_err(|e| format!("advertising: {e}"))?;
        let native_sessions = Arc::new(Mutex::new(NativeSessions::default()));
        let watched_sessions = native_sessions.clone();
        let native_watch = tokio::spawn(async move {
            while let Some(event) = native_events.next().await {
                watched_sessions
                    .lock()
                    .expect("diagnostic native sessions mutex")
                    .observe(event);
            }
            watched_sessions
                .lock()
                .expect("diagnostic native sessions mutex")
                .failure = Some("native lifecycle event stream ended before cleanup".into());
        });
        let attach_to = transport.clone();
        let carriers = CarrierTasks::default();
        let incoming_carriers = carriers.clone();
        let attach = tokio::spawn(async move {
            let mut n = 0;
            while let Some(request) = incoming.next().await {
                let channel = match request.accept().await {
                    Ok(channel) => channel,
                    Err(error) => {
                        mark(format!("INCOMING failed error={error}"));
                        continue;
                    }
                };
                n += 1;
                mark(format!(
                    "ATTACH incoming={n} source_address={} source_is_not_dial_mapping=true",
                    channel.peer().0
                ));
                incoming_carriers
                    .handles
                    .lock()
                    .expect("diagnostic carriers mutex")
                    .push(attach_to.attach(channel));
            }
        });
        let endpoint = match Endpoint::builder(presets::Minimal)
            .secret_key(self.key.clone())
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .add_custom_transport(Arc::new(transport.clone()))
            .address_lookup(transport.address_lookup())
            .hooks(transport.hooks())
            .bind()
            .await
        {
            Ok(endpoint) => endpoint,
            Err(e) => {
                attach.abort();
                native_watch.abort();
                let _ = adapter.close().await;
                return Err(format!("bind: {e}"));
            }
        };
        let received = Arc::new(AtomicUsize::new(0));
        let echo = Echo {
            peer: self.peer,
            received: received.clone(),
            failure: Arc::new(Mutex::new(None)),
        };
        let router = Router::builder(endpoint.clone())
            .accept(ALPN, echo.clone())
            .spawn();
        mark(format!("READY local={id} service={}", config.service.0));
        let result = tokio::time::timeout(self.timeout, async {
        if self.initiate {
            let scan_started = std::time::Instant::now();
            let mut scan = peer_link.discover().await.map_err(|e| format!("discovery: {e}"))?;
            let address = loop {
                let (_core_endpoint, peer) = scan
                    .next()
                    .await
                    .ok_or("discovery ended")?
                    .map_err(|e| e.to_string())?;
                // Scan start to item: a radio record lists a peer at once, a read takes seconds.
                mark(format!(
                    "DISCOVERY advertised_address={} hint_match={} after_ms={}",
                    peer.address.0,
                    peer.manufacturer_data.get(&COMPANY) == Some(&Options::hint(self.peer)),
                    scan_started.elapsed().as_millis()
                ));
                if peer.manufacturer_data.get(&COMPANY) == Some(&Options::hint(self.peer)) {
                    break peer.address;
                }
            };
            drop(scan);
            transport.set_peer_address(self.peer, address.clone());
            mark(format!(
                "MAP authenticated_target={} advertised_address={}",
                self.peer, address.0
            ));
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?;
            tokio::time::sleep(Options::scheduled_delay(self.start_at, now)).await;
            mark("CONNECT application_attempt=1");
            let first = endpoint
                .connect(self.peer, ALPN)
                .await
                .map_err(|e| format!("connect 1: {e}"))?;
            mark(format!(
                "QUIC outgoing peer={} handle={} paths={:?}",
                first.remote_id(),
                first.stable_id(),
                first.paths()
            ));
            Echo::probe(&first, id, 1).await?;
            let before = attempts.load(Ordering::SeqCst);
            let second = endpoint
                .connect(self.peer, ALPN)
                .await
                .map_err(|e| format!("connect 2: {e}"))?;
            Echo::probe(&second, id, 2).await?;
            if attempts.load(Ordering::SeqCst) != before {
                return Err(
                    "second QUIC connection unexpectedly dialled a new BLE carrier".into(),
                );
            }
            mark(format!(
                "REUSE first_handle={} second_handle={} dial_attempts={before}",
                first.stable_id(),
                second.stable_id()
            ));
            first.close(0u32.into(), b"end first diagnostic carrier only");
            drop(first);
            // Longer than the deliberately short idle timer: the remaining
            // connection must protect its BLE path after the first ends.
            tokio::time::sleep(Duration::from_secs(3)).await;
            if second.close_reason().is_some() {
                return Err("survivor closed after first connection ended".into());
            }
            #[cfg(test)]
            if acquire_before_survivor {
                // An extra scoped acquisition, without replacing the QUIC handle.
                let extra = dialer(address.clone()).await.map_err(|e| e.to_string())?;
                drop(extra);
            }
            Echo::probe(&second, id, 3).await?;
            let after = attempts.load(Ordering::SeqCst);
            if after != before {
                return Err(format!(
                    "survivor unexpectedly dialled a new BLE carrier: before={before} after={after} (round 3 echoed)"
                ));
            }
            mark(format!("SURVIVOR live_after_first_close_and_idle=true dial_attempts={after} unchanged=true"));
            second.close(0u32.into(), b"diagnostic done");
            drop(second);
        }
        if self.expect_incoming {
            while !echo.complete() {
                echo.check()?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        echo.check()?;
        let (joined, observed) = NativeSessions::finalize_cleanup(
            &native_sessions,
            Duration::from_secs(20),
            || carriers.wait_for_cleanup(&transport, Duration::from_secs(20)),
            || {
                let handles = carriers.handles.lock().expect("diagnostic carriers mutex");
                handles.is_empty() && transport.linked_peers().is_empty()
            },
        ).await?;
        mark(format!("NATIVE_CLEANUP active_sessions=0 observed_sessions={observed} evidence=driver_events final_checkpoint=current before_shutdown=true"));
        echo.check()?;
        mark(format!(
            "CLEANUP address_queues=0 incoming_carriers_joined={joined} before_shutdown=true dial_attempts={} received_rounds_mask={:03b} native_teardown=unverified",
            attempts.load(Ordering::SeqCst),
            received.load(Ordering::SeqCst)
        ));
        Ok::<(), String>(())
    })
    .await
    .unwrap_or_else(|_| Err("diagnostic timed out (inspect earliest missing native event)".into()));
        if result.is_err() {
            transport.close_links(&self.peer);
        }
        let _ = router.shutdown().await;
        native_watch.abort();
        let _ = native_watch.await;
        endpoint.close().await;
        attach.abort();
        let _ = attach.await;
        let _ = peer_link.close().await;
        let closed = adapter
            .close()
            .await
            .map_err(|e| format!("adapter shutdown: {e}"));
        // Only failure/late-arrival tasks can remain; this is teardown, not PASS evidence.
        let remaining =
            std::mem::take(&mut *carriers.handles.lock().expect("diagnostic carriers mutex"));
        for handle in remaining {
            handle.abort();
            let _ = handle.await;
        }
        result.and(closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn echo() -> Echo {
        Echo {
            peer: SecretKey::generate().public(),
            received: Arc::new(AtomicUsize::new(0)),
            failure: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn final_cleanup_rechecks_session_arriving_during_late_carrier_wait() {
        use std::{future::Future, task::Poll};
        let state = Mutex::new(NativeSessions {
            observed: 1,
            ..Default::default()
        });
        let calls = AtomicUsize::new(0);
        let finalization = NativeSessions::finalize_cleanup(
            &state,
            Duration::from_secs(1),
            || {
                let late = calls.fetch_add(1, Ordering::SeqCst) == 1;
                let state = &state;
                async move {
                    if late {
                        state.lock().unwrap().observe(GattEvent::Connected {
                            peer: PeerAddress("replacement".into()),
                            local_role: Role::Peripheral,
                            session: Some(2),
                        });
                        // Deliberately yield within the final carrier wait,
                        // after the initial native-clean checkpoint succeeded.
                        tokio::task::yield_now().await;
                    }
                    Ok(1)
                }
            },
            || true,
        );
        tokio::pin!(finalization);
        std::future::poll_fn(|cx| {
            assert!(finalization.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        std::future::poll_fn(|cx| {
            assert!(
                finalization.as_mut().poll(cx).is_pending(),
                "success must not escape while the replacement native session is active"
            );
            Poll::Ready(())
        })
        .await;
        state.lock().unwrap().observe(GattEvent::Disconnected {
            peer: PeerAddress("replacement".into()),
            local_role: Role::Peripheral,
            session: Some(2),
        });
        assert_eq!(finalization.await.unwrap(), (3, 2));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "recheck carrier queues after native retry"
        );
    }

    async fn final_cleanup_lost_evidence(ended: bool) {
        let state = Mutex::new(NativeSessions {
            observed: 1,
            ..Default::default()
        });
        let calls = AtomicUsize::new(0);
        let result = NativeSessions::finalize_cleanup(
            &state,
            Duration::from_secs(1),
            || {
                let late = calls.fetch_add(1, Ordering::SeqCst) == 1;
                let state = &state;
                async move {
                    if late {
                        tokio::task::yield_now().await;
                        let mut state = state.lock().unwrap();
                        if ended {
                            state.failure =
                                Some("native lifecycle event stream ended before cleanup".into());
                        } else {
                            state.observe(GattEvent::Lagged { dropped: 7 });
                        }
                    }
                    Ok(0)
                }
            },
            || true,
        )
        .await;
        assert!(
            result.as_ref().is_err_and(|error| error.contains(if ended {
                "stream ended"
            } else {
                "lost 7 events"
            })),
            "lost native evidence during the final carrier wait must fail closed: {result:?}"
        );
    }

    #[tokio::test]
    async fn final_cleanup_rejects_lag_during_late_carrier_wait() {
        final_cleanup_lost_evidence(false).await;
    }

    #[tokio::test]
    async fn final_cleanup_rejects_ended_stream_during_late_carrier_wait() {
        final_cleanup_lost_evidence(true).await;
    }

    #[tokio::test]
    async fn final_cleanup_accepts_current_clean_evidence() {
        let state = Mutex::new(NativeSessions {
            observed: 2,
            ..Default::default()
        });
        let result = NativeSessions::finalize_cleanup(
            &state,
            Duration::from_secs(1),
            || async {
                tokio::task::yield_now().await;
                Ok(1)
            },
            || true,
        )
        .await;
        assert_eq!(result.unwrap(), (2, 2));
    }

    #[test]
    fn scheduled_start_preserves_subsecond_deadline() {
        let now = Duration::from_millis(100_900);
        assert_eq!(
            Options::scheduled_delay(101, now),
            Duration::from_millis(100)
        );
        assert_eq!(
            Options::scheduled_delay(101, Duration::from_millis(100_100)),
            Duration::from_millis(900)
        );
        assert_eq!(Options::scheduled_delay(100, now), Duration::ZERO);
        assert_eq!(Options::scheduled_delay(0, now), Duration::ZERO);
    }

    #[tokio::test]
    async fn native_cleanup_waits_for_each_role_and_session_and_rejects_lag() {
        let state = Arc::new(Mutex::new(NativeSessions::default()));
        assert!(
            NativeSessions::wait_for_native_cleanup(&state, Duration::from_millis(5))
                .await
                .is_err(),
            "an empty observer with no lifecycle evidence is not teardown proof"
        );
        let peer = PeerAddress("native-peer".into());
        for (local_role, session) in [(Role::Central, 1), (Role::Peripheral, 2)] {
            state.lock().unwrap().observe(GattEvent::Connected {
                peer: peer.clone(),
                local_role,
                session: Some(session),
            });
        }
        // Duplicate readiness must not invent an additional session.
        state.lock().unwrap().observe(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Peripheral,
            session: Some(2),
        });
        state.lock().unwrap().observe(GattEvent::Disconnected {
            peer: peer.clone(),
            local_role: Role::Central,
            session: Some(1),
        });
        let result =
            NativeSessions::wait_for_native_cleanup(&state, Duration::from_millis(10)).await;
        assert!(
            result.is_err(),
            "the peripheral session still exists: {result:?}"
        );
        // A stale disconnect cannot release the replacement session.
        state.lock().unwrap().observe(GattEvent::Disconnected {
            peer: peer.clone(),
            local_role: Role::Peripheral,
            session: Some(1),
        });
        assert!(
            NativeSessions::wait_for_native_cleanup(&state, Duration::from_millis(10))
                .await
                .is_err()
        );
        state.lock().unwrap().observe(GattEvent::Disconnected {
            peer,
            local_role: Role::Peripheral,
            session: Some(2),
        });
        assert_eq!(
            NativeSessions::wait_for_native_cleanup(&state, Duration::from_millis(100))
                .await
                .unwrap(),
            2
        );
        state
            .lock()
            .unwrap()
            .observe(GattEvent::Lagged { dropped: 1 });
        assert!(
            NativeSessions::wait_for_native_cleanup(&state, Duration::from_millis(100))
                .await
                .is_err()
        );
    }

    #[test]
    fn duplicate_round_one_cannot_complete_listener() {
        let echo = echo();
        for _ in 0..3 {
            let _ = echo.acknowledged(1, Ok::<_, String>(None));
        }
        assert!(
            !echo.complete(),
            "three copies of round one are not rounds 1/2/3"
        );
    }

    #[test]
    fn stopped_echo_cannot_count_as_delivered() {
        let echo = echo();
        let result = echo.acknowledged(1, Ok::<_, String>(Some(7u32.into())));
        assert!(result.is_err(), "STOP_SENDING is not delivery: {result:?}");
        assert_eq!(echo.received.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn failed_echo_ack_cannot_count_as_delivered() {
        let echo = echo();
        let result = echo.acknowledged(1, Err("connection lost before ACK"));
        assert!(result.is_err(), "lost ACK is not delivery: {result:?}");
        assert_eq!(echo.received.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn survivor_rejects_extra_acquisition_even_when_round_three_echoes() {
        use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
        use ble_gatt::{CapabilityReport, PeerAddress};
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .is_test(true)
            .try_init();
        let network = MockNetwork::new();
        let make = |name: &str| {
            Arc::new(MockDriver::new(
                PeerAddress(name.into()),
                network.clone(),
                CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            ))
        };
        let a = SecretKey::generate();
        let b = SecretKey::generate();
        let opts = |key, peer, initiate| Options {
            key,
            peer,
            initiate,
            expect_incoming: !initiate,
            start_at: 0,
            timeout: Duration::from_secs(15),
        };
        let aa = Adapter::new(make("redial-a")).await;
        let ba = Adapter::new(make("redial-b")).await;
        let (ar, _br) = tokio::join!(
            opts(a.clone(), b.public(), true).run_inner(aa, true),
            run(ba, opts(b, a.public(), false)),
        );
        assert!(
            ar.as_ref()
                .is_err_and(|e| e.contains("survivor unexpectedly dialled")),
            "must reject a new carrier even if logical QUIC and round 3 survive: {ar:?}"
        );
    }

    async fn mock_connections() -> (
        Connection,
        Connection,
        Endpoint,
        Endpoint,
        Adapter,
        Adapter,
        tokio::task::JoinHandle<()>,
    ) {
        use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
        use ble_gatt::{CapabilityReport, PeerAddress};
        let network = MockNetwork::new();
        let make = |name: &str| {
            Arc::new(MockDriver::new(
                PeerAddress(name.into()),
                network.clone(),
                CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            ))
        };
        let aa = Adapter::new(make("ack-a")).await;
        let ba = Adapter::new(make("ack-b")).await;
        let a = SecretKey::generate();
        let b = SecretKey::generate();
        let config = Options::config(b.public());
        let bt = BleGattTransport::builder().build();
        let bp = PeerLink::with_driver(ba.driver(), PeerLinkConfig::new(config.clone()));
        let ap = PeerLink::with_driver(aa.driver(), PeerLinkConfig::new(config));
        let mut incoming = bp.incoming().unwrap();
        bp.set_advertising(true).await.unwrap();
        let accept = tokio::spawn({
            let bt = bt.clone();
            async move {
                let _keep_alive = bp;
                while let Some(request) = incoming.next().await {
                    let Ok(channel) = request.accept().await else {
                        continue;
                    };
                    bt.attach(channel);
                }
            }
        });
        let at = BleGattTransport::builder().dialer(dial_with(ap)).build();
        at.set_peer_address(b.public(), PeerAddress("ack-b".into()));
        let endpoint = |key, transport: BleGattTransport| async move {
            Endpoint::builder(presets::Minimal)
                .secret_key(key)
                .relay_mode(RelayMode::Disabled)
                .clear_ip_transports()
                .alpns(vec![ALPN.to_vec()])
                .address_lookup(transport.address_lookup())
                .hooks(transport.hooks())
                .add_custom_transport(Arc::new(transport))
                .bind()
                .await
                .unwrap()
        };
        let ae = endpoint(a, at).await;
        let be = endpoint(b.clone(), bt).await;
        let (ac, bc) = tokio::join!(ae.connect(b.public(), ALPN), async {
            be.accept().await.unwrap().await.unwrap()
        });
        (ac.unwrap(), bc, ae, be, aa, ba, accept)
    }

    async fn wire_ack_case(stop: bool) {
        let (client, server, ae, be, aa, ba, attach) = mock_connections().await;
        let echo = echo();
        let (mut tx, mut rx) = client.open_bi().await.unwrap();
        tx.write_all(b"request").await.unwrap();
        let (mut send, _recv) = server.accept_bi().await.unwrap();
        send.write_all(b"echo").await.unwrap();
        // Leave the send side unfinished so an automatic transport ACK cannot
        // race STOP_SENDING into successful completion. Happy-path FIN/ACK is
        // exercised by the full runner tests.
        // STOP or connection loss before allowing successful stream consumption.
        // Await the actual iroh/noq result, not a source-string assertion.
        if stop {
            rx.stop(7u32.into()).unwrap();
        } else {
            server.close(9u32.into(), b"test connection loss before ACK");
        }
        let ack = tokio::time::timeout(Duration::from_secs(5), send.stopped())
            .await
            .unwrap();
        if stop {
            assert!(
                matches!(ack, Ok(Some(code)) if code == 7u32.into()),
                "{ack:?}"
            );
        } else {
            assert!(ack.is_err(), "{ack:?}");
        }
        assert!(echo.acknowledged(1, ack).is_err());
        assert!(!echo.complete());
        assert_eq!(echo.received.load(Ordering::SeqCst), 0);
        client.close(0u32.into(), b"test done");
        ae.close().await;
        be.close().await;
        attach.abort();
        let _ = attach.await;
        aa.close().await.unwrap();
        ba.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_listener_rejects_duplicate_round_one() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (client, server, ae, be, aa, ba, attach) = mock_connections().await;
            let echo = Echo {
                peer: server.remote_id(),
                received: Arc::new(AtomicUsize::new(0)),
                failure: Arc::new(Mutex::new(None)),
            };
            let handler = tokio::spawn({
                let echo = echo.clone();
                async move { echo.accept(server).await }
            });
            Echo::probe(&client, echo.peer, 1).await.unwrap();
            Echo::probe(&client, echo.peer, 1).await.unwrap();
            assert!(handler.await.unwrap().is_err());
            assert!(!echo.complete());
            assert!(echo
                .check()
                .unwrap_err()
                .contains("duplicate acknowledged round 1"));
            client.close(0u32.into(), b"test done");
            ae.close().await;
            be.close().await;
            attach.abort();
            let _ = attach.await;
            aa.close().await.unwrap();
            ba.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_stopped_stream_is_not_delivery() {
        tokio::time::timeout(Duration::from_secs(10), wire_ack_case(true))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_connection_loss_before_ack_is_not_delivery() {
        tokio::time::timeout(Duration::from_secs(10), wire_ack_case(false))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn empty_address_map_does_not_hide_live_incoming_carrier() {
        let transport = BleGattTransport::builder().build();
        let task = tokio::spawn(std::future::pending());
        let carriers = CarrierTasks {
            handles: Arc::new(Mutex::new(vec![task])),
        };
        let result = carriers
            .wait_for_cleanup(&transport, Duration::from_millis(100))
            .await;
        carriers.handles.lock().unwrap()[0].abort();
        assert!(
            result.is_err(),
            "empty address queues must not mask a live carrier: {result:?}"
        );
    }

    #[test]
    fn distinct_acknowledged_rounds_complete_only_after_all_three() {
        let echo = echo();
        for round in [3, 1] {
            echo.acknowledged(round, Ok::<_, String>(None)).unwrap();
            assert!(!echo.complete());
        }
        echo.acknowledged(2, Ok::<_, String>(None)).unwrap();
        assert!(echo.complete());
        echo.check().unwrap();
    }

    #[tokio::test]
    async fn cleanup_joins_finished_carriers_and_rejects_cancelled_task() {
        let transport = BleGattTransport::builder().build();
        let done = tokio::spawn(async {});
        let carriers = CarrierTasks {
            handles: Arc::new(Mutex::new(vec![done])),
        };
        assert_eq!(
            carriers
                .wait_for_cleanup(&transport, Duration::from_secs(1))
                .await
                .unwrap(),
            1
        );
        assert!(carriers.handles.lock().unwrap().is_empty());
        let cancelled = tokio::spawn(std::future::pending());
        cancelled.abort();
        carriers.handles.lock().unwrap().push(cancelled);
        let error = carriers
            .wait_for_cleanup(&transport, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.contains("incoming carrier task failed"), "{error}");
    }

    #[test]
    fn probe_is_multifragment_and_identifies_sender_and_round() {
        let key = SecretKey::generate();
        let first = payload(key.public(), 1);
        assert_eq!(first.len(), 4096);
        assert!(first.starts_with(key.public().as_bytes()));
        assert_ne!(first, payload(key.public(), 2));
    }
}
