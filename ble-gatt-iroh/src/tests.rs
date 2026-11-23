use super::*;
use ble_gatt::datagram::DatagramConfig;
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{CapabilityReport, CharacteristicUuid, PeerLinkConfig, ServiceUuid};

fn managed_peer(
    network: &Arc<MockNetwork>,
    address: &str,
    config: &DatagramConfig,
) -> Arc<PeerLink> {
    PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress(address.into()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        PeerLinkConfig::new(config.clone()),
    )
}

#[tokio::test]
async fn address_rotation_keeps_retired_addresses_bounded_and_forgets_the_oldest() {
    let transport = BleGattTransport::builder().build();
    let id = iroh::SecretKey::generate().public();
    let rotation = |index: usize| PeerAddress(format!("rotation-{index}"));
    for index in 0..=RETIRED_ADDRESS_LIMIT + 1 {
        transport.set_peer_address(id, rotation(index));
        if index == 1 {
            // The first retired address is strictly the oldest.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    let retired = transport.inner.retired.lock().unwrap();
    assert_eq!(retired.len(), RETIRED_ADDRESS_LIMIT);
    assert!(!retired.contains_key(&rotation(0)), "oldest is forgotten");
    assert!(retired.contains_key(&rotation(1)));
    assert!(retired.contains_key(&rotation(RETIRED_ADDRESS_LIMIT)));
    assert!(
        !retired.contains_key(&rotation(RETIRED_ADDRESS_LIMIT + 1)),
        "the current address is dialled"
    );
}

async fn managed_pair(
    server: &Arc<PeerLink>,
    client: &Arc<PeerLink>,
    address: &PeerAddress,
) -> (PeerChannel, PeerChannel) {
    let mut incoming = server.incoming().unwrap();
    server.set_advertising(true).await.unwrap();
    tokio::join!(
        async {
            client
                .request(address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { incoming.next().await.unwrap().accept().await.unwrap() },
    )
}

/// Connects `client` to `server` over IP and accepts it, failing instead
/// of hanging when the handshake does not finish. The server binds the
/// unspecified address, which Windows refuses as a destination
/// (WSAEADDRNOTAVAIL), so this dials the same port on loopback.
async fn connect_on_ip(
    client: &iroh::Endpoint,
    server: &iroh::Endpoint,
    alpn: &[u8],
) -> (iroh::endpoint::Connection, iroh::endpoint::Connection) {
    let address = iroh::EndpointAddr::from_parts(
        server.id(),
        server.bound_sockets().into_iter().map(|mut socket| {
            if socket.ip().is_unspecified() {
                socket.set_ip(match socket {
                    std::net::SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
                    std::net::SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
                });
            }
            TransportAddr::Ip(socket)
        }),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        let (outgoing, accepted) = tokio::join!(client.connect(address, alpn), async {
            server.accept().await.unwrap().await.unwrap()
        });
        (outgoing.unwrap(), accepted)
    })
    .await
    .expect("IP connect and accept must finish")
}

#[test]
fn an_address_round_trips_through_its_custom_form() {
    let peer = PeerAddress("AA:BB:CC:DD:EE:FF".to_string());
    let addr = custom_addr(&peer);
    assert_eq!(addr.id(), TRANSPORT_ID);
    assert_eq!(peer_address(&addr), Some(peer));
}

#[tokio::test]
async fn either_authenticated_key_can_request_a_carrier() {
    use std::sync::atomic::AtomicUsize;
    let mut keys = [
        iroh::SecretKey::generate().public(),
        iroh::SecretKey::generate().public(),
    ];
    keys.sort();
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = attempts.clone();
    let dialer: Dialer = Arc::new(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(ble_gatt::BleError::GattBusy(
                "busy is not unavailable".into(),
            ))
        })
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let peer = PeerAddress("remote".into());
    transport.set_peer_address(keys[0], peer.clone());
    let sender = Sender {
        inner: transport.inner.clone(),
    };
    sender.send(&peer, vec![0xc0]).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "an authenticated key does not control BLE initiation"
    );
}

#[tokio::test]
async fn busy_is_retryable_but_unknown_incoming_sources_are_not_dialled() {
    for known in [false, true] {
        let (called, mut calls) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |_| {
            called.send(()).unwrap();
            Box::pin(async { Err(ble_gatt::BleError::GattBusy("transient contention".into())) })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let peer = PeerAddress("remote".into());
        if known {
            transport.set_peer_address(iroh::SecretKey::generate().public(), peer.clone());
        }
        transport
            .inner
            .incoming_addresses
            .lock()
            .unwrap()
            .insert(peer.clone());
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        sender.send(&peer, vec![0xc0]).unwrap();
        let result = tokio::time::timeout(Duration::from_millis(50), calls.recv()).await;
        assert_eq!(result.is_ok(), known);
        if known {
            tokio::task::yield_now().await;
            sender.send(&peer, vec![0xc0]).unwrap();
            tokio::time::timeout(Duration::from_secs(1), calls.recv())
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn stale_carrier_cleanup_cannot_remove_the_replacement() {
    let transport = BleGattTransport::builder().build();
    let peer = PeerAddress("remote".into());
    let (stale, _stale_rx) = mpsc::channel(1);
    let (current, mut current_rx) = mpsc::channel(1);
    transport
        .inner
        .links
        .lock()
        .unwrap()
        .insert(peer.clone(), current.clone());
    transport.inner.remove_link(&peer, &stale);
    let sender = Sender {
        inner: transport.inner.clone(),
    };
    sender.send(&peer, vec![0xc0]).unwrap();
    assert!(
        matches!(current_rx.recv().await, Some(Outgoing::Packet(packet)) if packet == vec![0xc0])
    );
    transport.inner.remove_link(&peer, &current);
    assert!(transport.linked_peers().is_empty());
}

#[tokio::test]
async fn late_packets_do_not_resurrect_a_released_incoming_carrier() {
    for (initial, routed, active) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
    ] {
        let network = MockNetwork::new();
        let server_address = PeerAddress("server".into());
        let client_address = PeerAddress("client".into());
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let server = managed_peer(&network, &server_address.0, &config);
        let client = managed_peer(&network, &client_address.0, &config);
        let (channel, carrier) = managed_pair(&server, &client, &server_address).await;
        let transport = BleGattTransport::builder().build();
        let id = iroh::SecretKey::generate().public();
        let addr = TransportAddr::Custom(custom_addr(&client_address));
        transport.inner.learn(id, 0, &addr);
        transport.inner.forget_connection(0);
        if active {
            transport.inner.learn(id, 1, &addr);
        }

        let packet = vec![if initial { 0xc0 } else { 0x40 }, 1, 2, 3];
        let mut task = if routed {
            channel.send(packet).await.unwrap();
            let first = carrier.recv().await.unwrap().unwrap();
            transport.attach_after(carrier, first)
        } else {
            let task = transport.attach(carrier);
            channel.send(packet).await.unwrap();
            task
        };
        if !initial && !active {
            tokio::time::timeout(Duration::from_millis(300), &mut task)
                .await
                .expect("late non-Initial traffic cannot keep a released address alive")
                .unwrap();
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut task)
                    .await
                    .is_err(),
                "a new Initial or another active user protects its carrier"
            );
            transport.close_link(&client_address);
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(transport.linked_peers().is_empty());
    }
}

#[tokio::test]
async fn pending_dial_completion_does_not_remove_an_incoming_replacement() {
    for success in [false, true] {
        let network = MockNetwork::new();
        let local = PeerAddress("local".into());
        let remote = PeerAddress("remote".into());
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let a = managed_peer(&network, &local.0, &config);
        let b = managed_peer(&network, &remote.0, &config);
        let (complete, completion) = oneshot::channel::<ble_gatt::Result<PeerChannel>>();
        let completion = Arc::new(Mutex::new(Some(completion)));
        let (started, mut starts) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |_| {
            let completion = completion.lock().unwrap().take().unwrap();
            let started = started.clone();
            Box::pin(async move {
                started.send(()).unwrap();
                completion.await.unwrap()
            })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        sender.send(&remote, vec![0xc0]).unwrap();
        starts.recv().await.unwrap();
        let (peer_carrier, accepted) = managed_pair(&a, &b, &local).await;
        let duplicate = accepted.clone();
        let replacement = transport.attach(accepted);
        if success {
            // A late acquired use is not another native carrier.
            assert!(complete.send(Ok(duplicate)).is_ok());
        } else {
            drop(duplicate);
            assert!(complete
                .send(Err(ble_gatt::BleError::GattBusy(
                    "outgoing attempt lost".into()
                )))
                .is_ok());
        }
        // Queued packets must use the independently accepted carrier.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), peer_carrier.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![0xc0]
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while transport
                .inner
                .closers
                .lock()
                .unwrap()
                .get(&remote)
                .unwrap()
                .len()
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        sender.send(&remote, b"replacement".to_vec()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), peer_carrier.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            b"replacement"
        );
        assert!(!replacement.is_finished());
        transport.close_link(&remote);
        tokio::time::timeout(Duration::from_secs(2), replacement)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn cancelling_a_pending_dial_drops_its_future_and_queue() {
    struct OnDrop(Option<oneshot::Sender<()>>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let (dropped, drop_seen) = oneshot::channel();
    let dropped = Arc::new(Mutex::new(Some(dropped)));
    let (started, mut starts) = mpsc::unbounded_channel();
    let dialer: Dialer = Arc::new(move |_| {
        let probe = OnDrop(dropped.lock().unwrap().take());
        let started = started.clone();
        Box::pin(async move {
            let _probe = probe;
            started.send(()).unwrap();
            std::future::pending().await
        })
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let remote = PeerAddress("remote".into());
    Sender {
        inner: transport.inner.clone(),
    }
    .send(&remote, vec![0xc0])
    .unwrap();
    starts.recv().await.unwrap();
    transport.close_link(&remote);
    tokio::time::timeout(Duration::from_secs(1), drop_seen)
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    assert!(transport.linked_peers().is_empty());
    assert!(transport.inner.closers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn closing_a_published_queue_before_the_dial_task_starts_cancels_it() {
    use std::sync::atomic::AtomicUsize;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let dialer: Dialer = Arc::new(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let peer = PeerAddress("publication-race".into());
    let closing = transport.clone();
    let address = peer.clone();
    *transport.inner.published.lock().unwrap() = Some(Box::new(move || {
        assert_eq!(closing.linked_peers(), vec![address.clone()]);
        closing.close_link(&address);
    }));
    Sender {
        inner: transport.inner.clone(),
    }
    .send(&peer, vec![1])
    .unwrap();
    tokio::task::yield_now().await;
    assert!(
        transport.linked_peers().is_empty(),
        "close must see the published queue's cancellation signal"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "cancelled work must not start the dialer"
    );
    assert!(transport.inner.closers.lock().unwrap().is_empty());
}

async fn received_packet(endpoint: &mut dyn CustomEndpoint, peer: &PeerAddress) -> Vec<u8> {
    let mut bytes = [0; 256];
    let mut meta = [noq_udp::RecvMeta::default()];
    let mut info = [RecvInfo::new(custom_addr(peer), None)];
    std::future::poll_fn(|cx| {
        endpoint.poll_recv(
            cx,
            &mut [io::IoSliceMut::new(&mut bytes)],
            &mut meta,
            &mut info,
        )
    })
    .await
    .unwrap();
    bytes[..meta[0].len].to_vec()
}

#[tokio::test]
async fn retired_generation_cleanup_cannot_suppress_or_remove_a_live_replacement() {
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let network = MockNetwork::new();
    let server = managed_peer(&network, "generation-server", &config);
    let client = managed_peer(&network, "generation-client", &config);
    let server_address = PeerAddress("generation-server".into());
    let client_address = PeerAddress("generation-client".into());
    let mut incoming = server.incoming().unwrap();
    server.set_advertising(true).await.unwrap();
    let (old_remote, old) = tokio::join!(
        async {
            client
                .request(&server_address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { incoming.next().await.unwrap().accept().await.unwrap() }
    );
    let transport = BleGattTransport::builder().build();
    let mut endpoint = transport.bind().unwrap();
    let sender = Sender {
        inner: transport.inner.clone(),
    };
    let (reached, retiring) = oneshot::channel();
    let resume = Arc::new(Notify::new());
    *transport.inner.retiring.lock().unwrap() = Some((reached, resume.clone()));
    let old_task = transport.attach(old.clone());
    old_remote.send(vec![0, 0]).await.unwrap();
    assert_eq!(
        received_packet(endpoint.as_mut(), &client_address).await,
        vec![0, 0]
    );
    old.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), retiring)
        .await
        .unwrap()
        .unwrap();
    // A has closed its receive queue but cannot finish scoped release yet.
    // Drain the client's generation without resuming the old server adapter task.
    old_remote.release().await.unwrap();
    let (remote, new) = tokio::join!(
        async {
            client
                .request(&server_address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { incoming.next().await.unwrap().accept().await.unwrap() }
    );
    let first_new_task = transport.attach(new.clone());
    let second_new_task = transport.attach(new.clone());
    let new_task = transport.attach(new);
    for number in 1..=3u8 {
        sender.send(&client_address, vec![1, number]).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), remote.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![1, number]
        );
        remote.send(vec![0, number]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                received_packet(endpoint.as_mut(), &client_address)
            )
            .await
            .unwrap(),
            vec![0, number]
        );
    }
    resume.notify_one();
    old_task.await.unwrap();
    first_new_task.await.unwrap();
    second_new_task.await.unwrap();
    for number in 4..=6u8 {
        sender.send(&client_address, vec![1, number]).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), remote.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![1, number]
        );
        remote.send(vec![0, number]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                received_packet(endpoint.as_mut(), &client_address)
            )
            .await
            .unwrap(),
            vec![0, number]
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), remote.recv())
            .await
            .is_err()
    );
    assert!(tokio::time::timeout(
        Duration::from_millis(50),
        received_packet(endpoint.as_mut(), &client_address)
    )
    .await
    .is_err());
    assert!(!new_task.is_finished());
    transport.close_link(&client_address);
    tokio::time::timeout(Duration::from_secs(2), new_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn explicit_destination_hints_preserve_identity_validation_and_dial_guards() {
    let (called, mut calls) = mpsc::unbounded_channel();
    let dialer: Dialer = Arc::new(move |peer| {
        called.send(peer).unwrap();
        Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let id = iroh::SecretKey::generate().public();
    let authenticated_id = iroh::SecretKey::generate().public();
    let current = PeerAddress("hint-current".into());
    let retired = PeerAddress("hint-retired".into());
    let private = PeerAddress("hint-private-source".into());
    let supplied = PeerAddress("hint-unlearned".into());
    transport.set_peer_address(id, retired.clone());
    transport.set_peer_address(id, current.clone());
    transport.inner.learn(
        authenticated_id,
        0,
        &TransportAddr::Custom(custom_addr(&supplied)),
    );
    transport
        .inner
        .incoming_addresses
        .lock()
        .unwrap()
        .insert(private.clone());
    for peer in [&current, &retired, &private, &supplied] {
        transport
            .inner
            .acquisition
            .lock()
            .unwrap()
            .entry(peer.clone())
            .or_default()
            .terminal = true;
    }
    let remote = iroh::EndpointAddr::from_parts(
        id,
        [
            TransportAddr::Custom(custom_addr(&current)),
            TransportAddr::Custom(custom_addr(&retired)),
            TransportAddr::Custom(custom_addr(&private)),
            TransportAddr::Custom(custom_addr(&supplied)),
            TransportAddr::Custom(CustomAddr::from_parts(TRANSPORT_ID, &[0xff])),
            TransportAddr::Custom(CustomAddr::from_parts(0x20, b"other-transport")),
            TransportAddr::Ip("127.0.0.1:12345".parse().unwrap()),
        ],
    );
    transport
        .hooks()
        .before_connect(&remote, b"fresh-user")
        .await;
    {
        let acquisition = transport.inner.acquisition.lock().unwrap();
        assert_eq!(
            acquisition.len(),
            4,
            "invalid and non-BLE destinations are ignored"
        );
        for peer in [&current, &retired, &private, &supplied] {
            assert!(!acquisition[peer].terminal);
            assert_eq!(
                acquisition[peer].generation, 1,
                "mapped and supplied duplicates authorize once"
            );
        }
    }
    {
        let learned = transport.inner.learned.lock().unwrap();
        assert_eq!(
            learned.len(),
            1,
            "caller hints create no authenticated identities"
        );
        assert_eq!(
            learned[&supplied].endpoint, authenticated_id,
            "hints cannot overwrite authenticated identity"
        );
    }
    assert_eq!(transport.inner.peers.lock().unwrap().len(), 1);
    assert_eq!(transport.inner.peers.lock().unwrap()[&id], current);
    let sender = Sender {
        inner: transport.inner.clone(),
    };
    for peer in [&retired, &private] {
        sender.send(peer, vec![0xc0, 1]).unwrap();
    }
    assert!(
        transport.linked_peers().is_empty(),
        "authorization does not remove dial guards"
    );
    assert!(matches!(
        calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    sender.send(&supplied, vec![0x40, 2]).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), calls.recv())
            .await
            .unwrap()
            .unwrap(),
        supplied
    );
}

#[tokio::test]
async fn explicit_destination_recovers_after_authenticated_history_drains() {
    use iroh::endpoint::presets;

    const ALPN: &[u8] = b"explicit-history";
    let network = MockNetwork::new();
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let address = PeerAddress("history-server".into());
    let server_link = managed_peer(&network, &address.0, &config);
    let client_link = managed_peer(&network, "history-client", &config);
    let server_transport = BleGattTransport::builder().build();
    let mut incoming = server_link.incoming().unwrap();
    server_link.set_advertising(true).await.unwrap();
    let (attached, mut channels) = mpsc::unbounded_channel();
    let attaching = tokio::spawn({
        let transport = server_transport.clone();
        async move {
            while let Some(request) = incoming.next().await {
                let channel = request.accept().await.unwrap();
                transport.attach(channel.clone());
                attached.send(channel).unwrap();
            }
        }
    });
    let server = iroh::Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .alpns(vec![ALPN.to_vec()])
        .add_custom_transport(Arc::new(server_transport.clone()))
        .hooks(server_transport.hooks())
        .bind()
        .await
        .unwrap();
    let transport = BleGattTransport::builder()
        .dialer(dial_with(client_link.clone()))
        .build();
    let client = iroh::Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(transport.clone()))
        .hooks(transport.hooks())
        .bind()
        .await
        .unwrap();
    let remote =
        iroh::EndpointAddr::from_parts(server.id(), [TransportAddr::Custom(custom_addr(&address))]);
    let connect = || async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (outgoing, accepted) = tokio::join!(client.connect(remote.clone(), ALPN), async {
                server.accept().await.unwrap().await.unwrap()
            });
            (outgoing.unwrap(), accepted)
        })
        .await
        .unwrap()
    };
    let (first, accepted) = connect().await;
    let channel = channels.recv().await.unwrap();
    assert_eq!(first.remote_id(), server.id());
    assert_eq!(
        transport
            .inner
            .learned
            .lock()
            .unwrap()
            .get(&address)
            .unwrap()
            .endpoint,
        server.id(),
        "real TLS handshake established the learned identity"
    );
    let generation = transport.inner.acquisition.lock().unwrap()[&address].generation;
    let (second, other_accepted) = connect().await;
    assert_eq!(
        transport.inner.acquisition.lock().unwrap()[&address].generation,
        generation,
        "a fresh explicit user shares a healthy carrier's authorization"
    );
    assert!(matches!(
        channels.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    second
        .send_datagram(b"shared carrier".to_vec().into())
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), other_accepted.read_datagram())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"shared carrier"
    );
    channel.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !transport.linked_peers().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for connection in [&first, &second, &accepted, &other_accepted] {
        connection.close(0u32.into(), b"done");
    }
    drop((first, second, accepted, other_accepted));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !transport.inner.learned.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("authenticated connection history must actually drain");
    assert!(transport.inner.addresses_of(&server.id()).is_empty());
    assert!(transport.inner.acquisition.lock().unwrap()[&address].terminal);
    let (replacement, replacement_accepted) = connect().await;
    replacement
        .send_datagram(b"after drain".to_vec().into())
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), replacement_accepted.read_datagram())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"after drain"
    );
    replacement.close(0u32.into(), b"done");
    drop((replacement, replacement_accepted));
    client.close().await;
    server.close().await;
    attaching.abort();
    let _ = attaching.await;
    client_link.close().await.unwrap();
    server_link.close().await.unwrap();
}

#[tokio::test]
async fn explicit_authorization_survives_a_terminal_carriers_delayed_publication() {
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let network = MockNetwork::new();
    let server = managed_peer(&network, "authorization-server", &config);
    let client = managed_peer(&network, "authorization-client", &config);
    let (remote, channel) = managed_pair(
        &server,
        &client,
        &PeerAddress("authorization-server".into()),
    )
    .await;
    let peer = channel.peer();
    let (called, mut calls) = mpsc::unbounded_channel();
    let dialer: Dialer = Arc::new(move |peer| {
        called.send(peer).unwrap();
        Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let id = iroh::SecretKey::generate().public();
    transport.set_peer_address(id, peer.clone());
    let mut endpoint = transport.bind().unwrap();
    let (reached, terminating) = oneshot::channel();
    let resume = Arc::new(Notify::new());
    *transport.inner.terminating.lock().unwrap() = Some((reached, resume.clone()));
    let task = transport.attach(channel.clone());
    remote.send(vec![0, 1]).await.unwrap();
    assert_eq!(received_packet(endpoint.as_mut(), &peer).await, vec![0, 1]);
    // Core has published the terminal reason, but the old adapter task
    // has not yet published suppression or closed its packet queue.
    remote.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), terminating)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::PeerDisconnect)
    );
    assert!(matches!(
        transport
            .hooks()
            .before_connect(&iroh::EndpointAddr::from(id), b"new-user")
            .await,
        BeforeConnectOutcome::Accept
    ));
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    Sender {
        inner: transport.inner.clone(),
    }
    .send(&peer, vec![0x40, 2])
    .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), calls.recv())
            .await
            .expect("fresh explicit authorization must survive the old task's terminal publication")
            .unwrap(),
        peer
    );
}

// Hookless negative contract coverage: peer scoped release becomes a wire
// Disconnect, indistinguishable here from explicit peer disconnect. The
// shared integration endpoint helper installs hooks; this test deliberately
// withholds the fresh hook until after proving packet-only suppression.
#[tokio::test]
async fn peer_idle_release_needs_fresh_authorization_for_reverse_traffic() {
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let network = MockNetwork::new();
    let server = managed_peer(&network, "idle-auth-server", &config);
    let client = managed_peer(&network, "idle-auth-client", &config);
    let (remote, channel) =
        managed_pair(&server, &client, &PeerAddress("idle-auth-server".into())).await;
    let peer = channel.peer();
    let (called, mut calls) = mpsc::unbounded_channel();
    let dialer: Dialer = Arc::new(move |peer| {
        called.send(peer).unwrap();
        Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let id = iroh::SecretKey::generate().public();
    transport.set_peer_address(id, peer.clone());
    let mut endpoint = transport.bind().unwrap();
    let task = transport.attach(channel.clone());
    remote.send(vec![0, 1]).await.unwrap();
    assert_eq!(received_packet(endpoint.as_mut(), &peer).await, vec![0, 1]);
    // Exactly the action performed by the peer's idle packet-task exit.
    remote.release().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::PeerDisconnect)
    );
    let sender = Sender {
        inner: transport.inner.clone(),
    };
    for bytes in [vec![0xc0, 1], vec![0x40, 2]] {
        sender.send(&peer, bytes).unwrap();
    }
    assert!(
        transport
            .inner
            .acquisition
            .lock()
            .unwrap()
            .get(&peer)
            .unwrap()
            .terminal
    );
    assert!(transport.linked_peers().is_empty());
    assert!(matches!(
        calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    // Only an authoritative new application action clears this state;
    // packets alone above deliberately had no fresh hook invocation.
    transport
        .hooks()
        .before_connect(&iroh::EndpointAddr::from(id), b"reverse")
        .await;
    sender.send(&peer, vec![0x40, 3]).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), calls.recv())
            .await
            .unwrap()
            .unwrap(),
        peer
    );
}

#[tokio::test]
async fn queued_old_generation_barrier_cannot_report_delivery_after_replacement() {
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let network = MockNetwork::new();
    let server = managed_peer(&network, "barrier-server", &config);
    let client = managed_peer(&network, "barrier-client", &config);
    let (_remote, channel) =
        managed_pair(&server, &client, &PeerAddress("barrier-server".into())).await;
    let transport = BleGattTransport::builder().build();
    let peer = channel.peer();
    let first = transport.attach(channel.clone());
    let (reached, barrier) = oneshot::channel();
    let old_queue = transport
        .inner
        .links
        .lock()
        .unwrap()
        .get(&peer)
        .unwrap()
        .clone();
    old_queue.try_send(Outgoing::Packet(vec![0, 1])).unwrap();
    old_queue.try_send(Outgoing::Barrier(reached)).unwrap();
    let second = transport.attach(channel.clone());
    let third = transport.attach(channel);
    transport.close_link(&peer);
    for task in [first, second, third] {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        barrier.await.is_err(),
        "discarded old-generation data is not close-delivery proof"
    );
    assert!(transport.inner.dropped.lock().unwrap().contains_key(&peer));
    assert!(transport.linked_peers().is_empty());
}

#[tokio::test]
async fn failed_queued_close_barrier_cannot_report_close_sent() {
    use iroh::endpoint::presets;
    const ALPN: &[u8] = b"failed-close-barrier";
    let server = iroh::Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let client = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    let (connection, accepted) = connect_on_ip(&client, &server, ALPN).await;
    let transport = BleGattTransport::builder().build();
    let peer = PeerAddress("failed-barrier".into());
    transport
        .inner
        .learn(server.id(), 0, &TransportAddr::Custom(custom_addr(&peer)));
    let (queue, mut pending) = mpsc::channel(4);
    queue.try_send(Outgoing::Packet(vec![0, 7])).unwrap();
    transport.inner.links.lock().unwrap().insert(peer, queue);
    let weak = connection.weak_handle();
    connection.close(0u32.into(), b"done");
    let inner = transport.inner.clone();
    let mut proof = tokio::spawn(async move { inner.close_sent(&weak, 0, Instant::now()).await });
    assert!(matches!(pending.recv().await, Some(Outgoing::Packet(bytes)) if bytes == vec![0, 7]));
    let Outgoing::Barrier(reached) = tokio::time::timeout(Duration::from_secs(2), pending.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected close barrier")
    };
    drop(reached);
    drop(pending);
    // Failure can finish false if QUIC already drained, but never true.
    if let Ok(result) = tokio::time::timeout(Duration::from_millis(50), &mut proof).await {
        assert!(!result.unwrap());
    } else {
        drop(connection);
        drop(accepted);
        client.close().await;
        server.close().await;
        assert!(!tokio::time::timeout(Duration::from_secs(5), proof)
            .await
            .unwrap()
            .unwrap());
    }
}

// iroh exposes no public close-path operation. Replay its documented
// post-Closed snapshot at the same reconciliation seam used by hooks,
// while real QUIC stays alive on IP and a real mock GATT task idles.
// This is not a claim of physically induced multipath closure.
async fn historical_path_runtime(survivor: bool) {
    use iroh::endpoint::presets;

    const ALPN: &[u8] = b"path-ownership-regression";
    let server = iroh::Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let client = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    let (connection, accepted) = connect_on_ip(&client, &server, ALPN).await;
    let surviving_connection = if survivor {
        let (other, incoming) = connect_on_ip(&client, &server, ALPN).await;
        assert_ne!(connection.stable_id(), other.stable_id());
        Some((other, incoming))
    } else {
        None
    };
    assert!(connection
        .paths()
        .iter()
        .any(|path| matches!(path.remote_addr(), TransportAddr::Ip(_))));

    let network = MockNetwork::new();
    let local = PeerAddress("idle-server".into());
    let remote = PeerAddress("historical-client".into());
    let config = DatagramConfig::new(
        ServiceUuid(uuid::Uuid::new_v4()),
        CharacteristicUuid(uuid::Uuid::new_v4()),
    );
    let gatt_server = managed_peer(&network, &local.0, &config);
    let gatt_client = managed_peer(&network, &remote.0, &config);
    let (_carrier, accepted_carrier) = managed_pair(&gatt_server, &gatt_client, &local).await;
    let transport = BleGattTransport::builder()
        .link_idle_timeout(Duration::from_millis(80))
        .build();
    let ble = TransportAddr::Custom(custom_addr(&remote));
    let current: Vec<_> = connection
        .paths()
        .iter()
        .map(|path| path.remote_addr().clone())
        .collect();
    // Prior snapshot had both transports. Closing the BLE path does NOT
    // end this connection, close the carrier, or erase its history.
    transport
        .inner
        .reconcile_paths(server.id(), 0, current.iter().cloned().chain([ble.clone()]));
    if survivor {
        // A different connection still has a path at the same address.
        transport.inner.reconcile_paths(server.id(), 1, [ble]);
    }
    let mut task = transport.attach(accepted_carrier);
    transport.inner.reconcile_paths(server.id(), 0, current);
    if survivor {
        assert!(
            tokio::time::timeout(Duration::from_millis(350), &mut task)
                .await
                .is_err(),
            "another connection's current BLE path must protect the carrier"
        );
        let (other, incoming) = surviving_connection.as_ref().unwrap();
        other.send_datagram(b"survivor".to_vec().into()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), incoming.read_datagram())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"survivor"
        );
        // The survivor also moves away, without closing the carrier.
        transport.inner.reconcile_paths(server.id(), 1, []);
    }
    tokio::time::timeout(Duration::from_millis(700), &mut task)
        .await
        .expect("historical BLE ownership must not prevent idle expiry")
        .unwrap();
    assert!(transport.linked_peers().is_empty());
    assert!(
        transport
            .inner
            .learned
            .lock()
            .unwrap()
            .get(&remote)
            .unwrap()
            .connections
            .contains(&0),
        "history must remain for close barriers and ended-packet suppression"
    );
    assert!(connection.close_reason().is_none());
    connection
        .send_datagram(b"still alive on IP".to_vec().into())
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), accepted.read_datagram())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"still alive on IP"
    );
    connection.close(0u32.into(), b"done");
}

#[tokio::test]
async fn historical_ble_path_expires_while_quic_survives_on_ip() {
    historical_path_runtime(false).await;
}

#[tokio::test]
async fn historical_ble_path_keeps_a_different_connections_surviving_path() {
    historical_path_runtime(true).await;
}

#[test]
fn another_transports_address_is_not_a_peer() {
    let addr = CustomAddr::from_parts(0x20, b"AA:BB:CC:DD:EE:FF");
    assert_eq!(peer_address(&addr), None);
}
