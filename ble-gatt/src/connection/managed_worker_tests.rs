use super::*;
fn worker() -> Worker {
    let (commands, receiver) = mpsc::channel(4);
    let (incoming, _) = mpsc::channel(1);
    Worker::new(
        EndpointId::from_bytes([1; 16]),
        PeerLinkConfig::new(datagram::DatagramConfig::new(
            crate::ServiceUuid(uuid::Uuid::from_u128(11)),
            crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
        )),
        None,
        commands,
        receiver,
        incoming,
        Arc::new(Mutex::new(HashMap::new())),
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn crossed_retirement_preserves_individual_loser_reply_and_aggregate_owners() {
    for cleanup_first in [true, false] {
        for winner_first in [true, false] {
            let mut worker = worker();
            let peer = PeerAddress("crossed-retained".into());
            let endpoint = EndpointId::from_bytes([2; 16]);
            for generation in [1, 2] {
                let mut attempt =
                    worker.attempt(peer.clone(), generation == 1, CancellationToken::new());
                attempt.endpoint = Some(endpoint);
                worker.attempts.insert(generation, attempt);
            }
            worker.acquired.insert(peer.clone(), 1);
            let (reply, mut individual) = oneshot::channel();
            worker.retire(vec![1], AttemptError::Superseded, Some(reply));
            let (reply, winner_reply) = oneshot::channel();
            worker.retire(vec![2], AttemptError::PeerDisconnected, Some(reply));
            assert!(
                matches!(
                    individual.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "winner retirement dropped loser's retained reply before its cleanup/transport"
            );
            let (reply, mut aggregate) = oneshot::channel();
            worker.retire(
                worker.endpoint_generations(endpoint),
                AttemptError::Disconnected,
                Some(reply),
            );
            let mut pending = |worker: &Worker| {
                assert!(matches!(
                    aggregate.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                assert_eq!(
                    worker.endpoint_generations(endpoint).len(),
                    if winner_first { 1 } else { 2 }
                );
            };
            if winner_first {
                worker.retired(
                    vec![(peer.clone(), 2)],
                    Err(BleError::Gatt("winner transport".into())),
                );
            }
            if cleanup_first {
                worker.cleaned(
                    peer.clone(),
                    1,
                    Err(BleError::Unsupported("loser cleanup".into())),
                );
            } else {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            }
            assert!(matches!(
                individual.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            pending(&worker);
            if cleanup_first {
                worker.retired(vec![(peer.clone(), 1)], Ok(()));
            } else {
                worker.cleaned(
                    peer.clone(),
                    1,
                    Err(BleError::Unsupported("loser cleanup".into())),
                );
            }
            assert!(
                matches!(individual.await.unwrap(), Err(BleError::Unsupported(message)) if message == "loser cleanup")
            );
            if !winner_first {
                assert!(matches!(
                    aggregate.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                // Stale A notifications cannot remove B, replace its error or decrement twice.
                worker.retired(vec![(peer.clone(), 1)], Err(BleError::Gatt("stale".into())));
                worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale".into())));
                assert_eq!(worker.endpoint_generations(endpoint), vec![2]);
                worker.retired(
                    vec![(peer.clone(), 2)],
                    Err(BleError::Gatt("winner transport".into())),
                );
            }
            assert!(
                matches!(winner_reply.await.unwrap(), Err(BleError::Gatt(message)) if message == "winner transport"),
                "A's cleanup error cannot overwrite B's transport outcome"
            );
            let aggregate = aggregate.await.unwrap();
            if winner_first {
                assert!(
                    matches!(aggregate, Err(BleError::Unsupported(message)) if message == "loser cleanup")
                );
            } else {
                assert!(
                    matches!(aggregate, Err(BleError::Gatt(message)) if message == "winner transport")
                );
            }
            assert!(worker.endpoint_generations(endpoint).is_empty());
        }
    }
}

#[tokio::test]
async fn crossed_retirement_counts_cleanup_and_waiter_references_by_owner() {
    let mut worker = worker();
    let peer = PeerAddress("crossed-budget".into());
    worker.acquired.insert(peer.clone(), 1);
    worker.attempts.insert(
        2,
        worker.attempt(peer.clone(), false, CancellationToken::new()),
    );
    assert_eq!(
        worker.link_count(),
        2,
        "B cannot hide acquired A's link budget"
    );
    worker.retire(vec![2], AttemptError::PeerDisconnected, None);
    assert_eq!(
        worker.link_count(),
        2,
        "retiring B cannot hide acquired A either"
    );
    worker.retiring.insert(
        (peer.clone(), 1),
        Retirement {
            generation: 1,
            endpoint: None,
            complete: false,
            waiters: Vec::new(),
            error: None,
            inbound: None,
        },
    );
    assert_eq!(
        worker.link_count(),
        2,
        "matching acquired A is counted once"
    );
    let operations = worker.operations.len();
    let mut receivers = Vec::new();
    for _ in 0..MAX_PENDING_OPERATIONS / 2 {
        let (reply, receiver) = oneshot::channel();
        worker.retire(vec![1, 2], AttemptError::Disconnected, Some(reply));
        receivers.push(receiver);
    }
    assert_eq!(
        worker
            .retiring
            .values()
            .map(|r| r.waiters.len())
            .sum::<usize>(),
        MAX_PENDING_OPERATIONS
    );
    let (reply, receiver) = oneshot::channel();
    worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
    assert!(matches!(
        receiver.await.unwrap(),
        Err(BleError::GattBusy(_))
    ));
    drop(receivers.pop());
    let (reply, receiver) = oneshot::channel();
    worker.retire(vec![1, 2], AttemptError::Disconnected, Some(reply));
    receivers.push(receiver);
    assert_eq!(
        worker
            .retiring
            .values()
            .map(|r| r.waiters.len())
            .sum::<usize>(),
        MAX_PENDING_OPERATIONS
    );
    assert_eq!(
        worker.operations.len(),
        operations,
        "joining owners cannot allocate native operations"
    );
    worker.retired(vec![(peer.clone(), 2)], Ok(()));
    worker.cleaned(peer.clone(), 1, Ok(()));
    for receiver in &mut receivers {
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    worker.retired(vec![(peer, 1)], Ok(()));
    for receiver in receivers {
        receiver.await.unwrap().unwrap();
    }
    assert_eq!(worker.link_count(), 0);
    for generation in 1..=MAX_PENDING_OPERATIONS {
        let peer = PeerAddress(format!("budget-{generation}"));
        worker.attempts.insert(
            generation as u64,
            worker.attempt(peer, true, CancellationToken::new()),
        );
    }
    assert!(
        !worker.operation_capacity(),
        "native operation cap remains 64"
    );
}

#[tokio::test]
async fn shutdown_answers_every_crossed_retirement_owner_and_shared_aggregate() {
    for abort in [false, true] {
        let mut worker = worker();
        let peer = PeerAddress("shutdown-crossed".into());
        let endpoint = EndpointId::from_bytes([2; 16]);
        for generation in [1, 2] {
            let mut attempt =
                worker.attempt(peer.clone(), generation == 1, CancellationToken::new());
            attempt.endpoint = Some(endpoint);
            worker.attempts.insert(generation, attempt);
        }
        worker.acquired.insert(peer, 1);
        let (reply, individual) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Released, Some(reply));
        let (reply, aggregate) = oneshot::channel();
        worker.retire(
            worker.endpoint_generations(endpoint),
            AttemptError::Closed,
            Some(reply),
        );
        worker.operations.spawn(std::future::pending());
        worker.driver = Some(Arc::new(crate::drivers::mock::MockDriver::new(
            PeerAddress("shutdown-worker".into()),
            crate::drivers::mock::MockNetwork::new(),
            crate::CapabilityReport {
                central: true,
                peripheral: true,
            },
        )));
        worker.shutdown.cancel();
        if abort {
            worker.operations.abort_all();
        }
        worker.run().await;
        let reason = if abort {
            AttemptError::Cancelled
        } else {
            AttemptError::TimedOut
        };
        for receiver in [individual, aggregate] {
            assert!(
                matches!(receiver.await, Ok(Err(BleError::Attempt(actual))) if actual == reason)
            );
        }
    }
}

#[tokio::test]
async fn crossed_retirement_transfers_one_held_inbound_until_all_address_owners_finish() {
    use crate::drivers::mock::{MockDriver, MockNetwork};
    for first in [1, 2] {
        let mut worker = worker();
        let peer = PeerAddress("held-crossed-peer".into());
        worker.next_generation = 2;
        worker.acquired.insert(peer.clone(), 1);
        for generation in [1, 2] {
            worker.attempts.insert(
                generation,
                worker.attempt(peer.clone(), generation == 1, CancellationToken::new()),
            );
        }
        worker.retire(vec![1], AttemptError::Superseded, None);
        worker.retire(vec![2], AttemptError::PeerDisconnected, None);
        let mut servers = Vec::new();
        // Two independent raw sessions with the same address exercise the
        // address-scoped holding cap without one replacing the other's HAL.
        for _ in 0..2 {
            let network = MockNetwork::new();
            let caps = crate::CapabilityReport {
                central: true,
                peripheral: true,
            };
            let local = Arc::new(MockDriver::new(
                PeerAddress("held-local".into()),
                network.clone(),
                caps,
            ));
            let remote = Arc::new(MockDriver::new(peer.clone(), network, caps));
            servers.push(worker.config.datagram.serve_driver(remote).await.unwrap());
            let channel = DatagramChannel::connect_driver(local, &peer, &worker.config.datagram)
                .await
                .unwrap();
            worker.inbound(channel);
            assert!(worker.attempts.is_empty());
            assert_eq!(
                worker
                    .retiring
                    .values()
                    .filter(|r| r.inbound.is_some())
                    .count(),
                1
            );
        }
        // Force the held channel onto the first completed owner so both
        // removal orders exercise transfer, regardless of HashMap iteration.
        let held = worker
            .retiring
            .values_mut()
            .find_map(|r| r.inbound.take())
            .unwrap();
        worker
            .retiring
            .get_mut(&(peer.clone(), first))
            .unwrap()
            .inbound = Some(held);
        if first == 1 {
            worker.cleaned(peer.clone(), 1, Ok(()));
        }
        worker.retired(vec![(peer.clone(), first)], Ok(()));
        assert!(
            worker.attempts.is_empty(),
            "other retirement must still exclude address admission"
        );
        assert!(worker.address_retiring(&peer));
        assert_eq!(
            worker
                .retiring
                .values()
                .filter(|r| r.inbound.is_some())
                .count(),
            1
        );
        let last = if first == 1 { 2 } else { 1 };
        worker.retired(vec![(peer.clone(), last)], Ok(()));
        if last == 1 {
            assert!(
                worker.attempts.is_empty(),
                "A transport cannot bypass matching acquired cleanup"
            );
            worker.cleaned(peer.clone(), 1, Ok(()));
        }
        assert!(!worker.address_retiring(&peer));
        assert_eq!(worker.attempts.len(), 1);
        assert!(worker.attempts.contains_key(&3));
        let ended = worker.attempts[&3].ended.clone();
        worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale".into())));
        worker.retired(vec![(peer, 2)], Err(BleError::Gatt("stale".into())));
        assert!(
            !ended.is_cancelled(),
            "old completions must not close admitted replacement"
        );
        worker.shutdown.cancel();
        for attempt in worker.attempts.values() {
            if let Some(task) = &attempt.task {
                task.abort();
            }
        }
    }
}

#[tokio::test]
async fn retirement_waiters_are_bounded_and_cancelled_slots_are_reclaimed() {
    let mut worker = worker();
    let peer = PeerAddress("bounded-waiters".into());
    let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
    worker.attempts.insert(1, attempt);
    worker.acquired.insert(peer.clone(), 1);
    worker.retire(vec![1], AttemptError::PeerDisconnected, None);
    let operations = worker.operations.len();
    let mut results = Vec::new();
    for _ in 0..MAX_PENDING_OPERATIONS {
        let (reply, result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        results.push(result);
    }
    let (reply, result) = oneshot::channel();
    worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
    assert!(matches!(result.await.unwrap(), Err(BleError::GattBusy(_))));
    drop(results.pop());
    for _ in 0..100 {
        let (reply, result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        assert_eq!(
            worker.retiring[&(peer.clone(), 1)].waiters.len(),
            MAX_PENDING_OPERATIONS
        );
        drop(result);
    }
    assert_eq!(
        worker.operations.len(),
        operations,
        "joining cannot spawn unbounded waiter tasks"
    );
    worker.retired(vec![(peer.clone(), 1)], Ok(()));
    for result in &mut results {
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    worker.cleaned(peer.clone(), 1, Ok(()));
    for result in results {
        result.await.unwrap().unwrap();
    }
    assert!(!worker.address_retiring(&peer));
}

#[tokio::test]
async fn stale_retirement_and_cleanup_cannot_acknowledge_or_remove_another_owner() {
    let mut worker = worker();
    let peer = PeerAddress("stale-owner".into());
    let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
    worker.attempts.insert(2, attempt);
    worker.acquired.insert(peer.clone(), 2);
    worker.retire(vec![2], AttemptError::PeerDisconnected, None);
    let (reply, mut result) = oneshot::channel();
    worker.retire(vec![2], AttemptError::Released, Some(reply));
    let (stale, stale_result) = oneshot::channel();
    worker.retire(vec![1], AttemptError::Disconnected, Some(stale));
    stale_result.await.unwrap().unwrap();
    worker.retired(
        vec![(peer.clone(), 1)],
        Err(BleError::Unsupported("stale failure".into())),
    );
    worker.cleaned(peer.clone(), 1, Err(BleError::Gatt("stale cleanup".into())));
    assert_eq!(worker.acquired.get(&peer), Some(&2));
    assert!(worker.cleanup_failure.is_none());
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    worker.retired(vec![(peer.clone(), 2)], Ok(()));
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    worker.cleaned(peer, 2, Ok(()));
    result.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_timeout_answers_pending_retirement_with_typed_timeout() {
    for abort in [false, true] {
        let mut worker = worker();
        let peer = PeerAddress("shutdown-retirement".into());
        let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        worker.attempts.insert(1, attempt);
        worker.acquired.insert(peer, 1);
        let (reply, result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Released, Some(reply));
        worker.operations.spawn(std::future::pending());
        worker.driver = Some(Arc::new(crate::drivers::mock::MockDriver::new(
            PeerAddress("shutdown-worker".into()),
            crate::drivers::mock::MockNetwork::new(),
            crate::CapabilityReport {
                central: true,
                peripheral: true,
            },
        )));
        worker.shutdown.cancel();
        if abort {
            worker.operations.abort_all();
        }
        worker.run().await;
        let reason = if abort {
            AttemptError::Cancelled
        } else {
            AttemptError::TimedOut
        };
        assert!(matches!(result.await, Ok(Err(BleError::Attempt(actual))) if actual == reason));
    }
}

#[tokio::test]
async fn endpoint_disconnect_joins_only_its_known_retirement() {
    let mut worker = worker();
    let endpoint = EndpointId::from_bytes([9; 16]);
    let peer = PeerAddress("endpoint-retirement".into());
    let mut attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
    attempt.endpoint = Some(endpoint);
    worker.attempts.insert(1, attempt);
    worker.acquired.insert(peer.clone(), 1);
    drop(worker.finish(1, AttemptError::PeerDisconnected));
    let generations = worker.endpoint_generations(endpoint);
    let (reply, mut result) = oneshot::channel();
    worker.retire(generations, AttemptError::Disconnected, Some(reply));
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(worker
        .endpoint_generations(EndpointId::from_bytes([8; 16]))
        .is_empty());
    worker.retired(vec![(peer.clone(), 1)], Ok(()));
    worker.cleaned(peer, 1, Ok(()));
    result.await.unwrap().unwrap();
}

#[tokio::test]
async fn retirement_reply_preserves_transport_failure() {
    for cleanup_first in [true, false] {
        let mut worker = worker();
        let peer = PeerAddress("transport-refusal".into());
        let mut attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        let (transport, mut commands) = mpsc::channel(4);
        attempt.transport = Some(transport);
        worker.attempts.insert(1, attempt);
        worker.acquired.insert(peer.clone(), 1);
        let (reply, mut result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        let handler = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                match command {
                    TransportCommand::Send(_, Some(reply)) => {
                        let _ = reply.send(Ok(()));
                    }
                    TransportCommand::Stop { reply, .. } => {
                        let _ = reply.send(Err(BleError::Unsupported("transport refusal".into())));
                        break;
                    }
                    _ => panic!("unexpected command"),
                }
            }
        });
        if cleanup_first {
            worker.cleaned(peer.clone(), 1, Ok(()));
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        match worker.operations.join_next().await.unwrap().unwrap() {
            Operation::Retired(targets, result) => worker.retired(targets, result),
            _ => panic!("expected retirement"),
        }
        if !cleanup_first {
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            worker.cleaned(peer, 1, Ok(()));
        }
        assert!(
            matches!(result.await.unwrap(), Err(BleError::Unsupported(message)) if message == "transport refusal")
        );
        handler.await.unwrap();
    }
}

#[tokio::test]
async fn retirement_reply_preserves_cleanup_error_in_both_completion_orders() {
    for cleanup_first in [true, false] {
        let mut worker = worker();
        let peer = PeerAddress("typed-cleanup".into());
        let attempt = worker.attempt(peer.clone(), true, CancellationToken::new());
        worker.attempts.insert(1, attempt);
        worker.acquired.insert(peer.clone(), 1);
        let (reply, mut result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Disconnected, Some(reply));
        if cleanup_first {
            worker.cleaned(
                peer.clone(),
                1,
                Err(BleError::Unsupported("cleanup refusal".into())),
            );
        } else {
            worker.retired(vec![(peer.clone(), 1)], Ok(()));
        }
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        if cleanup_first {
            worker.retired(vec![(peer.clone(), 1)], Ok(()));
        } else {
            worker.cleaned(
                peer.clone(),
                1,
                Err(BleError::Unsupported("cleanup refusal".into())),
            );
        }
        assert!(
            matches!(result.await.unwrap(), Err(BleError::Unsupported(message)) if message == "cleanup refusal")
        );
        assert!(matches!(
            worker.cleanup_failure,
            Some(BleError::Unsupported(_))
        ));
    }
}
#[tokio::test]
async fn inbound_waits_for_the_matching_acquired_cleanup_completion() {
    use crate::drivers::mock::{MockDriver, MockNetwork};
    let network = MockNetwork::new();
    let make = |name: &str| {
        Arc::new(MockDriver::new(
            PeerAddress(name.into()),
            network.clone(),
            crate::CapabilityReport {
                central: true,
                peripheral: true,
            },
        ))
    };
    let a = make("cleanup-a");
    let b = make("cleanup-b");
    let config = PeerLinkConfig::new(datagram::DatagramConfig::new(
        crate::ServiceUuid(uuid::Uuid::from_u128(11)),
        crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
    ));
    let _served = config.datagram.serve_driver(b).await.unwrap();
    let peer = PeerAddress("cleanup-b".into());
    let channel = DatagramChannel::connect_driver(a.clone(), &peer, &config.datagram)
        .await
        .unwrap();
    let (commands, receiver) = mpsc::channel(4);
    let (incoming, _requests) = mpsc::channel(1);
    let mut worker = Worker::new(
        EndpointId::from_bytes([1; 16]),
        config,
        Some(a),
        commands,
        receiver,
        incoming,
        Arc::new(Mutex::new(HashMap::new())),
        CancellationToken::new(),
    );
    let cleanup = worker.reserve_cleanup(peer.clone(), 1);
    worker.inbound(channel);
    assert!(
        worker.attempts.is_empty(),
        "an old acquired cleanup still owns this address"
    );
    worker.cleaned(peer.clone(), 0, Ok(()));
    assert_eq!(
        worker.acquired.get(&peer),
        Some(&1),
        "stale completion must not release another generation"
    );
    assert!(
        worker.attempts.is_empty(),
        "stale completion must not admit the held channel"
    );
    drop(cleanup);
    match worker.operations.join_next().await.unwrap().unwrap() {
        Operation::Cleaned(peer, generation, result) => worker.cleaned(peer, generation, result),
        _ => panic!("expected acquired cleanup completion"),
    }
    assert_eq!(
        worker.attempts.len(),
        1,
        "the held channel must be admitted only after matching completion"
    );
    worker.shutdown.cancel();
    for attempt in worker.attempts.values() {
        if let Some(task) = &attempt.task {
            task.abort();
        }
    }
}

#[tokio::test]
async fn retirement_and_acquired_cleanup_must_both_complete_in_either_order() {
    for cleanup_first in [true, false] {
        let (commands, receiver) = mpsc::channel(4);
        let (incoming, _requests) = mpsc::channel(1);
        let config = PeerLinkConfig::new(datagram::DatagramConfig::new(
            crate::ServiceUuid(uuid::Uuid::from_u128(11)),
            crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
        ));
        let mut worker = Worker::new(
            EndpointId::from_bytes([1; 16]),
            config,
            None,
            commands,
            receiver,
            incoming,
            Arc::new(Mutex::new(HashMap::new())),
            CancellationToken::new(),
        );
        let peer = PeerAddress("both-completions".into());
        worker.acquired.insert(peer.clone(), 1);
        worker.retiring.insert(
            (peer.clone(), 1),
            Retirement {
                generation: 1,
                endpoint: None,
                complete: false,
                waiters: Vec::new(),
                error: None,
                inbound: None,
            },
        );
        let (reply, mut result) = oneshot::channel();
        worker.retire(vec![1], AttemptError::Released, Some(reply));
        assert!(
            matches!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "already-retiring last release must join its generation"
        );
        if cleanup_first {
            worker.cleaned(peer.clone(), 1, Ok(()));
        } else {
            worker.retired(vec![(peer.clone(), 1)], Ok(()));
        }
        assert!(
            matches!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "one completion cannot acknowledge release"
        );
        assert!(
            worker.address_retiring(&peer),
            "the other owner still holds the reservation"
        );
        if cleanup_first {
            worker.retired(vec![(peer.clone(), 1)], Ok(()));
        } else {
            worker.cleaned(peer.clone(), 1, Ok(()));
        }
        assert!(!worker.address_retiring(&peer));
        assert!(!worker.acquired.contains_key(&peer));
        assert!(result.try_recv().unwrap().is_ok());
    }
}

#[tokio::test]
async fn inbound_admission_counts_an_unrelated_acquired_cleanup_reservation() {
    use crate::drivers::mock::{MockDriver, MockNetwork};
    let network = MockNetwork::new();
    let make = |name: &str| {
        Arc::new(MockDriver::new(
            PeerAddress(name.into()),
            network.clone(),
            crate::CapabilityReport {
                central: true,
                peripheral: true,
            },
        ))
    };
    let a = make("admission-a");
    let b = make("admission-b");
    let mut config = PeerLinkConfig::new(datagram::DatagramConfig::new(
        crate::ServiceUuid(uuid::Uuid::from_u128(11)),
        crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
    ));
    config.max_links = MaxLinks(1);
    let _served = config.datagram.serve_driver(b).await.unwrap();
    let channel = DatagramChannel::connect_driver(
        a.clone(),
        &PeerAddress("admission-b".into()),
        &config.datagram,
    )
    .await
    .unwrap();
    let (commands, receiver) = mpsc::channel(4);
    let (incoming, _requests) = mpsc::channel(1);
    let mut worker = Worker::new(
        EndpointId::from_bytes([1; 16]),
        config,
        Some(a),
        commands,
        receiver,
        incoming,
        Arc::new(Mutex::new(HashMap::new())),
        CancellationToken::new(),
    );
    let cleanup = worker.reserve_cleanup(PeerAddress("unrelated-cleanup".into()), 1);
    worker.inbound(channel);
    let admitted = worker.attempts.len();
    // Clean up even on the RED path, without leaving an extra pump running.
    for attempt in worker.attempts.values() {
        if let Some(task) = &attempt.task {
            task.abort();
        }
    }
    drop(cleanup);
    worker.shutdown.cancel();
    assert_eq!(
        admitted, 0,
        "MaxLinks(1) is still held by acquired cleanup, not only active attempts"
    );
}

#[tokio::test]
async fn awaited_retirement_reports_native_teardown_failure() {
    let (commands, receiver) = mpsc::channel(4);
    let (incoming, _requests) = mpsc::channel(1);
    let config = PeerLinkConfig::new(crate::datagram::DatagramConfig::new(
        crate::ServiceUuid(uuid::Uuid::from_u128(11)),
        crate::CharacteristicUuid(uuid::Uuid::from_u128(12)),
    ));
    let mut worker = Worker::new(
        EndpointId::from_bytes([1; 16]),
        config,
        None,
        commands,
        receiver,
        incoming,
        Arc::new(Mutex::new(HashMap::new())),
        CancellationToken::new(),
    );
    let mut attempt = worker.attempt(PeerAddress("peer".into()), true, CancellationToken::new());
    let (transport, mut receiver) = mpsc::channel(4);
    attempt.transport = Some(transport);
    worker.attempts.insert(1, attempt);
    let handler = tokio::spawn(async move {
        while let Some(command) = receiver.recv().await {
            match command {
                TransportCommand::Send(_, Some(reply)) => {
                    let _ = reply.send(Ok(()));
                }
                TransportCommand::Stop { reply, .. } => {
                    let _ = reply.send(Err(BleError::Unsupported(
                        "native disconnect refused".into(),
                    )));
                    break;
                }
                _ => panic!("unexpected transport command"),
            }
        }
    });
    let result = worker.finish(1, AttemptError::Released).await;
    assert!(
        matches!(result, Err(BleError::Unsupported(_))),
        "native teardown failure was swallowed: {result:?}"
    );
    handler.await.unwrap();
}
