//! Discovery from radio records (ADR-0012 §2).
use super::*;

const FINI_COMPANY: u16 = 0xFFFF;
const FINI_RECORD: [u8; 5] = [0x01, 0xAA, 0xBB, 0xCC, 0xDD];

/// A Fini-shaped config: flags and fingerprint under 0xFFFF, carried in the
/// fast advertisement.
fn fast_config(name: Option<&str>) -> DatagramConfig {
    let mut config = config();
    config
        .advertised_manufacturer_data
        .insert(FINI_COMPANY, FINI_RECORD.to_vec());
    config.advertised_local_name = name.map(Into::into);
    config.fast_advertisement_company = Some(FINI_COMPANY);
    config
}

fn advertiser_with(
    network: &Arc<MockNetwork>,
    address: &str,
    config: DatagramConfig,
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
        PeerLinkConfig::new(config),
    )
}

fn connects(control: &MetadataControl) -> usize {
    control.connects.load(std::sync::atomic::Ordering::SeqCst)
}

async fn first_sighting(driver: &BlockingVisibility) -> crate::DiscoveredPeer {
    driver
        .inner
        .scan(config().service)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn a_fast_advertisement_lists_the_peer_without_a_connection() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-complete", fast_config(None));
    advertiser.set_advertising(true).await.unwrap();
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "fast-complete-reader", fast_config(None));
    for _ in 0..2 {
        let mut discovery = reader.discover().await.unwrap();
        let (endpoint, peer) = next_item(&mut discovery, Duration::from_millis(300))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(endpoint, advertiser.endpoint_id());
        assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
        assert!(
            next_item(&mut discovery, Duration::from_millis(300))
                .await
                .is_none(),
            "complete from the radio: nothing more to report"
        );
    }
    assert_eq!(connects(&control), 0);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn an_incomplete_fast_advertisement_is_listed_at_once_then_read_once() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-named", fast_config(Some("Pixel")));
    advertiser.set_advertising(true).await.unwrap();
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "fast-named-reader", fast_config(None));
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, listed) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(listed.name, None, "no room for the name next to the record");
    assert_eq!(
        listed.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    let (endpoint, named) = next_item(&mut discovery, Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(named.name.as_deref(), Some("Pixel"));
    assert!(next_item(&mut discovery, Duration::from_millis(300))
        .await
        .is_none());
    assert_eq!(connects(&control), 1, "one completion read");
    drop(discovery);
    // A second scan knows this endpoint and hash: no read.
    let mut again = reader.discover().await.unwrap();
    let mut names = Vec::new();
    while let Some(item) = next_item(&mut again, Duration::from_millis(300)).await {
        names.push(item.unwrap().1.name);
    }
    assert_eq!(names.last().cloned().flatten().as_deref(), Some("Pixel"));
    assert_eq!(connects(&control), 1, "the name came from the cache");
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_stalled_completion_read_neither_delays_the_listing_nor_holds_off_a_dial() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-stalled", fast_config(Some("Pixel")));
    let mut incoming = advertiser.incoming().unwrap();
    advertiser.set_advertising(true).await.unwrap();
    let (driver, _control, reader) =
        BlockingVisibility::metadata_with(network, "fast-stalled-reader", fast_config(None));
    // A peer that does not answer: the first connection hangs.
    let address = PeerAddress("fast-stalled".into());
    driver
        .inner
        .stall_connect(&address, Duration::from_secs(30));
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, listed) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(
        listed.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    // Let the completion read start and hang, then dial: the read yields.
    tokio::time::sleep(Duration::from_millis(200)).await;
    driver.inner.stall_connect(&address, Duration::ZERO);
    let started = tokio::time::Instant::now();
    let request = loop {
        match reader.request(&address).await {
            Err(crate::BleError::AdapterBusy(_)) if started.elapsed() < Duration::from_secs(2) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => break other,
        }
    };
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "refused for {:?}",
        started.elapsed()
    );
    let request = request.expect("the dial gets the address");
    let accepted = tokio::spawn(async move { incoming.next().await.unwrap().accept().await });
    let channel = tokio::time::timeout(Duration::from_secs(2), request.accept())
        .await
        .unwrap()
        .unwrap();
    accepted.await.unwrap().unwrap();
    drop(channel);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_header_is_read_once_per_hash_across_scans() {
    use crate::advertisement::GattAdvertisement;
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "header-peer", config());
    advertiser
        .update_advertisement(GattAdvertisement {
            manufacturer_data: [(FINI_COMPANY, FINI_RECORD.to_vec())].into(),
            local_name: Some("Desk".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("header-peer".into()), Some(-50));
    let (_, control, reader) = BlockingVisibility::metadata(network.clone(), "header-reader");
    async fn scan(reader: &PeerLink) -> Vec<(EndpointId, crate::DiscoveredPeer)> {
        let mut discovery = reader.discover().await.unwrap();
        let mut found = Vec::new();
        while let Some(item) = next_item(&mut discovery, Duration::from_secs(2)).await {
            found.push(item.unwrap());
        }
        found
    }
    let first = scan(&reader).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, advertiser.endpoint_id());
    assert_eq!(first[0].1.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1);
    let second = scan(&reader).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].1.name.as_deref(), Some("Desk"));
    assert_eq!(
        connects(&control),
        1,
        "an unchanged header is not read again"
    );
    // The Add Device flag changes: a new hash, one read.
    advertiser
        .update_advertisement(GattAdvertisement {
            manufacturer_data: [(FINI_COMPANY, vec![0x00, 0xAA, 0xBB, 0xCC, 0xDD])].into(),
            local_name: Some("Desk".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let third = scan(&reader).await;
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].1.manufacturer_data[&FINI_COMPANY][0], 0x00);
    assert_eq!(connects(&control), 2);
    // A stale platform entry (no signal strength) is not a sighting.
    network.set_rssi(&PeerAddress("header-peer".into()), None);
    assert!(scan(&reader).await.is_empty());
    assert_eq!(connects(&control), 2);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[test]
fn a_read_header_is_read_again_once_its_cached_value_is_gone() {
    use crate::advertisement::{MetadataKey, TrackedPeer};
    let now = tokio::time::Instant::now();
    let key = MetadataKey {
        tag: [1, 2, 3, 4],
        hash: 5,
    };
    let sighting = crate::DiscoveredPeer {
        address: PeerAddress("expired-peer".into()),
        name: None,
        services: Vec::new(),
        manufacturer_data: Default::default(),
        service_data: Default::default(),
        rssi: Some(-50),
    };
    let mut tracked = TrackedPeer::header(sighting.clone(), key, now);
    tracked.managed_result(Some(&sighting), None);
    tracked.update_keyed(sighting.clone(), key, now, false);
    assert!(
        !tracked.managed_due(now, true),
        "the same header with its value cached is not read again"
    );
    // Expired or evicted from the cache: the next sighting reads it again.
    tracked.update_keyed(sighting, key, now, false);
    tracked.rearm(now);
    assert!(tracked.managed_due(now, true));
}

#[tokio::test]
async fn a_peer_without_a_record_is_read_once_then_reported_from_that_read() {
    let network = MockNetwork::new();
    let address = PeerAddress("unnamed-peer".into());
    network.set_radio_capacity(&address, crate::drivers::mock::RadioCapacity::None);
    let advertiser = advertiser_with(&network, "unnamed-peer", fast_config(Some("Linux")));
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&address, Some(-50));
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "unnamed-reader", fast_config(None));
    for scan in 1..=2 {
        let started = tokio::time::Instant::now();
        let mut discovery = reader.discover().await.unwrap();
        let (endpoint, peer) = next_item(&mut discovery, Duration::from_secs(3))
            .await
            .unwrap()
            .unwrap();
        if scan == 1 {
            assert!(started.elapsed() >= crate::advertisement::SCAN_RESPONSE_GRACE);
        } else {
            assert!(
                started.elapsed() < crate::advertisement::SCAN_RESPONSE_GRACE,
                "the address is known: reported from the last read"
            );
        }
        assert_eq!(endpoint, advertiser.endpoint_id());
        assert_eq!(peer.name.as_deref(), Some("Linux"));
        assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
        assert_eq!(
            connects(&control),
            1,
            "one read; the next waits KNOWN_PEER_REFRESH"
        );
        drop(discovery);
    }
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn foreign_data_shaped_like_a_fast_advertisement_is_neither_listed_nor_read() {
    let network = MockNetwork::new();
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "foreign-reader", fast_config(None));
    let (input, receiver) = tokio::sync::mpsc::channel(2);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    // Another company's manufacturer data with a fast advertisement's first
    // byte and length, and no managed service: a scanner lets it through.
    let mut foreign = vec![0x5A; 21];
    foreign[0] = 0xFC;
    input
        .send(Ok(crate::DiscoveredPeer {
            address: PeerAddress("foreign-peer".into()),
            name: None,
            services: Vec::new(),
            manufacturer_data: [(0x0006, foreign)].into(),
            service_data: Default::default(),
            rssi: Some(-50),
        }))
        .await
        .unwrap();
    input
        .send(Err(crate::BleError::Gatt("sighting barrier".into())))
        .await
        .unwrap();
    assert!(matches!(
        next_item(&mut discovery, Duration::from_millis(300)).await,
        Some(Err(crate::BleError::Gatt(message))) if message == "sighting barrier"
    ));
    drop(input);
    assert!(
        next_item(
            &mut discovery,
            crate::advertisement::SCAN_RESPONSE_GRACE + Duration::from_secs(1)
        )
        .await
        .is_none(),
        "a peer without a record or the service is not tracked"
    );
    assert_eq!(connects(&control), 0, "and never read over GATT");
    reader.close().await.unwrap();
}

#[tokio::test]
async fn failed_header_reads_back_off_and_are_reported_once() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "backoff-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("backoff-peer".into()), Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "backoff-reader");
    let sighting = first_sighting(&driver).await;
    // Gone before the read: every connect fails.
    advertiser.close().await.unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    input.send(Ok(sighting)).await.unwrap();
    let started = tokio::time::Instant::now();
    let mut errors = 0;
    while let Ok(item) =
        tokio::time::timeout_at(started + Duration::from_millis(3600), discovery.next()).await
    {
        match item {
            Some(Err(_)) => errors += 1,
            Some(Ok(item)) => panic!("nothing to read, yet {item:?}"),
            None => break,
        }
    }
    // Reads at 0 s, 1 s and 3 s; the next one waits until 7 s.
    assert_eq!(connects(&control), 3);
    assert_eq!(errors, 1, "the failure is reported once");
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
}

#[tokio::test]
async fn a_rotated_address_with_the_same_header_is_not_read_again() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "rotating-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&PeerAddress("rotating-peer".into()), Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "rotation-reader");
    let mut sighting = first_sighting(&driver).await;
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let (endpoint, _) = next_item(&mut discovery, Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(connects(&control), 1);
    sighting.address = PeerAddress("rotated-address".into());
    input.send(Ok(sighting)).await.unwrap();
    let (endpoint, peer) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(peer.address, PeerAddress("rotated-address".into()));
    assert_eq!(
        connects(&control),
        1,
        "same tag and hash: the cache answers"
    );
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_rotated_fast_advertisement_is_read_once() {
    let network = MockNetwork::new();
    let advertiser = advertiser_with(&network, "fast-rotated", fast_config(Some("Pixel")));
    advertiser.set_advertising(true).await.unwrap();
    let (driver, control, reader) =
        BlockingVisibility::metadata_with(network, "fast-rotated-reader", fast_config(None));
    let newest = first_sighting(&driver).await;
    // Hidden from the mock scan: only the sightings below reach the reader.
    advertiser.set_advertising(false).await.unwrap();
    // The address the phone used before it rotated: a read there hangs.
    let mut old = newest.clone();
    old.address = PeerAddress("fast-rotated-old".into());
    driver
        .inner
        .stall_connect(&old.address, Duration::from_secs(30));
    let (input, receiver) = tokio::sync::mpsc::channel(2);
    input.send(Ok(old)).await.unwrap();
    input.send(Ok(newest)).await.unwrap();
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
    let mut endpoints = Vec::new();
    while let Ok(Some(item)) = tokio::time::timeout_at(deadline, discovery.next()).await {
        endpoints.push(item.unwrap().0);
    }
    assert!(!endpoints.is_empty(), "listed from its fast advertisement");
    assert!(endpoints
        .iter()
        .all(|endpoint| *endpoint == advertiser.endpoint_id()));
    assert_eq!(
        connects(&control),
        1,
        "one endpoint and hash at two addresses: one read"
    );
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn managed_discovery_reads_a_header_named_while_an_earlier_read_ran() {
    let network = MockNetwork::new();
    let address = PeerAddress("renamed-peer".into());
    let advertiser = advertiser_with(&network, "renamed-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&address, Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "renamed-reader");
    let first = first_sighting(&driver).await;
    // Hidden from the mock scan: only the sightings below reach the reader.
    advertiser.set_advertising(false).await.unwrap();
    let mut renamed = first.clone();
    // Another hash under the same endpoint tag: the radio names a newer value.
    renamed.service_data.get_mut(&config().service).unwrap()[1] ^= 1;
    driver
        .inner
        .stall_connect(&address, Duration::from_millis(400));
    let (input, receiver) = tokio::sync::mpsc::channel(2);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    input.send(Ok(first)).await.unwrap();
    while connects(&control) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    driver.inner.stall_connect(&address, Duration::ZERO);
    input.send(Ok(renamed)).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
    let mut found = Vec::new();
    while let Ok(Some(item)) = tokio::time::timeout_at(deadline, discovery.next()).await {
        if let Ok((endpoint, _)) = item {
            found.push(endpoint);
        }
    }
    assert_eq!(found, vec![advertiser.endpoint_id()]);
    assert_eq!(
        connects(&control),
        2,
        "the first read does not answer for the newer header"
    );
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn managed_discovery_tracks_a_new_peer_when_its_table_is_full() {
    let network = MockNetwork::new();
    let address = PeerAddress("crowded-peer".into());
    let advertiser = advertiser_with(&network, "crowded-peer", config());
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&address, Some(-50));
    let (driver, control, reader) = BlockingVisibility::metadata(network, "crowded-reader");
    let header = first_sighting(&driver).await;
    // Hidden from the mock scan: only the sightings below reach the reader.
    advertiser.set_advertising(false).await.unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(128);
    // A crowd without records fills the 64-peer table before the header arrives.
    let mut crowd = header.clone();
    crowd.service_data.clear();
    for n in 0..64 {
        crowd.address = PeerAddress(format!("crowd-{n}"));
        input.send(Ok(crowd.clone())).await.unwrap();
    }
    input.send(Ok(header)).await.unwrap();
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut found = None;
    while let Ok(Some(item)) = tokio::time::timeout_at(deadline, discovery.next()).await {
        if let Ok((endpoint, _)) = item {
            found = Some(endpoint);
            break;
        }
    }
    assert_eq!(
        found,
        Some(advertiser.endpoint_id()),
        "the stalest peer makes room"
    );
    drop(input);
    drop(discovery);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn an_advertisement_update_changes_the_header_hash_with_the_readable_fields() {
    use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
    let network = MockNetwork::new();
    let (driver, _, owner) =
        BlockingVisibility::metadata_with(network, "update-header", fast_config(Some("Pixel")));
    owner.set_advertising(true).await.unwrap();
    fn readable(spec: &crate::GattServiceSpec) -> Vec<u8> {
        spec.characteristics
            .iter()
            .find(|value| value.uuid == GATT_ADVERTISEMENT_CHARACTERISTIC)
            .unwrap()
            .initial_value
            .clone()
    }
    fn header(spec: &crate::GattServiceSpec) -> managed_codec::RadioRecord {
        managed_codec::RadioRecord::decode(&spec.service_data[&spec.uuid]).unwrap()
    }
    let before = driver.served_service().unwrap();
    let flagged = GattAdvertisement {
        manufacturer_data: [(FINI_COMPANY, vec![0x00, 0xAA, 0xBB, 0xCC, 0xDD])].into(),
        local_name: Some("Pixel".into()),
        ..Default::default()
    };
    owner.update_advertisement(flagged.clone()).await.unwrap();
    let after = driver.served_service().unwrap();
    assert_ne!(header(&before), header(&after));
    let value = readable(&after);
    assert_eq!(
        header(&after),
        managed_codec::RadioRecord::Header {
            hash: managed_codec::RadioRecord::metadata_hash(&value),
            tag: owner.endpoint_id().as_bytes()[..4].try_into().unwrap(),
        }
    );
    let mut expected = flagged;
    expected.service_data.insert(
        config().service,
        managed_codec::ManagedMetadata {
            endpoint: owner.endpoint_id(),
        }
        .encode(),
    );
    assert_eq!(GattAdvertisement::decode(&value).unwrap(), expected);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn the_folded_manufacturer_data_reaches_the_scanner_as_it_was_set() {
    let network = MockNetwork::new();
    let mut advertised = fast_config(None);
    advertised
        .advertised_manufacturer_data
        .insert(0x0001, vec![9]);
    let advertiser = advertiser_with(&network, "folding-peer", advertised);
    advertiser.set_advertising(true).await.unwrap();
    let (driver, control, reader) =
        BlockingVisibility::metadata_with(network.clone(), "folding-reader", fast_config(None));
    // On the radio the 0xFFFF entry is the fast record, not the caller's 5 bytes.
    let radio = first_sighting(&driver).await;
    assert_eq!(radio.manufacturer_data[&FINI_COMPANY][0], 0xFC);
    assert_eq!(radio.manufacturer_data[&FINI_COMPANY].len(), 26);
    let mut discovery = reader.discover().await.unwrap();
    let (_, peer) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
    assert_eq!(peer.manufacturer_data[&0x0001], vec![9]);
    assert_eq!(
        peer.service_data[&config().service],
        managed_codec::ManagedMetadata {
            endpoint: advertiser.endpoint_id()
        }
        .encode()
    );
    assert_eq!(connects(&control), 0);
    drop(discovery);
    // A scanner that does not know the company reads instead, and still gets the 5 bytes.
    let blind = mock_link(&network, "folding-blind");
    let mut discovery = blind.discover().await.unwrap();
    let (_, peer) = next_item(&mut discovery, Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.manufacturer_data[&FINI_COMPANY], FINI_RECORD.to_vec());
    drop(discovery);
    blind.close().await.unwrap();
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}

#[tokio::test]
async fn a_publisher_at_its_own_address_joins_the_provider_read_once() {
    // A Windows peer: its provider's advertisement has the service and no record,
    // and a publisher at another address sends the fast advertisement.
    let network = MockNetwork::new();
    let provider = PeerAddress("windows-provider".into());
    network.set_radio_capacity(&provider, crate::drivers::mock::RadioCapacity::None);
    let (windows, _, advertiser) = BlockingVisibility::metadata_with(
        network.clone(),
        "windows-provider",
        fast_config(Some("Desk")),
    );
    advertiser.set_advertising(true).await.unwrap();
    network.set_rssi(&provider, Some(-50));
    let fast = windows
        .served_service()
        .unwrap()
        .fast_advertisement
        .unwrap();
    let publisher = crate::DiscoveredPeer {
        address: PeerAddress("windows-publisher".into()),
        name: None,
        services: Vec::new(),
        manufacturer_data: [(fast.company_id, fast.data)].into(),
        service_data: Default::default(),
        rssi: Some(-50),
    };
    let (_, control, reader) =
        BlockingVisibility::metadata_with(network, "publisher-reader", fast_config(None));
    let (input, receiver) = tokio::sync::mpsc::channel(4);
    *control.scan_input.lock().unwrap() = Some(receiver);
    let mut discovery = reader.discover().await.unwrap();
    // Before any read the publisher's address is not reported: it is not connectable.
    input.send(Ok(publisher.clone())).await.unwrap();
    let (endpoint, read) = next_item(&mut discovery, Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(read.address, provider);
    assert_eq!(read.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1, "the provider is read once");
    // Now the publisher's record is reported at the provider's address, from the cache.
    input.send(Ok(publisher)).await.unwrap();
    let (endpoint, joined) = next_item(&mut discovery, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(joined.address, provider);
    assert_eq!(joined.name.as_deref(), Some("Desk"));
    assert_eq!(
        joined.manufacturer_data[&FINI_COMPANY],
        FINI_RECORD.to_vec()
    );
    drop(input);
    drop(discovery);
    // The next scan: the provider's address is answered from the publisher's key.
    let mut again = reader.discover().await.unwrap();
    let (endpoint, cached) = next_item(&mut again, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, advertiser.endpoint_id());
    assert_eq!(cached.name.as_deref(), Some("Desk"));
    assert_eq!(connects(&control), 1, "no second read");
    drop(again);
    reader.close().await.unwrap();
    advertiser.close().await.unwrap();
}
