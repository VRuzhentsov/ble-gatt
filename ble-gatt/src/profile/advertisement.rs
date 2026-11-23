//! The GATT advertisement: advertisement data a peripheral serves in a
//! readable characteristic, for the platforms that cannot put it in the
//! radio advertisement (layer: Use Cases; see `docs/architecture.md` and
//! ADR-0012).
//!
//! The design follows Google Nearby Connections (`google/nearby`,
//! Apache-2.0), which ships on Android, Windows and Apple:
//!
//! - **Fast advertisement.** Where the API and packet capacity allow it,
//!   the advertisement data travels in the radio advertisement as before,
//!   and a scanner needs no connection. Nearby's "fast advertisement" is
//!   the same idea.
//! - **GATT advertisement.** Every served datagram service also serves the
//!   whole advertisement data in [`GATT_ADVERTISEMENT_CHARACTERISTIC`].
//!   Optional radio fields depend on the API and packet capacity. A scanner
//!   that finds a peer without those fields connects, reads the characteristic and
//!   disconnects. Nearby's BLE medium (V2) keeps its full advertisement in
//!   GATT characteristics the same way
//!   (`connections/implementation/mediums/ble/discovered_peripheral_tracker.h`).
//!
//! [`complete`] does the second part for a scan stream, so a consumer sees
//! one `DiscoveredPeer` with the same fields whichever way they arrived.
//!
//! **Format.** One version byte, then advertising data structures as the
//! Bluetooth Core Specification Supplement (Part A) defines them:
//! `[length][AD type][data]`, where `length` counts the type byte and the
//! data. The types written are manufacturer specific data (`0xFF`), service
//! data with a 128-bit UUID (`0x21`) and the complete local name (`0x09`);
//! unknown types are skipped when reading, so a later version can add more.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    advertised_name, CharacteristicUuid, DiscoveredPeer, GattCharacteristicSpec, GattServiceSpec,
    PeerAddress, ServiceUuid,
};
use crate::hal::{BoxStream, Driver, MAX_ATT_ATTRIBUTE_LEN};

/// The characteristic that serves the GATT advertisement, in every service
/// a datagram peripheral serves. A fixed UUID, like Nearby's advertisement
/// slot characteristics, so a scanner knows where to read without a
/// discovery round of its own.
pub const GATT_ADVERTISEMENT_CHARACTERISTIC: CharacteristicUuid =
    CharacteristicUuid(Uuid::from_u128(0x2f9b6c4e_8d1a_4c7b_a3e5_6b0f1d2c8a91));

/// The format version this library writes and reads.
pub const FORMAT_VERSION: u8 = 1;

const AD_COMPLETE_LOCAL_NAME: u8 = 0x09;
const AD_SERVICE_DATA_16: u8 = 0x16;
const AD_SERVICE_DATA_32: u8 = 0x20;
const AD_SERVICE_DATA_128: u8 = 0x21;
const AD_MANUFACTURER_DATA: u8 = 0xFF;

/// Largest data one AD structure holds: its length byte counts the type
/// byte too.
const AD_MAX_DATA_LEN: usize = 254;

/// The Bluetooth Base UUID, `00000000-0000-1000-8000-00805F9B34FB`.
const BLUETOOTH_BASE_UUID: u128 = 0x0000_0000_0000_1000_8000_0080_5F9B_34FB;

/// The characteristic a peripheral adds to its service to serve `spec`'s
/// advertisement data: readable only.
pub fn characteristic_spec(spec: &GattServiceSpec) -> GattCharacteristicSpec {
    GattAdvertisement::characteristic_spec(spec)
}

/// Whether a scanner should read `peer`'s GATT advertisement: its radio
/// advertisement carried no manufacturer and no service data.
pub fn needs_gatt_advertisement(peer: &DiscoveredPeer) -> bool {
    peer.needs_gatt_advertisement()
}

/// Connect to `peer`, read its GATT advertisement and disconnect.
pub async fn read(driver: &dyn Driver, peer: &PeerAddress) -> Result<GattAdvertisement> {
    GattAdvertisement::read_owned(driver, peer, false).await
}

/// `scan` (a scan for `service` on `driver`), with the GATT advertisement
/// of every peer that [needs one](needs_gatt_advertisement) read and merged
/// in. Peers whose radio advertisement carries the data pass through without a read.
/// A peer that needs a read is reported after its first read and when later reads find changed data.
/// On failure, a peer not yet reported is reported with its radio advertisement data.
///
/// Reads run one at a time, so a crowd of peers cannot exhaust the
/// platform's connection slots. A scan error ends the stream as it ends a
/// scan.
pub fn complete(
    driver: Arc<dyn Driver>,
    service: ServiceUuid,
    scan: BoxStream<Result<DiscoveredPeer>>,
    policy: GattAdvertisementPolicy,
) -> BoxStream<Result<DiscoveredPeer>> {
    policy.complete(driver, service, scan)
}

/// The advertisement data a GATT advertisement carries: the same fields as
/// `GattServiceSpec`'s advertisement data and `DiscoveredPeer`'s.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GattAdvertisement {
    pub manufacturer_data: BTreeMap<u16, Vec<u8>>,
    pub service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    pub local_name: Option<String>,
}

impl GattAdvertisement {
    /// The advertisement data of `spec`.
    pub fn of(spec: &GattServiceSpec) -> Self {
        Self {
            manufacturer_data: spec.manufacturer_data.clone(),
            service_data: spec.service_data.clone(),
            local_name: spec.local_name.clone(),
        }
    }

    /// The characteristic value. An entry that would push the value past
    /// the 512 bytes one ATT attribute holds, or one AD structure past 255,
    /// is left out and logged.
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = vec![FORMAT_VERSION];
        let mut append_structure = |kind: u8, data: Vec<u8>| {
            if data.len() > AD_MAX_DATA_LEN
                || encoded.len() + 2 + data.len() > MAX_ATT_ATTRIBUTE_LEN
            {
                log::warn!(
                    "gatt advertisement: omit AD type {kind:#04x} ({} bytes): size limit exceeded",
                    data.len()
                );
                return;
            }
            encoded.push((data.len() + 1) as u8);
            encoded.push(kind);
            encoded.extend_from_slice(&data);
        };
        for (company, value) in &self.manufacturer_data {
            let mut data = company.to_le_bytes().to_vec();
            data.extend_from_slice(value);
            append_structure(AD_MANUFACTURER_DATA, data);
        }
        for (uuid, value) in &self.service_data {
            let mut data = uuid.0.as_u128().to_le_bytes().to_vec();
            data.extend_from_slice(value);
            append_structure(AD_SERVICE_DATA_128, data);
        }
        if let Some(name) = &self.local_name {
            append_structure(AD_COMPLETE_LOCAL_NAME, name.as_bytes().to_vec());
        }
        encoded
    }

    /// Read a characteristic value [`encode`](Self::encode) wrote.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (&version, mut rest) = bytes
            .split_first()
            .ok_or_else(|| BleError::Gatt("gatt advertisement: empty value".into()))?;
        if version != FORMAT_VERSION {
            return Err(BleError::Gatt(format!(
                "gatt advertisement: unsupported version {version}"
            )));
        }
        let mut out = Self::default();
        while let Some((&len, tail)) = rest.split_first() {
            let len = len as usize;
            if len == 0 {
                // A zero length ends the data early (Core Supplement Part A).
                break;
            }
            if tail.len() < len {
                return Err(BleError::Gatt(
                    "gatt advertisement: truncated AD structure".into(),
                ));
            }
            let (structure, next) = tail.split_at(len);
            rest = next;
            let (kind, data) = (structure[0], &structure[1..]);
            match kind {
                AD_MANUFACTURER_DATA => {
                    if let Some((company, value)) = data.split_first_chunk::<2>() {
                        out.manufacturer_data
                            .insert(u16::from_le_bytes(*company), value.to_vec());
                    }
                }
                AD_SERVICE_DATA_16 | AD_SERVICE_DATA_32 | AD_SERVICE_DATA_128 => {
                    if let Some((uuid, value)) = Self::service_data_entry(kind, data) {
                        out.service_data.insert(uuid, value);
                    }
                }
                AD_COMPLETE_LOCAL_NAME => out.local_name = String::from_utf8(data.to_vec()).ok(),
                _ => {}
            }
        }
        Ok(out)
    }

    /// `peer` with this advertisement data merged in. The radio
    /// advertisement keeps what it carried; the GATT advertisement adds the
    /// rest. Its name wins over the platform's, which on Windows is the
    /// computer's Bluetooth name rather than the one the peer chose.
    pub fn merge_into(&self, service: ServiceUuid, mut peer: DiscoveredPeer) -> DiscoveredPeer {
        for (company, value) in &self.manufacturer_data {
            peer.manufacturer_data
                .entry(*company)
                .or_insert_with(|| value.clone());
        }
        for (uuid, value) in &self.service_data {
            peer.service_data
                .entry(*uuid)
                .or_insert_with(|| value.clone());
        }
        peer.name = self
            .local_name
            .clone()
            .or_else(|| advertised_name(service, &peer.service_data, peer.name.take()));
        peer
    }
    fn service_data_entry(kind: u8, data: &[u8]) -> Option<(ServiceUuid, Vec<u8>)> {
        let expand_short_uuid =
            |value: u32| Uuid::from_u128(BLUETOOTH_BASE_UUID | (u128::from(value) << 96));
        let (uuid, rest) = match kind {
            AD_SERVICE_DATA_16 => {
                let (bytes, rest) = data.split_first_chunk::<2>()?;
                (
                    expand_short_uuid(u32::from(u16::from_le_bytes(*bytes))),
                    rest,
                )
            }
            AD_SERVICE_DATA_32 => {
                let (bytes, rest) = data.split_first_chunk::<4>()?;
                (expand_short_uuid(u32::from_le_bytes(*bytes)), rest)
            }
            _ => {
                let (bytes, rest) = data.split_first_chunk::<16>()?;
                (Uuid::from_u128(u128::from_le_bytes(*bytes)), rest)
            }
        };
        Some((ServiceUuid(uuid), rest.to_vec()))
    }
    pub(crate) fn characteristic_spec(spec: &GattServiceSpec) -> GattCharacteristicSpec {
        GattCharacteristicSpec {
            uuid: GATT_ADVERTISEMENT_CHARACTERISTIC,
            readable: true,
            writable: false,
            notifiable: false,
            initial_value: GattAdvertisement::of(spec).encode(),
        }
    }
    pub(crate) async fn read_managed(
        driver: &dyn Driver,
        peer: &PeerAddress,
    ) -> Result<GattAdvertisement> {
        Self::read_owned(driver, peer, true).await
    }

    async fn read_owned(
        driver: &dyn Driver,
        peer: &PeerAddress,
        managed_cleanup: bool,
    ) -> Result<GattAdvertisement> {
        // Subscribe before connecting so the connection observes events from its driver session.
        let events = driver.lifecycle_events();
        let connection = driver.connect(peer).await?;
        let mut owned_connection = if managed_cleanup {
            crate::connection::Connection::new_managed(connection, events)
        } else {
            crate::connection::Connection::new(connection, events)
        };
        let read_result = owned_connection
            .read(GATT_ADVERTISEMENT_CHARACTERISTIC)
            .await;
        // Disconnect the owned connection even if the read fails.
        // Cancellation after acquisition drops this handle and schedules disconnect of its owned driver session.
        // The driver handles cancellation while connect is pending.
        let _ = owned_connection.disconnect().await;
        GattAdvertisement::decode(&read_result?)
    }
}

impl DiscoveredPeer {
    pub(crate) fn needs_gatt_advertisement(&self) -> bool {
        self.manufacturer_data.is_empty() && self.service_data.is_empty()
    }
}

/// How [`complete`] reads GATT advertisements.
#[derive(Debug, Clone, Copy)]
pub struct GattAdvertisementPolicy {
    /// How often a peer's GATT advertisement is read again while the scan
    /// runs. The radio advertisement of such a peer does not change when
    /// its data does, so this bounds how stale its data can be (an Add
    /// Device flag, for example). Nearby puts a hash of the value in its
    /// advertisement header instead, and the managed profile (`PeerLink`)
    /// does too (ADR-0012 §5): its scanner reads again only when that hash
    /// changes, and refreshes only peers that advertise no record. A raw
    /// `serve` advertises no hash, so this scan refreshes.
    pub refresh: Duration,
    /// Longest one read may take, connection included.
    pub timeout: Duration,
    /// Reads that fail in a row before a peer is no longer read.
    pub max_failures: u32,
    /// Most peers tracked at once, including peers at the failure limit.
    /// More peers are reported without a read.
    pub max_peers: usize,
}

impl Default for GattAdvertisementPolicy {
    fn default() -> Self {
        Self {
            refresh: Duration::from_secs(10),
            timeout: Duration::from_secs(10),
            max_failures: 3,
            max_peers: 64,
        }
    }
}

impl GattAdvertisementPolicy {
    fn complete(
        self,
        driver: Arc<dyn Driver>,
        service: ServiceUuid,
        scan: BoxStream<Result<DiscoveredPeer>>,
    ) -> BoxStream<Result<DiscoveredPeer>> {
        let (sender, receiver) = mpsc::channel(64);
        tokio::spawn(async move {
            // Dropping the output cancels this scan and its active read.
            // The read's connection handle owns cleanup after acquisition.
            tokio::select! {
                result = AdvertisementScan::new(self).complete_scan(driver.as_ref(), service, scan, &sender) => {
                    if let Err(err) = result {
                        let _ = sender.send(Err(err)).await;
                    }
                }
                () = sender.closed() => {}
            }
        });
        Box::pin(ReceiverStream::new(receiver))
    }
}

/// The bounded metadata table and read schedule for one raw scan.
struct AdvertisementScan {
    tracked: HashMap<PeerAddress, TrackedPeer>,
    scan_active: bool,
    policy: GattAdvertisementPolicy,
}

impl AdvertisementScan {
    fn new(policy: GattAdvertisementPolicy) -> Self {
        Self {
            tracked: HashMap::new(),
            scan_active: true,
            policy,
        }
    }
    async fn complete_scan(
        mut self,
        driver: &dyn Driver,
        service: ServiceUuid,
        mut scan: BoxStream<Result<DiscoveredPeer>>,
        sender: &mpsc::Sender<Result<DiscoveredPeer>>,
    ) -> Result<()> {
        loop {
            let scheduled_read = self.next_read();
            if !self.scan_active && scheduled_read.is_none() {
                return Ok(());
            }
            let peer = tokio::select! {
                item = scan.next(), if self.scan_active => match item {
                    None => { self.scan_active = false; None }
                    Some(item) => self.track_peer(item?),
                },
                () = tokio::time::sleep_until(scheduled_read.as_ref().map_or_else(Instant::now, |(_, at)| *at)), if scheduled_read.is_some() => {
                    let (address, _) = scheduled_read.expect("a read is due");
                    let result = tokio::time::timeout(self.policy.timeout, read(driver, &address)).await
                        .unwrap_or_else(|_| Err(BleError::Gatt(format!("no answer in {:?}", self.policy.timeout))));
                    let entry = self.tracked.get_mut(&address).expect("the peer stays tracked");
                    entry.next_read_at = Instant::now() + self.policy.refresh;
                    entry.record_read(service, result)
                }
            };
            if let Some(peer) = peer {
                if sender.send(Ok(peer)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }

    fn next_read(&self) -> Option<(PeerAddress, Instant)> {
        self.tracked
            .iter()
            .filter(|(_, entry)| {
                entry.consecutive_failures == 0
                    || entry.consecutive_failures < self.policy.max_failures
            })
            // After the scan ends, read only peers not yet reported.
            .filter(|(_, entry)| self.scan_active || !entry.has_been_reported)
            .min_by_key(|(_, entry)| entry.next_read_at)
            .map(|(address, entry)| (address.clone(), entry.next_read_at))
    }

    fn track_peer(&mut self, peer: DiscoveredPeer) -> Option<DiscoveredPeer> {
        if !needs_gatt_advertisement(&peer) {
            return Some(peer);
        }
        if let Some(entry) = self.tracked.get_mut(&peer.address) {
            entry.peer = peer;
        } else if self.tracked.len() < self.policy.max_peers {
            self.tracked.insert(
                peer.address.clone(),
                TrackedPeer::new(peer, ReadKind::Unnamed, Instant::now()),
            );
        } else {
            return Some(peer);
        }
        None
    }
}

/// How long a managed scanner waits before it reads a peer whose radio
/// advertisement carried no record. Active scanning delivers the
/// advertisement and its scan response as separate results, and the record
/// can be in the scan response (the Android placement, ADR-0012 §2): a read
/// started on the first result would connect for nothing. On Windows the fast
/// record arrived within 0.1 s of the scan start; the grace also covers one
/// lost scan request (an Android advertiser sends again after about 250 ms),
/// and delays the read of a peer that has no record (a Windows or Linux peer)
/// by no more than that.
pub(crate) const SCAN_RESPONSE_GRACE: Duration = Duration::from_millis(300);

/// How often a managed scanner reads a peer without a radio record again once
/// a read found its endpoint at that address (a Windows PC while it scans, a
/// Linux peer). Meanwhile each sighting reports the last value, and a change of
/// its radio fields reads at once. Every 10 s held a link to the peer half of
/// the time (Android keeps an idle link 4 s), and the peer's own reads crossed
/// those links and stalled (ADR-0012 §2). A Windows peer's publisher names a
/// new value at once when the PC stops scanning.
pub(crate) const KNOWN_PEER_REFRESH: Duration = Duration::from_secs(30);

/// Deadline of one completion read. Shorter than a header read's 10 s: its
/// peer is listed already, and a read that stalls on a link the peer dropped
/// (Windows to Android, while the phone also reads the PC) is better retried
/// on a fresh link after the first backoff than waited for. The uncached GATT
/// discovery inside it gives up after 5 s.
pub(crate) const COMPLETION_READ_TIMEOUT: Duration = Duration::from_secs(6);

/// First wait after a failed read; it doubles per failure in a row.
pub(crate) const READ_BACKOFF_FIRST: Duration = Duration::from_secs(1);
/// Longest wait between failed reads. Nearby's
/// `advertisement_read_result.cc` uses the same 1 s to 5 min schedule.
pub(crate) const READ_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Most readable values a managed scanner remembers.
pub(crate) const METADATA_CACHE_CAPACITY: usize = 64;
/// How long a remembered value lasts after it was last seen.
pub(crate) const METADATA_CACHE_EXPIRY: Duration = Duration::from_secs(300);

/// The wait before the next read after `failures` failed reads in a row:
/// 1 s, 2 s, 4 s ... up to 5 min.
pub(crate) fn read_backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    (READ_BACKOFF_FIRST * (1u32 << doublings)).min(READ_BACKOFF_MAX)
}

/// What a radio record names: the endpoint tag and the hash of one readable
/// value (ADR-0012 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MetadataKey {
    pub tag: [u8; 4],
    pub hash: u32,
}

struct CacheEntry<V> {
    value: Option<V>,
    failures: u32,
    retry_at: Instant,
    seen: Instant,
}

/// The readable values a managed scanner has read, keyed by the radio
/// record that names them, for the scanner's lifetime: a value read once is
/// not read again while its record is unchanged, across scans and address
/// rotation. Failed reads back off (see [`read_backoff`]). Bounded:
/// [`METADATA_CACHE_CAPACITY`] entries, least recently seen first out, and
/// none older than [`METADATA_CACHE_EXPIRY`]. Nearby's
/// `discovered_peripheral_tracker.cc` keeps its GATT reads the same way.
pub(crate) struct MetadataCache<V> {
    entries: HashMap<MetadataKey, CacheEntry<V>>,
}

impl<V> Default for MetadataCache<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<V> MetadataCache<V> {
    /// The value read for `key`, if any. Marks it seen.
    pub(crate) fn hit(&mut self, key: MetadataKey, now: Instant) -> Option<&V> {
        self.prune(now);
        let entry = self.entries.get_mut(&key)?;
        entry.value.as_ref()?;
        entry.seen = now;
        entry.value.as_ref()
    }

    /// [`hit`](Self::hit) for a fresh sighting only. BlueZ reports the
    /// devices it cached at the start of every discovery, without a signal
    /// strength, and a device that is gone must not be reported from them.
    pub(crate) fn fresh_hit(
        &mut self,
        key: MetadataKey,
        peer: &DiscoveredPeer,
        now: Instant,
    ) -> Option<&V> {
        peer.rssi?;
        self.hit(key, now)
    }

    /// When `key` may be read next: now, or after its backoff.
    pub(crate) fn retry_at(&self, key: MetadataKey, now: Instant) -> Instant {
        self.entries
            .get(&key)
            .map_or(now, |entry| entry.retry_at.max(now))
    }

    /// Remember `value` for `key`.
    pub(crate) fn succeeded(&mut self, key: MetadataKey, value: V, now: Instant) {
        self.prune(now);
        self.make_room(key);
        self.entries.insert(
            key,
            CacheEntry {
                value: Some(value),
                failures: 0,
                retry_at: now,
                seen: now,
            },
        );
    }

    /// Record a failed read of `key`. Returns the wait before the next one.
    pub(crate) fn failed(&mut self, key: MetadataKey, now: Instant) -> Duration {
        self.prune(now);
        self.make_room(key);
        let entry = self.entries.entry(key).or_insert(CacheEntry {
            value: None,
            failures: 0,
            retry_at: now,
            seen: now,
        });
        entry.failures += 1;
        entry.seen = now;
        let wait = read_backoff(entry.failures);
        entry.retry_at = now + wait;
        wait
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, entry| now.duration_since(entry.seen) < METADATA_CACHE_EXPIRY);
    }

    fn make_room(&mut self, key: MetadataKey) {
        if self.entries.contains_key(&key) || self.entries.len() < METADATA_CACHE_CAPACITY {
            return;
        }
        if let Some(oldest) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.seen)
            .map(|(key, _)| *key)
        {
            self.entries.remove(&oldest);
        }
    }
}

/// Which read a tracked peer waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadKind {
    /// No radio record: read now and then, at the refresh interval.
    Unnamed,
    /// An advertisement header names the value: read once per key.
    Header(MetadataKey),
    /// The peer was reported from its fast advertisement; one read fills in
    /// the fields the radio could not carry. Failures are not reported: the
    /// peer already was.
    Completion(MetadataKey),
}

pub(crate) struct TrackedPeer {
    peer: DiscoveredPeer,
    next_read_at: Instant,
    last_advertisement: Option<GattAdvertisement>,
    consecutive_failures: u32,
    has_been_reported: bool,
    kind: ReadKind,
    read_done: bool,
    /// The last sighting: a full managed table forgets its stalest peer.
    last_seen: Instant,
}

impl TrackedPeer {
    /// A peer without a radio record. Read after [`SCAN_RESPONSE_GRACE`].
    pub(crate) fn managed(peer: DiscoveredPeer) -> Self {
        Self::new(
            peer,
            ReadKind::Unnamed,
            Instant::now() + SCAN_RESPONSE_GRACE,
        )
    }
    /// A peer without a radio record whose address a read already matched to
    /// an endpoint: reported from that read, read again after
    /// [`KNOWN_PEER_REFRESH`] or when its radio fields change.
    pub(crate) fn known(peer: DiscoveredPeer) -> Self {
        let mut tracked = Self::new(peer, ReadKind::Unnamed, Instant::now() + KNOWN_PEER_REFRESH);
        tracked.has_been_reported = true;
        tracked
    }
    /// After a read of a peer without a radio record: the next one waits
    /// [`KNOWN_PEER_REFRESH`], not the 10 s refresh.
    pub(crate) fn known_refresh(&mut self) {
        self.next_read_at = Instant::now() + KNOWN_PEER_REFRESH;
    }
    /// A peer whose advertisement header names `key`: read at `read_at`.
    pub(crate) fn header(peer: DiscoveredPeer, key: MetadataKey, read_at: Instant) -> Self {
        Self::new(peer, ReadKind::Header(key), read_at)
    }
    /// A peer already reported from its fast advertisement, whose readable
    /// value `key` has more than the radio carried.
    pub(crate) fn completion(peer: DiscoveredPeer, key: MetadataKey, read_at: Instant) -> Self {
        let mut tracked = Self::new(peer, ReadKind::Completion(key), read_at);
        tracked.last_advertisement = Some(GattAdvertisement {
            manufacturer_data: tracked.peer.manufacturer_data.clone(),
            service_data: tracked.peer.service_data.clone(),
            local_name: tracked.peer.name.clone(),
        });
        tracked
    }
    fn new(peer: DiscoveredPeer, kind: ReadKind, next_read_at: Instant) -> Self {
        Self {
            peer,
            next_read_at,
            last_advertisement: None,
            consecutive_failures: 0,
            has_been_reported: false,
            kind,
            read_done: false,
            last_seen: Instant::now(),
        }
    }
    /// The key of a header or completion read.
    pub(crate) fn key(&self) -> Option<MetadataKey> {
        match self.kind {
            ReadKind::Unnamed => None,
            ReadKind::Header(key) | ReadKind::Completion(key) => Some(key),
        }
    }
    pub(crate) fn is_unnamed(&self) -> bool {
        self.kind == ReadKind::Unnamed
    }
    pub(crate) fn is_completion(&self) -> bool {
        matches!(self.kind, ReadKind::Completion(_))
    }
    /// Whether this peer waits for, or runs, a read of `key` that has not
    /// succeeded yet.
    pub(crate) fn awaits_read(&self, key: MetadataKey) -> bool {
        self.key() == Some(key) && !self.read_done
    }
    pub(crate) fn last_seen(&self) -> Instant {
        self.last_seen
    }
    pub(crate) fn update_radio(&mut self, peer: DiscoveredPeer) {
        self.last_seen = Instant::now();
        if self.peer.manufacturer_data != peer.manufacturer_data
            || self.peer.service_data != peer.service_data
            || self.peer.name != peer.name
        {
            self.next_read_at = Instant::now();
        }
        self.peer = peer;
    }
    /// A new sighting of a keyed peer (`completion`: from its fast
    /// advertisement). A changed key or kind is read again, at `read_at`;
    /// an unchanged one only updates the radio fields.
    pub(crate) fn update_keyed(
        &mut self,
        peer: DiscoveredPeer,
        key: MetadataKey,
        read_at: Instant,
        completion: bool,
    ) {
        if self.key() != Some(key) || self.is_completion() != completion {
            *self = if completion {
                Self::completion(peer, key, read_at)
            } else {
                Self::header(peer, key, read_at)
            };
            return;
        }
        if completion && !self.read_done {
            // Reported again from the radio: the read reports what differs from this.
            *self = Self {
                next_read_at: self.next_read_at,
                consecutive_failures: self.consecutive_failures,
                has_been_reported: self.has_been_reported,
                ..Self::completion(peer, key, read_at)
            };
            return;
        }
        self.last_seen = Instant::now();
        self.peer = peer;
    }
    /// A keyed peer whose cached value is gone: a finished read runs again
    /// at `read_at`.
    pub(crate) fn rearm(&mut self, read_at: Instant) {
        if self.read_done {
            self.read_done = false;
            self.next_read_at = read_at;
        }
    }
    pub(crate) fn managed_due(&self, now: Instant, scan_active: bool) -> bool {
        !self.read_done && (!self.has_been_reported || scan_active) && self.next_read_at <= now
    }
    pub(crate) fn managed_initial(&self) -> bool {
        !self.has_been_reported
    }
    pub(crate) fn radio_peer(&self) -> DiscoveredPeer {
        self.peer.clone()
    }
    pub(crate) fn managed_scheduled(&mut self) {
        self.next_read_at = Instant::now() + GattAdvertisementPolicy::default().refresh;
    }
    pub(crate) fn managed_deferred(&mut self) {
        // Report admission failure once. An active scan can try again: a peer
        // without a record at refresh, a keyed read after the first backoff.
        self.has_been_reported = true;
        if self.is_unnamed() {
            self.managed_scheduled();
        } else {
            self.next_read_at = Instant::now() + READ_BACKOFF_FIRST;
        }
    }
    /// A read finished: `peer` on success. `retry_at` is when a keyed read
    /// may run again after a failure (the cache's backoff). Returns whether
    /// to report: a success with changed fields, or the first failure of a
    /// peer not reported yet.
    pub(crate) fn managed_result(
        &mut self,
        peer: Option<&DiscoveredPeer>,
        retry_at: Option<Instant>,
    ) -> bool {
        let was_reported = std::mem::replace(&mut self.has_been_reported, true);
        let Some(peer) = peer else {
            self.consecutive_failures += 1;
            self.next_read_at = retry_at
                .unwrap_or_else(|| Instant::now() + read_backoff(self.consecutive_failures));
            return !was_reported && !self.is_completion();
        };
        self.consecutive_failures = 0;
        if self.is_unnamed() {
            self.managed_scheduled();
        } else {
            self.read_done = true;
        }
        let metadata = GattAdvertisement {
            manufacturer_data: peer.manufacturer_data.clone(),
            service_data: peer.service_data.clone(),
            local_name: peer.name.clone(),
        };
        let changed = self.last_advertisement.as_ref() != Some(&metadata);
        self.last_advertisement = Some(metadata);
        changed
    }
    fn record_read(
        &mut self,
        service: ServiceUuid,
        result: Result<GattAdvertisement>,
    ) -> Option<DiscoveredPeer> {
        match result {
            Ok(advertisement) => {
                self.consecutive_failures = 0;
                if self.last_advertisement.as_ref() == Some(&advertisement) {
                    return None;
                }
                log::debug!("gatt advertisement: read from {}", self.peer.address.0);
                let peer = advertisement.merge_into(service, self.peer.clone());
                self.last_advertisement = Some(advertisement);
                self.has_been_reported = true;
                Some(peer)
            }
            Err(err) => {
                self.consecutive_failures += 1;
                log::debug!(
                    "gatt advertisement: reading {} failed ({err}), failure {}",
                    self.peer.address.0,
                    self.consecutive_failures
                );
                // Keep failed peers in the bounded table for the rest of this scan.
                let was_reported = std::mem::replace(&mut self.has_been_reported, true);
                (!was_reported).then(|| self.peer.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> GattAdvertisement {
        let mut advertisement = GattAdvertisement::default();
        advertisement
            .manufacturer_data
            .insert(0xFFFF, vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]);
        advertisement
            .service_data
            .insert(ServiceUuid(Uuid::from_u128(7)), vec![9, 9]);
        advertisement.local_name = Some("Desktop".into());
        advertisement
    }

    #[test]
    fn a_gatt_advertisement_round_trips() {
        let advertisement = sample();
        let bytes = advertisement.encode();
        assert_eq!(bytes[0], FORMAT_VERSION);
        // Manufacturer data: length 8 (type + 2-byte company + 5 bytes).
        assert_eq!(&bytes[1..4], &[8, AD_MANUFACTURER_DATA, 0xFF]);
        assert_eq!(GattAdvertisement::decode(&bytes).unwrap(), advertisement);
    }

    #[test]
    fn unknown_ad_types_are_skipped_and_bad_input_refused() {
        let mut bytes = sample().encode();
        bytes.extend_from_slice(&[3, 0x42, 1, 2]);
        assert_eq!(GattAdvertisement::decode(&bytes).unwrap(), sample());
        assert!(GattAdvertisement::decode(&[]).is_err(), "empty");
        assert!(
            GattAdvertisement::decode(&[2, 1, 0xFF]).is_err(),
            "unknown version"
        );
        assert!(
            GattAdvertisement::decode(&[FORMAT_VERSION, 9, AD_MANUFACTURER_DATA]).is_err(),
            "truncated"
        );
    }

    #[test]
    fn an_oversized_entry_is_left_out() {
        let mut advertisement = GattAdvertisement::default();
        advertisement.manufacturer_data.insert(1, vec![0; 300]);
        advertisement.local_name = Some("ok".into());
        let decoded = GattAdvertisement::decode(&advertisement.encode()).unwrap();
        assert!(decoded.manufacturer_data.is_empty());
        assert_eq!(decoded.local_name.as_deref(), Some("ok"));
    }

    fn key(n: u32) -> MetadataKey {
        MetadataKey {
            tag: [1, 2, 3, 4],
            hash: n,
        }
    }

    fn sighting(rssi: Option<i16>) -> DiscoveredPeer {
        DiscoveredPeer {
            address: PeerAddress("AA".into()),
            name: None,
            services: Vec::new(),
            manufacturer_data: BTreeMap::new(),
            service_data: BTreeMap::new(),
            rssi,
        }
    }

    #[test]
    fn the_metadata_cache_keeps_the_64_most_recently_seen() {
        let start = Instant::now();
        let mut cache = MetadataCache::default();
        for n in 0..METADATA_CACHE_CAPACITY as u32 {
            cache.succeeded(key(n), n, start + Duration::from_millis(u64::from(n)));
        }
        let later = start + Duration::from_secs(1);
        // Seen again, so not the oldest any more.
        assert_eq!(cache.hit(key(0), later), Some(&0));
        cache.succeeded(key(1000), 1000, later);
        assert_eq!(cache.len(), METADATA_CACHE_CAPACITY);
        assert_eq!(
            cache.hit(key(1), later),
            None,
            "least recently seen went first"
        );
        assert_eq!(cache.hit(key(0), later), Some(&0));
        assert_eq!(cache.hit(key(1000), later), Some(&1000));
    }

    #[test]
    fn the_metadata_cache_forgets_after_five_minutes_unseen() {
        let start = Instant::now();
        let mut cache = MetadataCache::default();
        cache.succeeded(key(1), 1, start);
        cache.succeeded(key(2), 2, start);
        let refreshed = start + Duration::from_secs(200);
        assert_eq!(cache.hit(key(2), refreshed), Some(&2));
        let expired = start + METADATA_CACHE_EXPIRY;
        assert_eq!(cache.hit(key(1), expired), None);
        assert_eq!(cache.hit(key(2), expired), Some(&2), "seen 200 s in");
        assert_eq!(cache.hit(key(2), expired + METADATA_CACHE_EXPIRY), None);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn failed_reads_back_off_from_one_second_to_five_minutes() {
        let expected = [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300];
        let waits: Vec<u64> = (1..=expected.len() as u32)
            .map(|failures| read_backoff(failures).as_secs())
            .collect();
        assert_eq!(waits, expected);
        let now = Instant::now();
        let mut cache: MetadataCache<u32> = MetadataCache::default();
        assert_eq!(cache.retry_at(key(5), now), now, "unknown: read now");
        assert_eq!(cache.failed(key(5), now), Duration::from_secs(1));
        assert_eq!(cache.failed(key(5), now), Duration::from_secs(2));
        assert_eq!(cache.retry_at(key(5), now), now + Duration::from_secs(2));
        assert_eq!(cache.hit(key(5), now), None, "a failure is no value");
        cache.succeeded(key(5), 5, now);
        assert_eq!(cache.failed(key(6), now), Duration::from_secs(1), "per key");
    }

    #[test]
    fn a_cache_hit_without_signal_strength_is_not_a_sighting() {
        let now = Instant::now();
        let mut cache = MetadataCache::default();
        cache.succeeded(key(1), "value", now);
        assert_eq!(cache.fresh_hit(key(1), &sighting(None), now), None);
        assert_eq!(
            cache.fresh_hit(key(1), &sighting(Some(-60)), now),
            Some(&"value")
        );
    }

    #[test]
    fn merging_keeps_the_radio_data_and_prefers_the_chosen_name() {
        let service = ServiceUuid(Uuid::from_u128(1));
        let peer = DiscoveredPeer {
            address: PeerAddress("AA".into()),
            name: Some("DESKTOP-J4VODHN".into()),
            services: vec![service],
            manufacturer_data: BTreeMap::new(),
            service_data: BTreeMap::new(),
            rssi: Some(-50),
        };
        assert!(needs_gatt_advertisement(&peer));
        let merged = sample().merge_into(service, peer);
        assert_eq!(merged.name.as_deref(), Some("Desktop"));
        assert_eq!(merged.manufacturer_data.get(&0xFFFF).map(Vec::len), Some(5));
        assert_eq!(merged.rssi, Some(-50));
        assert!(!needs_gatt_advertisement(&merged));
    }
}
