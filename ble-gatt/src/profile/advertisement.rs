//! The GATT advertisement: advertisement data a peripheral serves in a
//! readable characteristic, for the platforms that cannot put it in the
//! radio advertisement (layer: Use Cases; see `docs/architecture.md` and
//! ADR-0012).
//!
//! The design follows Google Nearby Connections (`google/nearby`,
//! Apache-2.0), which ships on Android, Windows and Apple:
//!
//! - **Fast advertisement.** Where the platform allows it (Android, Linux),
//!   the advertisement data travels in the radio advertisement as before,
//!   and a scanner needs no connection. Nearby's "fast advertisement" is
//!   the same idea.
//! - **GATT advertisement.** Every served datagram service also serves the
//!   whole advertisement data in [`GATT_ADVERTISEMENT_CHARACTERISTIC`].
//!   Windows cannot advertise manufacturer data with a GATT service, and
//!   Apple cannot advertise manufacturer or service data at all; a scanner
//!   that finds such a peer connects, reads the characteristic and
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
    advertised_name, CharacteristicUuid, DiscoveredPeer, GattCharacteristicSpec, GattServiceSpec, PeerAddress,
    ServiceUuid,
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
        let mut out = vec![FORMAT_VERSION];
        let mut push = |kind: u8, data: Vec<u8>| {
            if data.len() > AD_MAX_DATA_LEN || out.len() + 2 + data.len() > MAX_ATT_ATTRIBUTE_LEN {
                log::warn!("gatt advertisement: AD type {kind:#04x} ({} bytes) does not fit, left out", data.len());
                return;
            }
            out.push((data.len() + 1) as u8);
            out.push(kind);
            out.extend_from_slice(&data);
        };
        for (company, value) in &self.manufacturer_data {
            let mut data = company.to_le_bytes().to_vec();
            data.extend_from_slice(value);
            push(AD_MANUFACTURER_DATA, data);
        }
        for (uuid, value) in &self.service_data {
            let mut data = uuid.0.as_u128().to_le_bytes().to_vec();
            data.extend_from_slice(value);
            push(AD_SERVICE_DATA_128, data);
        }
        if let Some(name) = &self.local_name {
            push(AD_COMPLETE_LOCAL_NAME, name.as_bytes().to_vec());
        }
        out
    }

    /// Read a characteristic value [`encode`](Self::encode) wrote.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (&version, mut rest) =
            bytes.split_first().ok_or_else(|| BleError::Gatt("gatt advertisement: empty value".into()))?;
        if version != FORMAT_VERSION {
            return Err(BleError::Gatt(format!("gatt advertisement: unsupported version {version}")));
        }
        let mut out = Self::default();
        while let Some((&len, tail)) = rest.split_first() {
            let len = len as usize;
            if len == 0 {
                // A zero length ends the data early (Core Supplement Part A).
                break;
            }
            if tail.len() < len {
                return Err(BleError::Gatt("gatt advertisement: truncated AD structure".into()));
            }
            let (structure, next) = tail.split_at(len);
            rest = next;
            let (kind, data) = (structure[0], &structure[1..]);
            match kind {
                AD_MANUFACTURER_DATA => {
                    if let Some((company, value)) = data.split_first_chunk::<2>() {
                        out.manufacturer_data.insert(u16::from_le_bytes(*company), value.to_vec());
                    }
                }
                AD_SERVICE_DATA_16 | AD_SERVICE_DATA_32 | AD_SERVICE_DATA_128 => {
                    if let Some((uuid, value)) = service_data_entry(kind, data) {
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
            peer.manufacturer_data.entry(*company).or_insert_with(|| value.clone());
        }
        for (uuid, value) in &self.service_data {
            peer.service_data.entry(*uuid).or_insert_with(|| value.clone());
        }
        peer.name = self
            .local_name
            .clone()
            .or_else(|| advertised_name(service, &peer.service_data, peer.name.take()));
        peer
    }
}

fn service_data_entry(kind: u8, data: &[u8]) -> Option<(ServiceUuid, Vec<u8>)> {
    let short = |value: u32| Uuid::from_u128(BLUETOOTH_BASE_UUID | ((value as u128) << 96));
    let (uuid, rest) = match kind {
        AD_SERVICE_DATA_16 => {
            let (bytes, rest) = data.split_first_chunk::<2>()?;
            (short(u16::from_le_bytes(*bytes) as u32), rest)
        }
        AD_SERVICE_DATA_32 => {
            let (bytes, rest) = data.split_first_chunk::<4>()?;
            (short(u32::from_le_bytes(*bytes)), rest)
        }
        _ => {
            let (bytes, rest) = data.split_first_chunk::<16>()?;
            (Uuid::from_u128(u128::from_le_bytes(*bytes)), rest)
        }
    };
    Some((ServiceUuid(uuid), rest.to_vec()))
}

/// The characteristic a peripheral adds to its service to serve `spec`'s
/// advertisement data: readable only.
pub fn characteristic_spec(spec: &GattServiceSpec) -> GattCharacteristicSpec {
    GattCharacteristicSpec {
        uuid: GATT_ADVERTISEMENT_CHARACTERISTIC,
        readable: true,
        writable: false,
        notifiable: false,
        initial_value: GattAdvertisement::of(spec).encode(),
    }
}

/// Whether a scanner should read `peer`'s GATT advertisement: its radio
/// advertisement carried no manufacturer and no service data, which is how
/// a Windows or Apple peer looks.
pub fn needs_gatt_advertisement(peer: &DiscoveredPeer) -> bool {
    peer.manufacturer_data.is_empty() && peer.service_data.is_empty()
}

/// Connect to `peer`, read its GATT advertisement and disconnect.
pub async fn read(driver: &dyn Driver, peer: &PeerAddress) -> Result<GattAdvertisement> {
    let mut connection = driver.connect(peer).await?;
    let value = connection.read(GATT_ADVERTISEMENT_CHARACTERISTIC).await;
    // Always hang up: a link left open is inherited by the next dial
    // (ADR-0009), and this one was only for the read.
    let _ = connection.disconnect().await;
    GattAdvertisement::decode(&value?)
}

/// How [`complete`] reads GATT advertisements.
#[derive(Debug, Clone, Copy)]
pub struct GattAdvertisementPolicy {
    /// How often a peer's GATT advertisement is read again while the scan
    /// runs. The radio advertisement of such a peer does not change when
    /// its data does, so this bounds how stale its data can be (an Add
    /// Device flag, for example). Nearby learns of a change from a hash in
    /// its advertisement header instead; a 128-bit service UUID leaves no
    /// room for one on Windows.
    pub refresh: Duration,
    /// Longest one read may take, connection included.
    pub timeout: Duration,
    /// Reads that fail in a row before a peer is no longer read.
    pub max_failures: u32,
    /// Most peers tracked at once; more are reported without a read.
    pub max_peers: usize,
}

impl Default for GattAdvertisementPolicy {
    fn default() -> Self {
        Self { refresh: Duration::from_secs(10), timeout: Duration::from_secs(10), max_failures: 3, max_peers: 64 }
    }
}

struct Tracked {
    peer: DiscoveredPeer,
    due: Instant,
    last: Option<GattAdvertisement>,
    failures: u32,
    reported: bool,
}

/// `scan` (a scan for `service` on `driver`), with the GATT advertisement
/// of every peer that [needs one](needs_gatt_advertisement) read and merged
/// in. Peers whose radio advertisement carries the data pass straight
/// through. Such a peer is reported after its first read, and again when a
/// later read finds different data. When a read fails, the peer is reported
/// once as the radio advertisement shows it.
///
/// Reads run one at a time, so a crowd of peers cannot exhaust the
/// platform's connection slots. A scan error ends the stream as it ends a
/// scan.
pub fn complete(
    driver: Arc<dyn Driver>, service: ServiceUuid, mut scan: BoxStream<Result<DiscoveredPeer>>,
    policy: GattAdvertisementPolicy,
) -> BoxStream<Result<DiscoveredPeer>> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let mut tracked: HashMap<PeerAddress, Tracked> = HashMap::new();
        let mut scanning = true;
        loop {
            if tx.is_closed() {
                return;
            }
            // After the scan ends, only peers never read are still read.
            let due = tracked
                .iter()
                .filter(|(_, t)| scanning || t.last.is_none() && !t.reported)
                .min_by_key(|(_, t)| t.due)
                .map(|(address, t)| (address.clone(), t.due));
            if !scanning && due.is_none() {
                return;
            }
            tokio::select! {
                item = scan.next(), if scanning => match item {
                    None => scanning = false,
                    Some(Err(err)) => {
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                    Some(Ok(peer)) if !needs_gatt_advertisement(&peer) => {
                        if tx.send(Ok(peer)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(peer)) => {
                        if let Some(known) = tracked.get_mut(&peer.address) {
                            known.peer = peer;
                        } else if tracked.len() < policy.max_peers {
                            let address = peer.address.clone();
                            tracked.insert(address, Tracked { peer, due: Instant::now(), last: None, failures: 0, reported: false });
                        } else if tx.send(Ok(peer)).await.is_err() {
                            return;
                        }
                    }
                },
                _ = tokio::time::sleep_until(due.as_ref().map(|(_, at)| *at).unwrap_or_else(Instant::now)), if due.is_some() => {
                    let Some((address, _)) = due else { continue };
                    let read = tokio::time::timeout(policy.timeout, read(driver.as_ref(), &address)).await;
                    let Some(entry) = tracked.get_mut(&address) else { continue };
                    entry.due = Instant::now() + policy.refresh;
                    match read {
                        Ok(Ok(advertisement)) => {
                            entry.failures = 0;
                            if entry.last.as_ref() != Some(&advertisement) {
                                log::debug!("gatt advertisement: read from {}", address.0);
                                let peer = advertisement.merge_into(service, entry.peer.clone());
                                entry.last = Some(advertisement);
                                entry.reported = true;
                                if tx.send(Ok(peer)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        failed => {
                            let reason = match failed {
                                Ok(Err(err)) => err.to_string(),
                                _ => format!("no answer in {:?}", policy.timeout),
                            };
                            entry.failures += 1;
                            log::debug!("gatt advertisement: reading {} failed ({reason}), failure {}", address.0, entry.failures);
                            if !entry.reported {
                                entry.reported = true;
                                if tx.send(Ok(entry.peer.clone())).await.is_err() {
                                    return;
                                }
                            }
                            if entry.failures >= policy.max_failures {
                                tracked.remove(&address);
                            }
                        }
                    }
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> GattAdvertisement {
        let mut advertisement = GattAdvertisement::default();
        advertisement.manufacturer_data.insert(0xFFFF, vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]);
        advertisement.service_data.insert(ServiceUuid(Uuid::from_u128(7)), vec![9, 9]);
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
        assert!(GattAdvertisement::decode(&[2, 1, 0xFF]).is_err(), "unknown version");
        assert!(GattAdvertisement::decode(&[FORMAT_VERSION, 9, AD_MANUFACTURER_DATA]).is_err(), "truncated");
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
