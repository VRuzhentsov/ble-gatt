//! Conversions between WinRT and `crate::entities` types.

use std::collections::BTreeMap;

use uuid::Uuid;
use windows::core::GUID;
use windows::Devices::Bluetooth::Advertisement::BluetoothLEAdvertisementDataSection;
use windows::Devices::Bluetooth::GenericAttributeProfile::{GattCommunicationStatus, GattSession};
use windows::Devices::Radios::RadioState;
use windows::Storage::Streams::{DataReader, DataWriter, IBuffer};

use crate::entities::error::BleError;
use crate::entities::models::{RadioStatus, ServiceUuid};

// AD types (Bluetooth Assigned Numbers, 2.3) that carry service data.
pub(super) const AD_SERVICE_DATA_16: u8 = 0x16;
pub(super) const AD_SERVICE_DATA_32: u8 = 0x20;
pub(super) const AD_SERVICE_DATA_128: u8 = 0x21;

/// The Bluetooth Base UUID, `00000000-0000-1000-8000-00805F9B34FB`.
const BLUETOOTH_BASE_UUID: u128 = 0x0000_0000_0000_1000_8000_0080_5F9B_34FB;

/// Encodes and decodes WinRT GATT values and Bluetooth advertisement fields.
pub(super) struct WindowsCodec;

impl WindowsCodec {
    pub(super) fn to_guid(uuid: Uuid) -> GUID {
        GUID::from_u128(uuid.as_u128())
    }

    pub(super) fn from_guid(guid: GUID) -> Uuid {
        Uuid::from_u128(guid.to_u128())
    }

    pub(super) fn encode_buffer(bytes: &[u8]) -> windows::core::Result<IBuffer> {
        let writer = DataWriter::new()?;
        writer.WriteBytes(bytes)?;
        writer.DetachBuffer()
    }

    pub(super) fn to_buffer(bytes: &[u8]) -> crate::Result<IBuffer> {
        WindowsCodec::encode_buffer(bytes).map_err(|err| BleError::windows_error("DataWriter", err))
    }

    pub(super) fn from_buffer(buffer: &IBuffer) -> windows::core::Result<Vec<u8>> {
        let len = buffer.Length()? as usize;
        let mut bytes = vec![0u8; len];
        if len > 0 {
            DataReader::FromBuffer(buffer)?.ReadBytes(&mut bytes)?;
        }
        Ok(bytes)
    }

    /// `0x112233445566` → `"11:22:33:44:55:66"`, the form the Linux and Android
    /// drivers report.
    pub(super) fn format_address(address: u64) -> String {
        let bytes = address.to_be_bytes();
        format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]
        )
    }

    pub(super) fn parse_address(address: &str) -> Option<u64> {
        let parts: Vec<&str> = address.split(':').collect();
        if parts.len() != 6 {
            return None;
        }
        parts.iter().try_fold(0u64, |acc, part| {
            u8::from_str_radix(part, 16)
                .ok()
                .map(|byte| (acc << 8) | byte as u64)
        })
    }

    /// The remote address in a WinRT Bluetooth LE device id
    /// (`BluetoothLE#BluetoothLE<local>-<remote>`), formatted like
    /// [`format_address`]. A GATT server only learns a central through its
    /// `GattSession`, whose `DeviceId` is this string. Falls back to the whole
    /// id, which still identifies the central, when the format is not the
    /// expected one.
    pub(super) fn address_from_device_id(id: &str) -> String {
        id.rsplit('-')
            .next()
            .filter(|tail| WindowsCodec::parse_address(tail).is_some())
            .map(|tail| tail.to_ascii_uppercase())
            .unwrap_or_else(|| id.to_string())
    }

    pub(super) fn session_address(session: &GattSession) -> windows::core::Result<String> {
        Ok(WindowsCodec::address_from_device_id(
            &session.DeviceId()?.Id()?.to_string(),
        ))
    }

    /// One service data AD structure: its UUID (little-endian, 16, 32 or 128
    /// bits) and the bytes after it. `None` for other AD types.
    pub(super) fn parse_service_data(data_type: u8, data: &[u8]) -> Option<(ServiceUuid, Vec<u8>)> {
        let uuid_len = match data_type {
            AD_SERVICE_DATA_16 => 2,
            AD_SERVICE_DATA_32 => 4,
            AD_SERVICE_DATA_128 => 16,
            _ => return None,
        };
        if data.len() < uuid_len {
            return None;
        }
        let (uuid_bytes, value) = data.split_at(uuid_len);
        // AD fields are little-endian: the last byte is the most significant.
        let number = uuid_bytes
            .iter()
            .rev()
            .fold(0u128, |acc, byte| (acc << 8) | u128::from(*byte));
        // A 16- or 32-bit UUID stands for the Base UUID with its top 32 bits
        // replaced (Core Specification, Vol 3, Part B, 2.5.1).
        let uuid = if uuid_len == 16 {
            Uuid::from_u128(number)
        } else {
            Uuid::from_u128(BLUETOOTH_BASE_UUID | (number << 96))
        };
        Some((ServiceUuid(uuid), value.to_vec()))
    }

    /// Service data from the raw advertisement sections. WinRT parses service
    /// UUIDs and manufacturer data, but leaves service data as raw sections.
    pub(super) fn service_data_of(
        sections: impl IntoIterator<Item = BluetoothLEAdvertisementDataSection>,
    ) -> BTreeMap<ServiceUuid, Vec<u8>> {
        sections
            .into_iter()
            .filter_map(|section| {
                let kind = section.DataType().ok()?;
                let bytes = WindowsCodec::from_buffer(&section.Data().ok()?).ok()?;
                WindowsCodec::parse_service_data(kind, &bytes)
            })
            .collect()
    }

    /// Largest notification value for a subscribed client, from its
    /// `GattSubscribedClient::MaxNotificationSize` (ATT MTU minus the 3-byte
    /// header: 514 at MTU 517), capped at [`MAX_ATT_ATTRIBUTE_LEN`] as the
    /// Linux and Android drivers do. A peer drops a longer notification.
    ///
    /// [`MAX_ATT_ATTRIBUTE_LEN`]: crate::hal::MAX_ATT_ATTRIBUTE_LEN
    pub(super) fn notification_len(max_notification_size: u16) -> usize {
        (max_notification_size as usize).min(crate::hal::MAX_ATT_ATTRIBUTE_LEN)
    }
}

impl BleError {
    pub(super) fn windows_error(context: &str, err: windows::core::Error) -> BleError {
        BleError::Gatt(format!("{context}: {err}"))
    }

    pub(super) fn communication_error(
        context: &str,
        status: GattCommunicationStatus,
        peer: &str,
    ) -> BleError {
        if status == GattCommunicationStatus::Unreachable {
            BleError::NotConnected(peer.to_string())
        } else if status == GattCommunicationStatus::AccessDenied {
            BleError::Gatt(format!("{context}: access denied"))
        } else if status == GattCommunicationStatus::ProtocolError {
            BleError::Gatt(format!("{context}: ATT protocol error"))
        } else {
            BleError::Gatt(format!("{context}: status {}", status.0))
        }
    }
}

impl RadioStatus {
    pub(super) fn from_windows(state: windows::core::Result<RadioState>) -> RadioStatus {
        match state {
            Ok(RadioState::On) => RadioStatus::On,
            _ => RadioStatus::Off,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_round_trips() {
        assert_eq!(
            WindowsCodec::format_address(0x1122_3344_55AB),
            "11:22:33:44:55:AB"
        );
        assert_eq!(
            WindowsCodec::parse_address("11:22:33:44:55:AB"),
            Some(0x1122_3344_55AB)
        );
        assert_eq!(
            WindowsCodec::parse_address(&WindowsCodec::format_address(0xC0FF_EE00_0001)),
            Some(0xC0FF_EE00_0001)
        );
        assert_eq!(WindowsCodec::parse_address("11:22:33"), None);
    }

    #[test]
    fn the_remote_address_is_read_from_a_device_id() {
        assert_eq!(
            WindowsCodec::address_from_device_id(
                "BluetoothLE#BluetoothLEb4:6b:fc:1a:2b:3c-5e:aa:bb:cc:dd:ee"
            ),
            "5E:AA:BB:CC:DD:EE"
        );
        assert_eq!(
            WindowsCodec::address_from_device_id("something-else"),
            "something-else"
        );
    }

    #[test]
    fn a_uuid_round_trips_through_a_guid() {
        let uuid = Uuid::parse_str("6e400001-b5a3-f393-e0a9-e50e24dcca9e").unwrap();
        assert_eq!(WindowsCodec::from_guid(WindowsCodec::to_guid(uuid)), uuid);
    }

    #[test]
    fn service_data_is_parsed_for_every_uuid_size() {
        // 16-bit 0x180F (Battery Service), value 0x64.
        let (uuid, value) =
            WindowsCodec::parse_service_data(AD_SERVICE_DATA_16, &[0x0F, 0x18, 0x64]).unwrap();
        assert_eq!(
            uuid.0,
            Uuid::parse_str("0000180f-0000-1000-8000-00805f9b34fb").unwrap()
        );
        assert_eq!(value, vec![0x64]);

        let full = Uuid::parse_str("6e400001-b5a3-f393-e0a9-e50e24dcca9e").unwrap();
        let mut raw = full.as_u128().to_le_bytes().to_vec();
        raw.extend_from_slice(b"Phone");
        let (uuid, value) = WindowsCodec::parse_service_data(AD_SERVICE_DATA_128, &raw).unwrap();
        assert_eq!(uuid.0, full);
        assert_eq!(value, b"Phone".to_vec());

        assert!(
            WindowsCodec::parse_service_data(AD_SERVICE_DATA_128, &[0x01, 0x02]).is_none(),
            "too short"
        );
        assert!(
            WindowsCodec::parse_service_data(0xFF, &[0x01, 0x02, 0x03]).is_none(),
            "not service data"
        );
    }

    #[test]
    fn a_notification_is_capped_at_the_att_attribute_limit() {
        // MTU 517: WinRT reports 514, two more than ATT permits.
        assert_eq!(WindowsCodec::notification_len(514), 512);
        assert_eq!(WindowsCodec::notification_len(512), 512);
        // MTU 247.
        assert_eq!(WindowsCodec::notification_len(244), 244);
    }
}
