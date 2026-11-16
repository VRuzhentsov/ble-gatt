//! Raw GATT calls shared by the two binaries and the radio-free tests.
use std::time::Duration;

use ble_gatt::{
    Adapter, CharacteristicUuid, GattCharacteristicSpec, GattEvent, GattServiceSpec, Role,
    ServiceUuid,
};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
// Private demo UUIDs, not Bluetooth SIG service IDs or product commands.
pub const SERVICE: ServiceUuid =
    ServiceUuid(Uuid::from_u128(0x7d32e4b0_53c5_4ae0_868a_6d46c765e001));
pub const VALUE: CharacteristicUuid =
    CharacteristicUuid(Uuid::from_u128(0x7d32e4b0_53c5_4ae0_868a_6d46c765e002));

/// Read the initial value, write bytes, and receive the same bytes by notification.
/// Each stage has a time limit. Always wait for disconnect after an exchange.
pub async fn central(
    adapter: &Adapter,
    value: Vec<u8>,
    wait: Duration,
) -> Result<(Vec<u8>, Vec<u8>)> {
    GattDemo { adapter, wait }.central(value).await
}

/// The adapter and operation deadline shared by the raw GATT demonstration.
struct GattDemo<'a> {
    adapter: &'a Adapter,
    wait: Duration,
}

impl GattDemo<'_> {
    async fn central(self, value: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>)> {
        let Self { adapter, wait } = self;
        // Twenty bytes fit a write and notification at the minimum ATT MTU.
        if value.is_empty() || value.len() > 20 {
            return Err("Use 1 to 20 bytes of text.".into());
        }
        let central = adapter.central();
        let peer = timeout(wait, async {
            let mut scan = central.scan(SERVICE).await?;
            println!("Scan for service {}.", SERVICE.0);
            let peer = scan.next().await.ok_or("Scan ended without a peer.")??;
            println!("Connect to {}.", peer.address.0);
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(peer)
            // Drop the scan before connecting.
        })
        .await
        .map_err(|_| "Scan timed out.")??;
        let mut connection = timeout(wait, central.connect(&peer.address))
            .await
            .map_err(|_| "Connect timed out.")??;
        let exchange = timeout(wait, async {
            let initial = connection.read(VALUE).await?;
            // Subscribe first so the response cannot arrive before the subscription.
            let mut notifications = connection.subscribe(VALUE).await?;
            connection.write(VALUE, value.clone()).await?;
            let reply = notifications
                .next()
                .await
                .ok_or("Notification stream ended.")??;
            if reply != value {
                return Err("The reply does not match the write.".into());
            }
            Ok((initial, reply))
        })
        .await
        .map_err(|_| "GATT exchange timed out.");
        let disconnected = timeout(wait, connection.disconnect())
            .await
            .map_err(|_| "Disconnect timed out.")?;
        disconnected?;
        exchange?
    }
}

/// Serve one write and echo it. Keep serving until that central disconnects.
pub async fn peripheral(adapter: &Adapter, wait: Duration) -> Result<Vec<u8>> {
    GattDemo { adapter, wait }.peripheral().await
}

impl GattDemo<'_> {
    async fn peripheral(self) -> Result<Vec<u8>> {
        let Self { adapter, wait } = self;
        let peripheral = adapter.peripheral();
        // Subscribe before serving so no write or disconnect event is lost.
        let mut events = adapter.events();
        let spec = GattServiceSpec::new(
            SERVICE,
            vec![GattCharacteristicSpec {
                uuid: VALUE,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"ready".to_vec(),
            }],
        );
        let server = timeout(wait, peripheral.serve(spec))
            .await
            .map_err(|_| "Start server timed out.")??;
        println!("Serve {}. Wait for a write.", SERVICE.0);
        let exchange = timeout(wait, async {
            let mut written = None;
            while let Some(event) = events.next().await {
                match event {
                    GattEvent::CharacteristicWritten {
                        peer,
                        characteristic,
                        value,
                    } if characteristic == VALUE && written.is_none() => {
                        println!("Write from {}: {:?}", peer.0, value);
                        peripheral
                            .notify_peer(&peer, None, VALUE, value.clone())
                            .await?;
                        written = Some((peer, value));
                    }
                    GattEvent::Disconnected {
                        peer,
                        local_role: Role::Peripheral,
                        ..
                    } => {
                        if let Some((writer, value)) = &written {
                            if peer == *writer {
                                return Ok(value.clone());
                            }
                        }
                    }
                    GattEvent::Lagged { .. } => {
                        return Err("Server lost events. Run it again.".into())
                    }
                    _ => {}
                }
            }
            Err("Server event stream ended.".into())
        })
        .await
        .map_err(|_| "Wait for a write and disconnect timed out.");
        // Hold ServerHandle through the exchange. Stop explicitly, even on timeout.
        timeout(wait, server.stop())
            .await
            .map_err(|_| "Stop server timed out.")??;
        exchange?
    }
}
