import { invoke, Channel } from "@tauri-apps/api/core";

export interface Capabilities {
  central: boolean;
  peripheral: boolean;
}

export interface DiscoveredPeer {
  address: string;
  name: string | null;
  /**
   * Manufacturer-specific advertisement data, keyed by Bluetooth SIG company
   * ID (as a decimal string, since JSON object keys are always strings).
   * Vendor devices frequently publish their real identity here — a serial
   * number or device EUI — rather than in the BLE name, making this the only
   * way to tell two units of the same product apart before connecting.
   */
  manufacturerData: Record<string, number[]>;
  /** Service advertisement data, keyed by service UUID. */
  serviceData: Record<string, number[]>;
  /** Signal strength in dBm, when the platform reports it. */
  rssi: number | null;
}

export interface CharacteristicSpec {
  uuid: string;
  readable: boolean;
  writable: boolean;
  notifiable: boolean;
  initialValue: number[];
}

export interface ConnectionMtu {
  /** Negotiated ATT MTU for this connection. */
  attMtu: number;
  /**
   * Largest payload that fits in a single write. Chunk bulk transfers
   * against this rather than a hardcoded constant — it is only known after
   * MTU negotiation and differs per peer and per platform.
   */
  maxWriteLen: number;
}

/** Connection lifecycle, as delivered by {@link watchEvents}. */
export type GattEvent =
  | { type: "connected"; address: string }
  | { type: "disconnected"; address: string }
  | {
      type: "characteristicWritten";
      address: string;
      characteristicUuid: string;
      value: number[];
    };

export async function capabilities(): Promise<Capabilities> {
  return invoke("plugin:ble-gatt|ble_capabilities");
}

export async function advertise(
  serviceUuid: string,
  characteristics: CharacteristicSpec[],
): Promise<void> {
  return invoke("plugin:ble-gatt|ble_advertise", {
    serviceUuid,
    characteristics: characteristics.map((c) => ({
      uuid: c.uuid,
      readable: c.readable,
      writable: c.writable,
      notifiable: c.notifiable,
      initialValue: c.initialValue,
    })),
  });
}

export async function stopAdvertising(): Promise<void> {
  return invoke("plugin:ble-gatt|ble_stop_advertising");
}

export async function notify(characteristicUuid: string, value: number[]): Promise<void> {
  return invoke("plugin:ble-gatt|ble_notify", { characteristicUuid, value });
}

export async function scanOnce(serviceUuid: string, timeoutMs: number): Promise<DiscoveredPeer[]> {
  return invoke("plugin:ble-gatt|ble_scan_once", { serviceUuid, timeoutMs });
}

export async function connect(address: string): Promise<number> {
  return invoke("plugin:ble-gatt|ble_connect", { address });
}

export async function read(handle: number, characteristicUuid: string): Promise<number[]> {
  return invoke("plugin:ble-gatt|ble_read", { handle, characteristicUuid });
}

/**
 * Write to a characteristic.
 *
 * `withoutResponse` opts into ATT Write Command — substantially faster for
 * bulk transfer, but the peer silently drops writes it cannot keep up with,
 * so the caller owns pacing and integrity checking. Omit it for the
 * acknowledged write, which is the safe default.
 */
export async function write(
  handle: number,
  characteristicUuid: string,
  value: number[],
  withoutResponse?: boolean,
): Promise<void> {
  return invoke("plugin:ble-gatt|ble_write", {
    handle,
    characteristicUuid,
    value,
    withoutResponse,
  });
}

/** Negotiated MTU for a live connection. See {@link ConnectionMtu.maxWriteLen}. */
export async function connectionMtu(handle: number): Promise<ConnectionMtu> {
  return invoke("plugin:ble-gatt|ble_connection_mtu", { handle });
}

/**
 * Subscribe to connection lifecycle events.
 *
 * This is the only way the frontend learns that a peer disappeared *without
 * being asked to* — out of range, powered off, firmware crash. Every other
 * function here reports failures of operations you initiated, so without
 * this a UI partway through a transfer would simply appear to stall.
 *
 * Returns a disposer; call it to stop receiving events.
 */
export async function watchEvents(handler: (event: GattEvent) => void): Promise<() => void> {
  const channel = new Channel<GattEvent>();
  channel.onmessage = handler;
  await invoke("plugin:ble-gatt|ble_watch_events", { onEvent: channel });
  return () => {
    // Detaching the handler stops delivery on the JS side; the Rust task
    // then observes the closed channel on its next send and exits.
    channel.onmessage = () => {};
  };
}

export async function disconnect(handle: number): Promise<void> {
  return invoke("plugin:ble-gatt|ble_disconnect", { handle });
}
