import { invoke } from "@tauri-apps/api/core";

export interface Capabilities {
  central: boolean;
  peripheral: boolean;
}

export interface DiscoveredPeer {
  address: string;
  name: string | null;
}

export interface CharacteristicSpec {
  uuid: string;
  readable: boolean;
  writable: boolean;
  notifiable: boolean;
  initialValue: number[];
}

export async function capabilities(): Promise<Capabilities> {
  return invoke("plugin:ble-gatt|ble_capabilities");
}

export async function advertise(serviceUuid: string, characteristics: CharacteristicSpec[]): Promise<void> {
  return invoke("plugin:ble-gatt|ble_advertise", {
    serviceUuid,
    characteristics: characteristics.map((c) => ({
      uuid: c.uuid,
      readable: c.readable,
      writable: c.writable,
      notifiable: c.notifiable,
      initial_value: c.initialValue,
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

export async function write(handle: number, characteristicUuid: string, value: number[]): Promise<void> {
  return invoke("plugin:ble-gatt|ble_write", { handle, characteristicUuid, value });
}

export async function disconnect(handle: number): Promise<void> {
  return invoke("plugin:ble-gatt|ble_disconnect", { handle });
}
