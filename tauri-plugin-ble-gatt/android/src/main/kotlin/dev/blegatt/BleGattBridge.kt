package dev.blegatt

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattServer
import android.bluetooth.BluetoothGattServerCallback
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.bluetooth.le.AdvertiseCallback
import android.bluetooth.le.AdvertiseData
import android.bluetooth.le.AdvertiseSettings
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.content.pm.PackageManager
import android.os.ParcelUuid
import android.util.Log
import java.util.UUID

private const val TAG = "BleGattBridge"

/**
 * Real Android BLE GATT central + peripheral implementation. Called from
 * Rust via raw JNI (`env.call_method(...)`) — not through Tauri's mobile
 * plugin `Invoke`/`@Command` machinery, so this class has no `Plugin`
 * base class or `@TauriPlugin` annotation. That is deliberate: it is what
 * keeps a hypothetical non-Tauri Android Rust consumer able to reuse this
 * file (copy it + the manifest permissions into their own app, no Tauri
 * Android runtime required) rather than being structurally locked into
 * Tauri's IPC path. See docs/adr/0002 for the full rationale.
 *
 * Permission checks are deliberately not performed here — declaring
 * `<uses-permission>` in this module's AndroidManifest.xml is this
 * bridge's responsibility; requesting the *runtime* grant from the user is
 * the consuming app's. An operation attempted without a granted permission
 * throws `SecurityException`, which Rust surfaces as a `BleError::Gatt`.
 */
class BleGattBridge(private val context: Context, private val nativeHandle: Long) {

    private val bluetoothManager =
        context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
    private val adapter: BluetoothAdapter? = bluetoothManager.adapter

    init {
        try {
            val hasBleFeature = context.packageManager.hasSystemFeature(PackageManager.FEATURE_BLUETOOTH_LE)
            Log.d(
                TAG,
                "init: FEATURE_BLUETOOTH_LE=$hasBleFeature adapter=${adapter != null} " +
                    "adapter.isEnabled=${adapter?.isEnabled} " +
                    "bluetoothLeScanner=${adapter?.bluetoothLeScanner != null} " +
                    "isMultipleAdvertisementSupported=${adapter?.isMultipleAdvertisementSupported} " +
                    "bluetoothLeAdvertiser=${adapter?.bluetoothLeAdvertiser != null}"
            )
        } catch (e: Exception) {
            Log.e(TAG, "init: capability probe threw", e)
        }
    }

    // --- Central role state ---
    private var scanCallback: ScanCallback? = null
    private val connectedGatts = HashMap<String, BluetoothGatt>()
    private val pendingCharacteristics = HashMap<String, HashMap<String, BluetoothGattCharacteristic>>()

    // --- Peripheral role state ---
    private var gattServer: BluetoothGattServer? = null
    private var advertiseCallback: AdvertiseCallback? = null
    private val serverCharacteristics = HashMap<String, BluetoothGattCharacteristic>()
    private val subscribedDevices = HashMap<String, MutableSet<BluetoothDevice>>()

    /// Android allows exactly one outstanding server notification at a time:
    /// `notifyCharacteristicChanged` only reports that a send was
    /// *initiated*, and the real status arrives on `onNotificationSent`.
    /// Issuing the next fragment before that lands makes the stack either
    /// reject it or transmit the characteristic's already-overwritten value,
    /// which silently corrupts a multi-fragment datagram. So notifications
    /// are queued and drained strictly one at a time.
    private class PendingNotify(
        val device: BluetoothDevice,
        val characteristicUuid: String,
        val value: ByteArray,
        val requestId: Long,
    )

    private val notifyQueue = ArrayDeque<PendingNotify>()
    private var notifyInFlight: PendingNotify? = null

    fun hasCentralSupport(): Boolean = adapter?.bluetoothLeScanner != null

    fun hasPeripheralSupport(): Boolean =
        adapter?.isMultipleAdvertisementSupported == true && adapter.bluetoothLeAdvertiser != null

    // ---------------------------------------------------------------
    // Central role
    // ---------------------------------------------------------------

    /// Returns false if a scan is already running. There is one
    /// `scanCallback` and one discovery sender, so a second concurrent scan
    /// would silently replace the first — and dropping either stream would
    /// then cancel the wrong one. Refusing is honest; per-scan ownership can
    /// come later if a caller genuinely needs concurrent scans.
    fun startScan(serviceUuid: String): Boolean {
        if (scanCallback != null) {
            Log.w(TAG, "startScan refused: a scan is already active")
            return false
        }
        val scanner = adapter?.bluetoothLeScanner ?: return false
        val target = UUID.fromString(serviceUuid)
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()
        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                val record = result.scanRecord
                val uuids = record?.serviceUuids
                Log.d(TAG, "onScanResult: ${result.device.address} name=${result.device.name} advertisedUuids=$uuids")
                if (uuids == null || !uuids.contains(ParcelUuid(target))) {
                    return
                }

                // Flatten both advertisement maps into parallel arrays — see
                // Native.kt for why the JNI boundary takes them this way.
                val manufacturer = record.manufacturerSpecificData
                val manufacturerIds = IntArray(manufacturer?.size() ?: 0)
                val manufacturerValues = arrayOfNulls<ByteArray>(manufacturer?.size() ?: 0)
                if (manufacturer != null) {
                    for (i in 0 until manufacturer.size()) {
                        manufacturerIds[i] = manufacturer.keyAt(i)
                        manufacturerValues[i] = manufacturer.valueAt(i) ?: ByteArray(0)
                    }
                }

                val serviceData = record.serviceData ?: emptyMap()
                val serviceDataUuids = arrayOfNulls<String>(serviceData.size)
                val serviceDataValues = arrayOfNulls<ByteArray>(serviceData.size)
                serviceData.entries.forEachIndexed { i, entry ->
                    serviceDataUuids[i] = entry.key.uuid.toString()
                    serviceDataValues[i] = entry.value ?: ByteArray(0)
                }

                @Suppress("UNCHECKED_CAST")
                onPeerDiscovered(
                    nativeHandle,
                    result.device.address,
                    result.device.name,
                    result.rssi,
                    manufacturerIds,
                    manufacturerValues as Array<ByteArray>,
                    serviceDataUuids as Array<String>,
                    serviceDataValues as Array<ByteArray>,
                )
            }

            override fun onScanFailed(errorCode: Int) {
                // Reported asynchronously, after Rust already has a
                // discovery stream. Without forwarding it, a scan that never
                // started is indistinguishable from one that found nothing.
                Log.e(TAG, "onScanFailed: errorCode=$errorCode")
                onScanFailed(nativeHandle, errorCode)
            }
        }
        Log.d(TAG, "startScan: serviceUuid=$serviceUuid scanner=${scanner != null}")
        scanCallback = callback
        // Matching against the raw advertised UUID list in onScanResult
        // above, rather than a native android.bluetooth.le.ScanFilter — a
        // filtered scan against a Root Canal virtual controller (Android
        // emulator's Bluetooth simulator) delivered zero results in testing
        // even when the target peer was confirmed advertising and the
        // filter parameters were accepted (status=0); an unfiltered scan
        // with matching done here works identically on real adapters and
        // sidesteps whatever that native-filter gap is. See docs/adr/0002.
        scanner.startScan(emptyList(), settings, callback)
        return true
    }

    fun stopScan() {
        // Clear ownership unconditionally. Returning early when the scanner
        // is gone (Bluetooth switched off before the scan stream was
        // dropped) left `scanCallback` set forever, and every later
        // `startScan` then rejected the scan as already active — scanning
        // stayed dead for the lifetime of this bridge even after Bluetooth
        // came back.
        val scanner = adapter?.bluetoothLeScanner
        val callback = scanCallback
        scanCallback = null
        if (scanner != null && callback != null) {
            try {
                scanner.stopScan(callback)
            } catch (e: Exception) {
                Log.w(TAG, "stopScan: error stopping scan", e)
            }
        }
    }

    fun connect(address: String) {
        val device = adapter?.getRemoteDevice(address)
        if (device == null) {
            // No adapter at all. Rust has already stored connected_tx, so
            // returning silently strands it forever — the same failure the
            // advertiser path had. Report the link as never established.
            Log.w(TAG, "connect: no Bluetooth adapter available for $address")
            onDisconnected(nativeHandle, address, false)
            return
        }
        val callback = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                when (newState) {
                    BluetoothProfile.STATE_CONNECTED -> {
                        // Ask for the largest MTU the spec allows before
                        // discovering services. The peer decides the real
                        // value and reports it via onMtuChanged; until then
                        // Rust keeps the conservative 23-byte default.
                        gatt.requestMtu(MAX_ATT_MTU)
                        if (!gatt.discoverServices()) {
                            // No callback is guaranteed when this returns
                            // false, so the pending connect would hang
                            // forever. Report the link as gone instead.
                            Log.w(TAG, "discoverServices did not start for $address")
                            onDisconnected(nativeHandle, address, false)
                            gatt.close()
                        }
                    }
                    BluetoothProfile.STATE_DISCONNECTED -> {
                        connectedGatts.remove(address)
                        pendingCharacteristics.remove(address)
                        // Fires for unsolicited drops too (out of range,
                        // peer powered off), which is the whole point of
                        // surfacing this to Rust rather than only reporting
                        // disconnects we initiated.
                        onDisconnected(nativeHandle, address, false)
                        gatt.close()
                    }
                }
            }

            override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    Log.d(TAG, "onMtuChanged: $address negotiated mtu=$mtu")
                    onMtuChanged(nativeHandle, address, mtu)
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    // Previously ignored, so a failed discovery still
                    // resolved connect() successfully and handed back a
                    // connection with no usable characteristics.
                    Log.w(TAG, "onServicesDiscovered failed for $address status=$status")
                    connectedGatts.remove(address)
                    pendingCharacteristics.remove(address)
                    onDisconnected(nativeHandle, address, false)
                    gatt.close()
                    return
                }
                val byUuid = HashMap<String, BluetoothGattCharacteristic>()
                for (service in gatt.services) {
                    for (characteristic in service.characteristics) {
                        byUuid[characteristic.uuid.toString()] = characteristic
                    }
                }
                pendingCharacteristics[address] = byUuid
                onConnected(nativeHandle, address, false)
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicRead(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int
            ) {
                onCharacteristicRead(
                    nativeHandle, address, characteristic.uuid.toString(),
                    characteristic.value ?: ByteArray(0), status == BluetoothGatt.GATT_SUCCESS
                )
            }

            override fun onCharacteristicWrite(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int
            ) {
                onCharacteristicWriteResult(
                    nativeHandle, address, characteristic.uuid.toString(), status == BluetoothGatt.GATT_SUCCESS
                )
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicChanged(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic
            ) {
                onCharacteristicChanged(
                    nativeHandle, address, characteristic.uuid.toString(), characteristic.value ?: ByteArray(0)
                )
            }

            override fun onDescriptorWrite(
                gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int
            ) {
                onSubscribed(
                    nativeHandle,
                    address,
                    descriptor.characteristic.uuid.toString(),
                    status == BluetoothGatt.GATT_SUCCESS,
                )
            }
        }
        val gatt = device.connectGatt(context, false, callback)
        connectedGatts[address] = gatt
    }

    fun disconnect(address: String) {
        connectedGatts[address]?.disconnect()
    }

    private fun findCharacteristic(address: String, characteristicUuid: String): BluetoothGattCharacteristic? =
        pendingCharacteristics[address]?.get(characteristicUuid)

    /// Every failure path must report back. An early `return` here leaves
    /// the Rust side's oneshot unresolved and its caller awaiting forever —
    /// and unknown UUIDs or incomplete service discovery are ordinary
    /// errors, not exotic ones.
    fun readCharacteristic(address: String, characteristicUuid: String) {
        val gatt = connectedGatts[address]
        val characteristic = findCharacteristic(address, characteristicUuid)
        if (gatt == null || characteristic == null || !gatt.readCharacteristic(characteristic)) {
            Log.w(TAG, "readCharacteristic could not start: $address/$characteristicUuid")
            onCharacteristicRead(nativeHandle, address, characteristicUuid, ByteArray(0), false)
        }
    }

    /// `withoutResponse` selects ATT Write Command over Write Request —
    /// materially faster for bulk transfer, at the cost of the peer silently
    /// dropping writes it can't keep up with. See `models::WriteType`.
    @Suppress("DEPRECATION")
    fun writeCharacteristic(
        address: String, characteristicUuid: String, value: ByteArray, withoutResponse: Boolean,
    ) {
        val gatt = connectedGatts[address]
        val characteristic = findCharacteristic(address, characteristicUuid)
        if (gatt == null || characteristic == null) {
            Log.w(TAG, "writeCharacteristic could not start: $address/$characteristicUuid")
            onCharacteristicWriteResult(nativeHandle, address, characteristicUuid, false)
            return
        }
        characteristic.writeType = if (withoutResponse) {
            BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
        } else {
            BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
        }
        characteristic.value = value
        if (!gatt.writeCharacteristic(characteristic)) {
            Log.w(TAG, "writeCharacteristic rejected by the stack: $address/$characteristicUuid")
            onCharacteristicWriteResult(nativeHandle, address, characteristicUuid, false)
        }
    }

    fun subscribeCharacteristic(address: String, characteristicUuid: String) {
        val gatt = connectedGatts[address]
        val characteristic = findCharacteristic(address, characteristicUuid)
        val cccd = characteristic?.getDescriptor(CLIENT_CHARACTERISTIC_CONFIG_UUID)
        if (gatt == null || characteristic == null || cccd == null) {
            Log.w(TAG, "subscribeCharacteristic could not start: $address/$characteristicUuid")
            onSubscribed(nativeHandle, address, characteristicUuid, false)
            return
        }
        if (!gatt.setCharacteristicNotification(characteristic, true)) {
            // Local delivery was never enabled, so onCharacteristicChanged
            // can never fire. Writing the remote CCCD anyway would let the
            // descriptor callback report success and hand Rust a live-looking
            // stream that stays silent forever.
            Log.w(TAG, "setCharacteristicNotification failed: $address/$characteristicUuid")
            onSubscribed(nativeHandle, address, characteristicUuid, false)
            return
        }
        @Suppress("DEPRECATION")
        cccd.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
        @Suppress("DEPRECATION")
        if (!gatt.writeDescriptor(cccd)) {
            Log.w(TAG, "subscribe descriptor write rejected: $address/$characteristicUuid")
            onSubscribed(nativeHandle, address, characteristicUuid, false)
        }
    }

    // ---------------------------------------------------------------
    // Peripheral role
    // ---------------------------------------------------------------

    fun startAdvertising(
        serviceUuid: String, characteristicUuids: Array<String>, readable: BooleanArray, writable: BooleanArray,
        notifiable: BooleanArray, initialValues: Array<ByteArray>,
    ) {
        val advertiser = adapter?.bluetoothLeAdvertiser
        if (advertiser == null) {
            // Central-only device, or Bluetooth switched off. Returning
            // silently left Rust's oneshot pending forever, which is worse
            // than the unsupported error the backend contract asks for.
            Log.w(TAG, "startAdvertising: no BLE advertiser available")
            failAdvertise(ADVERTISE_ERROR_UNAVAILABLE)
            return
        }

        val serverCallback = object : BluetoothGattServerCallback() {
            /// `addService` completes asynchronously. Advertising used to
            /// start immediately after the call, so a delayed or rejected
            /// registration still resolved Rust's `advertise()` as success
            /// while centrals could see the advertisement but not the
            /// service it promised.
            override fun onServiceAdded(status: Int, service: BluetoothGattService) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    Log.e(TAG, "onServiceAdded failed: status=$status")
                    failAdvertise(status)
                    return
                }
                Log.d(TAG, "onServiceAdded ok, starting advertisement")
                beginAdvertising(advertiser, serviceUuid)
            }

            /// Real server-side lifecycle. Without this the peripheral role
            /// had no genuine connect/disconnect signal at all — it was
            /// synthesized from write traffic, so a central that connected
            /// and then left was never reported as gone.
            override fun onConnectionStateChange(device: BluetoothDevice, status: Int, newState: Int) {
                when (newState) {
                    BluetoothProfile.STATE_CONNECTED -> {
                        Log.d(TAG, "server: central connected ${device.address}")
                        onConnected(nativeHandle, device.address, true)
                    }
                    BluetoothProfile.STATE_DISCONNECTED -> {
                        Log.d(TAG, "server: central disconnected ${device.address}")
                        for (subscribers in subscribedDevices.values) {
                            subscribers.remove(device)
                        }
                        onDisconnected(nativeHandle, device.address, true)
                    }
                }
            }

            override fun onCharacteristicReadRequest(
                device: BluetoothDevice, requestId: Int, offset: Int, characteristic: BluetoothGattCharacteristic
            ) {
                val value = characteristic.value ?: ByteArray(0)
                gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicWriteRequest(
                device: BluetoothDevice, requestId: Int, characteristic: BluetoothGattCharacteristic,
                preparedWrite: Boolean, responseNeeded: Boolean, offset: Int, value: ByteArray,
            ) {
                characteristic.value = value
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
                }
                onServerCharacteristicWritten(nativeHandle, device.address, characteristic.uuid.toString(), value)
            }

            override fun onNotificationSent(device: BluetoothDevice, status: Int) {
                val completed = synchronized(this@BleGattBridge) {
                    val done = notifyInFlight
                    notifyInFlight = null
                    done
                }
                if (completed != null) {
                    onNotifySent(
                        nativeHandle,
                        completed.requestId,
                        status == BluetoothGatt.GATT_SUCCESS,
                    )
                }
                pumpNotifications()
            }

            override fun onDescriptorWriteRequest(
                device: BluetoothDevice, requestId: Int, descriptor: BluetoothGattDescriptor,
                preparedWrite: Boolean, responseNeeded: Boolean, offset: Int, value: ByteArray,
            ) {
                val characteristicUuid = descriptor.characteristic.uuid.toString()
                if (value.contentEquals(BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE)) {
                    val added = subscribedDevices
                        .getOrPut(characteristicUuid) { mutableSetOf() }
                        .add(device)
                    // Report only the transition. This is what tells Rust the
                    // peer is reachable by notify — announcing at connection
                    // time was too early, since a greeting sent before the
                    // CCCD write has nowhere to go.
                    if (added) {
                        onServerSubscribed(nativeHandle, device.address)
                    }
                } else {
                    subscribedDevices[characteristicUuid]?.remove(device)
                }
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
                }
            }
        }
        val server = bluetoothManager.openGattServer(context, serverCallback)
        gattServer = server

        val service = BluetoothGattService(
            UUID.fromString(serviceUuid), BluetoothGattService.SERVICE_TYPE_PRIMARY
        )
        for (i in characteristicUuids.indices) {
            var properties = 0
            var permissions = 0
            if (readable[i]) {
                properties = properties or BluetoothGattCharacteristic.PROPERTY_READ
                permissions = permissions or BluetoothGattCharacteristic.PERMISSION_READ
            }
            if (writable[i]) {
                // Both modes, matching linux.rs (`write` and
                // `write_without_response`). Advertising only PROPERTY_WRITE
                // let a peer using WriteType::WithoutResponse discover no
                // such capability and reject the write command locally,
                // before it ever reached onCharacteristicWriteRequest.
                properties = properties or
                    BluetoothGattCharacteristic.PROPERTY_WRITE or
                    BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE
                permissions = permissions or BluetoothGattCharacteristic.PERMISSION_WRITE
            }
            if (notifiable[i]) {
                properties = properties or BluetoothGattCharacteristic.PROPERTY_NOTIFY
            }
            val characteristic = BluetoothGattCharacteristic(
                UUID.fromString(characteristicUuids[i]), properties, permissions
            )
            @Suppress("DEPRECATION")
            characteristic.value = initialValues[i]
            if (notifiable[i]) {
                val cccd = BluetoothGattDescriptor(
                    CLIENT_CHARACTERISTIC_CONFIG_UUID,
                    BluetoothGattDescriptor.PERMISSION_READ or BluetoothGattDescriptor.PERMISSION_WRITE
                )
                characteristic.addDescriptor(cccd)
            }
            service.addCharacteristic(characteristic)
            serverCharacteristics[characteristicUuids[i]] = characteristic
        }
        if (!server.addService(service)) {
            Log.e(TAG, "addService was rejected outright for $serviceUuid")
            failAdvertise(ADVERTISE_ERROR_SERVICE_REJECTED)
            return
        }
        // Advertising now starts from onServiceAdded, once registration has
        // actually succeeded.
    }

    private fun beginAdvertising(advertiser: android.bluetooth.le.BluetoothLeAdvertiser, serviceUuid: String) {
        val settings = AdvertiseSettings.Builder()
            .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_BALANCED)
            .setConnectable(true)
            .build()
        val data = AdvertiseData.Builder()
            .addServiceUuid(ParcelUuid(UUID.fromString(serviceUuid)))
            .setIncludeDeviceName(false)
            .build()
        val callback = object : AdvertiseCallback() {
            override fun onStartSuccess(settingsInEffect: AdvertiseSettings) {
                Log.d(TAG, "advertise onStartSuccess: serviceUuid=$serviceUuid")
                onAdvertiseResult(nativeHandle, true, 0)
            }

            override fun onStartFailure(errorCode: Int) {
                // Android reports this asynchronously — advertising already
                // in progress, controller out of resources. Reporting it
                // back is what stops Rust claiming the service is reachable
                // when no advertisement exists.
                Log.e(TAG, "advertise onStartFailure: errorCode=$errorCode serviceUuid=$serviceUuid")
                failAdvertise(errorCode)
            }
        }
        advertiseCallback = callback
        Log.d(TAG, "startAdvertising: serviceUuid=$serviceUuid advertiser=${advertiser != null}")
        advertiser.startAdvertising(settings, data, callback)
    }

    /// Close every open GATT and the server, so no Kotlin object is left
    /// holding a callback that captures `nativeHandle`. Rust calls this
    /// before the last `Arc<Inner>` can be freed; without it a later
    /// link-state callback would reconstruct an `Arc` from freed memory.
    fun closeAll() {
        for (gatt in connectedGatts.values) {
            try {
                gatt.disconnect()
                gatt.close()
            } catch (e: Exception) {
                Log.w(TAG, "closeAll: error closing gatt", e)
            }
        }
        connectedGatts.clear()
        pendingCharacteristics.clear()
        stopScan()
        stopAdvertising()
    }

    fun stopAdvertising() {
        val advertiser = adapter?.bluetoothLeAdvertiser
        advertiseCallback?.let { advertiser?.stopAdvertising(it) }
        advertiseCallback = null

        // Report every served central as gone *before* dropping the server.
        // Closing a GATT server delivers no onConnectionStateChange for the
        // centrals attached to it, so without this Rust keeps them in its
        // single-central map with their channels live — and a later
        // advertise has the stale generation disconnecting the new server's
        // central as an interloper.
        val served = subscribedDevices.values.flatten().map { it.address }.distinct()
        subscribedDevices.clear()
        for (address in served) {
            onDisconnected(nativeHandle, address, true)
        }

        failPendingNotifications()

        gattServer?.close()
        gattServer = null
        serverCharacteristics.clear()
    }

    /// Report an advertise failure *and* release everything the attempt
    /// opened.
    ///
    /// Reporting alone left `gattServer` and its registered service live. A
    /// retry then overwrote the sole `gattServer` reference, so the first
    /// server could never be closed by `closeAll()` — leaking the Bluetooth
    /// resource and, worse, leaving its callback holding `nativeHandle`
    /// after Rust had freed that state.
    private fun failAdvertise(errorCode: Int) {
        stopAdvertising()
        onAdvertiseResult(nativeHandle, false, errorCode)
    }

    /// Addresses currently subscribed to a characteristic. Rust expresses a
    /// broadcast as one addressed send per subscriber, so every payload goes
    /// through the same completion-confirmed queue.
    fun subscribedAddresses(characteristicUuid: String): Array<String> =
        subscribedDevices[characteristicUuid]
            ?.map { it.address }
            ?.distinct()
            ?.toTypedArray()
            ?: emptyArray()

    /// Drain the notification queue, one outstanding send at a time.
    ///
    /// Failures are reported to Rust here rather than swallowed: a caller
    /// that believes a fragment was sent when it was not will wait for a
    /// reply that can never come.
    private fun pumpNotifications() {
        while (true) {
            val next = synchronized(this) {
                if (notifyInFlight != null) return
                val candidate = notifyQueue.removeFirstOrNull() ?: return
                notifyInFlight = candidate
                candidate
            }
            val server = gattServer
            val characteristic = serverCharacteristics[next.characteristicUuid]
            val initiated = if (server != null && characteristic != null) {
                @Suppress("DEPRECATION")
                characteristic.value = next.value
                @Suppress("DEPRECATION")
                server.notifyCharacteristicChanged(next.device, characteristic, false)
            } else {
                false
            }
            if (initiated) {
                // onNotificationSent resumes the pump.
                return
            }
            synchronized(this) { notifyInFlight = null }
            onNotifySent(nativeHandle, next.requestId, false)
        }
    }

    /// Queue a notification for exactly one subscribed central, resolved
    /// asynchronously through `onNotifySent(requestId, ...)`.
    ///
    /// Returns false only for a synchronous impossibility (no server, or the
    /// peer is not subscribed); anything queued is answered by the callback.
    fun notifyCharacteristicTo(
        address: String, characteristicUuid: String, value: ByteArray, requestId: Long,
    ): Boolean {
        if (gattServer == null || serverCharacteristics[characteristicUuid] == null) {
            return false
        }
        val device = subscribedDevices[characteristicUuid]
            ?.firstOrNull { it.address == address }
            ?: return false
        synchronized(this) {
            notifyQueue.addLast(PendingNotify(device, characteristicUuid, value, requestId))
        }
        pumpNotifications()
        return true
    }

    /// Fail every queued and in-flight notification. Called when the server
    /// goes away, so Rust callers are not left waiting on sends that can no
    /// longer complete.
    private fun failPendingNotifications() {
        val abandoned = synchronized(this) {
            val all = notifyQueue.toList() + listOfNotNull(notifyInFlight)
            notifyQueue.clear()
            notifyInFlight = null
            all
        }
        for (pending in abandoned) {
            onNotifySent(nativeHandle, pending.requestId, false)
        }
    }


    /// Drop a central's connection to our GATT server, and forget its
    /// subscriptions so `notifyCharacteristic` stops broadcasting to it even
    /// before `onConnectionStateChange` lands.
    fun disconnectServerPeer(address: String) {
        val server = gattServer ?: return
        val device = subscribedDevices.values
            .asSequence()
            .flatten()
            .firstOrNull { it.address == address }
            ?: adapter?.getRemoteDevice(address)
            ?: return
        for (peers in subscribedDevices.values) {
            peers.removeAll { it.address == address }
        }
        server.cancelConnection(device)
    }

    companion object {
        private val CLIENT_CHARACTERISTIC_CONFIG_UUID: UUID =
            UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

        /// Largest ATT MTU the Bluetooth spec permits. Requested on every
        /// connection; the peer negotiates it down to whatever it supports.
        private const val MAX_ATT_MTU: Int = 517

        /// Locally-assigned advertise error codes, chosen above the range
        /// Android's AdvertiseCallback uses (1..5) so they cannot be
        /// confused with a real controller error.
        private const val ADVERTISE_ERROR_UNAVAILABLE: Int = 100
        private const val ADVERTISE_ERROR_SERVICE_REJECTED: Int = 101
    }
}
