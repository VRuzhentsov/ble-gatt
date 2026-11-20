package dev.blegatt

/**
 * Where the parts of one legacy advertisement go (ADR-0012 §2). Plain Kotlin, no Android
 * types, so it is testable off a device.
 *
 * The advertising packet holds the service UUID, the manufacturer data and the service data
 * under other UUIDs. The scan response holds one thing: the fast advertisement, which then
 * replaces the manufacturer data under its company (a scanner restores that entry from it),
 * or else the service data under the served UUID that Rust chose (the managed advertisement
 * header, or the name). Rust's `GattServiceSpec::legacy_placement` is the same rule.
 */
internal class AdvertisePlacement(
    val manufacturerIds: IntArray,
    val manufacturerValues: Array<ByteArray>,
    val serviceDataUuids: Array<String>,
    val serviceDataValues: Array<ByteArray>,
    /** The fast advertisement's company in the scan response, or [NO_COMPANY]. */
    val scanResponseCompanyId: Int,
    val scanResponseManufacturerData: ByteArray?,
    /** Service data under the served UUID in the scan response, when there is no fast record. */
    val scanResponseServiceData: ByteArray?,
) {
    val fastPlaced: Boolean get() = scanResponseManufacturerData != null

    /** Flags 3, the 128-bit UUID list 18, each manufacturer AD 4 + n, each 128-bit service data AD 18 + n. */
    fun advertisingLength(): Int =
        3 + 18 + manufacturerValues.sumOf { 4 + it.size } + serviceDataValues.sumOf { 18 + it.size }

    fun scanResponseLength(): Int {
        val manufacturer = scanResponseManufacturerData
        val service = scanResponseServiceData
        return when {
            manufacturer != null -> 4 + manufacturer.size
            service != null -> 18 + service.size
            else -> 0
        }
    }

    companion object {
        /** `FAST_ADVERTISEMENT_MAX_LEN` in Rust: 31 less the AD header and the company. */
        const val FAST_ADVERTISEMENT_MAX_LEN: Int = 27
        const val NO_COMPANY: Int = -1

        /** `withFast = false` is the placement after Android refused the fast one. */
        fun place(
            manufacturerIds: IntArray, manufacturerValues: Array<ByteArray>,
            serviceDataUuids: Array<String>, serviceDataValues: Array<ByteArray>,
            scanResponseServiceData: ByteArray?,
            fastCompanyId: Int, fastData: ByteArray?,
            withFast: Boolean,
        ): AdvertisePlacement {
            val fast = if (withFast && fastCompanyId != NO_COMPANY && fastData != null &&
                fastData.size <= FAST_ADVERTISEMENT_MAX_LEN) fastData else null
            if (fast == null) {
                return AdvertisePlacement(
                    manufacturerIds, manufacturerValues, serviceDataUuids, serviceDataValues,
                    NO_COMPANY, null, scanResponseServiceData?.takeIf { it.isNotEmpty() },
                )
            }
            val kept = manufacturerIds.indices.filter { manufacturerIds[it] != fastCompanyId }
            return AdvertisePlacement(
                IntArray(kept.size) { manufacturerIds[kept[it]] },
                Array(kept.size) { manufacturerValues[kept[it]] },
                serviceDataUuids, serviceDataValues,
                fastCompanyId, fast, null,
            )
        }
    }
}
