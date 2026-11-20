package dev.blegatt

// Run like AdvertisingResumeTest: compile with AdvertisePlacement.kt and run
// `dev.blegatt.AdvertisePlacementTest` (an object, so its main does not clash with that file's).
object AdvertisePlacementTest {
@JvmStatic
fun main(args: Array<String>) {
    val fini = byteArrayOf(0x01, 0xAA.toByte(), 0xBB.toByte(), 0xCC.toByte(), 0xDD.toByte())
    val header = ByteArray(9) { 0xF8.toByte() }
    val fast = ByteArray(26) { 0xFC.toByte() }

    // Fini on Android: the fast record replaces 0xFFFF and takes the scan response.
    val placed = AdvertisePlacement.place(
        intArrayOf(0xFFFF), arrayOf(fini), emptyArray(), emptyArray(),
        header, 0xFFFF, fast, withFast = true,
    )
    check(placed.fastPlaced)
    check(placed.manufacturerIds.isEmpty()) { "0xFFFF folded into the record" }
    check(placed.scanResponseCompanyId == 0xFFFF)
    check(placed.scanResponseServiceData == null) { "the header is superseded" }
    check(placed.advertisingLength() == 21) { "advertising ${placed.advertisingLength()}" }
    check(placed.scanResponseLength() == 30) { "scan response ${placed.scanResponseLength()}" }

    // After ADVERTISE_FAILED_DATA_TOO_LARGE: the caller's data and the header, as before.
    val fallback = AdvertisePlacement.place(
        intArrayOf(0xFFFF), arrayOf(fini), emptyArray(), emptyArray(),
        header, 0xFFFF, fast, withFast = false,
    )
    check(!fallback.fastPlaced)
    check(fallback.manufacturerIds.contentEquals(intArrayOf(0xFFFF)))
    check(fallback.scanResponseServiceData!!.contentEquals(header))
    check(fallback.advertisingLength() == 30)
    check(fallback.scanResponseLength() == 27)

    // Other companies stay in the advertising packet.
    val other = AdvertisePlacement.place(
        intArrayOf(0x0001, 0xFFFF), arrayOf(byteArrayOf(9), fini), emptyArray(), emptyArray(),
        null, 0xFFFF, fast, withFast = true,
    )
    check(other.manufacturerIds.contentEquals(intArrayOf(0x0001)))
    check(other.manufacturerValues[0].contentEquals(byteArrayOf(9)))

    // No fast record (a raw spec): the name travels as service data, as in ABI 7.
    val named = AdvertisePlacement.place(
        IntArray(0), emptyArray(), emptyArray(), emptyArray(),
        "Pixel".toByteArray(), AdvertisePlacement.NO_COMPANY, null, withFast = true,
    )
    check(!named.fastPlaced)
    check(named.scanResponseLength() == 18 + 5)

    // Too long for one AD structure: not placed.
    val oversized = AdvertisePlacement.place(
        IntArray(0), emptyArray(), emptyArray(), emptyArray(),
        null, 0xFFFF, ByteArray(28), withFast = true,
    )
    check(!oversized.fastPlaced)
    check(oversized.scanResponseLength() == 0)
    println("AdvertisePlacement: fast fold, too-large fallback, other companies, name, oversize passed")
}
}
