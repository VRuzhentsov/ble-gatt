package dev.blegatt

/** Commit retry metadata only after the matching native start succeeds. Caller holds the bridge monitor. */
internal class AdvertisingResume {
    private var committed: (() -> Unit)? = null
    private var pending: (() -> Unit)? = null

    fun begin(resume: () -> Unit) {
        pending = resume
    }

    fun confirm() {
        pending?.let { committed = it }
        pending = null
    }

    fun cancel() { pending = null }
    fun replace(resume: () -> Unit) { committed = resume; pending = null }
    fun clear() { committed = null; pending = null }
    fun current(): (() -> Unit)? = committed
}
