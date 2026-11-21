package dev.blegatt

fun main() {
    val state = AdvertisingResume()
    var radio = "hidden"
    state.begin { radio = "A" }
    state.confirm()
    state.begin { radio = "B" }
    state.cancel() // Failed B, or a cancelled start superseded before its callback.
    state.current()!!()
    check(radio == "A") { "failed B must resume committed A, got $radio" }
    state.begin { radio = "B" }
    state.confirm() // Native success after Rust cancelled its outcome receiver.
    state.current()!!()
    check(radio == "B")
    state.replace { radio = "C" } // Hidden metadata update commits synchronously.
    state.current()!!()
    check(radio == "C")
    state.clear()
    check(state.current() == null)
    println("AdvertisingResume: failed update rollback, cancelled waiter success, hidden update, clear passed")
}
