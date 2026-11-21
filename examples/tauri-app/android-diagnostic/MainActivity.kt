package dev.blegatt.example

import android.content.Intent
import android.os.Bundle
import android.util.Log
import androidx.activity.enableEdgeToEdge
import java.io.File

class MainActivity : TauriActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        if (BuildConfig.DEBUG) {
            File(filesDir, "either-peer-foreground").delete()
            File(filesDir, "either-peer-stop").delete()
        }
        super.onCreate(savedInstanceState)
        if (BuildConfig.DEBUG && diagnosticMode().isNotEmpty()) {
            try {
                startForegroundService(Intent(this, DiagnosticService::class.java))
            } catch (error: Exception) {
                Log.e("EitherPeerService", "FAIL foreground service; approve existing Bluetooth permissions in foreground, then relaunch", error)
            }
        }
    }

    private fun diagnosticMode(): String = ProcessBuilder("getprop", "debug.blegatt.iroh.mode")
        .start().inputStream.bufferedReader().use { it.readText().trim() }
}
