package dev.blegatt.example

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.Process
import android.util.Log
import java.io.File

/** Debug example process lifetime only: no wake lock, radio toggle or location request. */
class DiagnosticService : Service() {
    private val handler = Handler(Looper.getMainLooper())
    private var ticks = 0
    private val poll = object : Runnable {
        override fun run() {
            ticks++
            if (ticks >= 190 || File(filesDir, "either-peer-stop").exists()) {
                stopSelf()
            } else handler.postDelayed(this, 1000)
        }
    }
    override fun onCreate() {
        super.onCreate()
        if (!BuildConfig.DEBUG) { stopSelf(); return }
        try {
            val manager = getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(NotificationChannel("either-peer", "BLE diagnostic", NotificationManager.IMPORTANCE_LOW))
            val notification = Notification.Builder(this, "either-peer")
                .setContentTitle("Isolated BLE diagnostic")
                .setContentText("Synthetic traffic only; stops within 190 seconds")
                .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
                .setOngoing(true).build()
            startForeground(1, notification)
            File(filesDir, "either-peer-foreground").writeText(Process.myPid().toString())
            Log.i("EitherPeerService", "READY foreground pid=${Process.myPid()} timeout_seconds=190")
            handler.postDelayed(poll, 1000)
        } catch (error: Exception) {
            Log.e("EitherPeerService", "FAIL foreground service prerequisites", error)
            stopSelf()
        }
    }
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int = START_NOT_STICKY
    override fun onBind(intent: Intent?): IBinder? = null
    override fun onDestroy() {
        handler.removeCallbacks(poll)
        File(filesDir, "either-peer-foreground").delete()
        File(filesDir, "either-peer-stop").delete()
        stopForeground(STOP_FOREGROUND_REMOVE)
        Log.i("EitherPeerService", "STOP foreground pid=${Process.myPid()}")
        super.onDestroy()
    }
}
