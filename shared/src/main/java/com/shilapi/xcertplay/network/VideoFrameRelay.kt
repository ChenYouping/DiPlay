package com.shilapi.xcertplay.network

import android.util.Log
import com.shilapi.xcertplay.airplay.VideoCodec
import com.shilapi.xcertplay.media.MediaCodecSupport
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * One-way mirror of the main CarPlay screen to a secondary client over the NCM link.
 *
 * Design (simplified 2026-11-19): the server is a PURE FORWARDER. It taps the
 * already-decrypted Annex-B H.264/H.265 frames at [relayConfig]/[relayFrame] and
 * writes them, unchanged and in arrival order, to the single TCP client on port
 * 7101. There is NO GOP cache, NO keyframe detection, NO sequence numbering and
 * NO replay queue: all of those lived here previously and were the root cause of
 * a long chain of seam bugs (-12909, "first frame only", "freeze on live switch").
 *
 * Decoder state now belongs to the iOS client: it waits for the next IDR before
 * decoding and freezes on its last frame while no IDR is available. When a client
 * connects we make ONE best-effort request to the iPhone for a fresh keyframe
 * (via the injected [keyFrameRequester], which reaches AirPlay's forceKeyFrame
 * command). This is fire-and-forget — a static screen may not produce an IDR, in
 * which case the client simply keeps its last frame, which is correct.
 *
 * Wire format (client connects, server pushes continuously; client never sends):
 *
 *   magic  = 4 bytes ASCII "NPVS"
 *   kind   = 1 byte: 1 = config (codec + codecData), 2 = video frame
 *   codec  = 1 byte: 0 = H264, 1 = H265 (meaningful for config; 0 for frame)
 *   length = 4 bytes signed big-endian int
 *   body   = [length] bytes (avcC/hvcC for config; Annex-B NALUs for frame)
 *
 * Observability (all via [logLine] -> logcat + exportable diagnostic report):
 *   NPVS-EVT   state transitions (listen/client/config/keyframeReq/reset)
 *   NPVS-STAT  a 5-second heartbeat with counters on every hop, even when idle
 *   NPVS-WARN  silent drops, swallowed exceptions, slow/failed socket writes
 */
object VideoFrameRelay {
    private const val TAG = "xcertplay-npvs"
    const val DEFAULT_PORT = 7101
    private const val STAT_WINDOW_MS = 5_000L
    private const val SLOW_PUSH_MS = 50L

    private data class Config(val codecByte: Byte, val data: ByteArray)
    private val configRef = AtomicReference<Config?>(null)

    interface Sink {
        fun onConfig(codecByte: Byte, data: ByteArray)
        fun onFrame(frame: ByteArray)
    }

    private val sinks = CopyOnWriteArrayList<Sink>()

    /**
     * Injected by AndroidMediaSink: asks the iPhone (over AirPlay forceKeyFrame)
     * to emit a fresh IDR. Called ONCE when a mirror client connects. Never throws
     * into the hot path — all call sites wrap it in runCatching.
     */
    @Volatile private var keyFrameRequester: (() -> Unit)? = null
    fun installKeyFrameRequester(requester: () -> Unit) { keyFrameRequester = requester }

    @Volatile private var logSink: (String) -> Unit = {}
    private fun logLine(message: String) {
        Log.i(TAG, message)
        runCatching { logSink(message) }
    }

    // 5-second windowed stats. Emitted even with no frames so a silent upstream
    // (in=0) is distinguishable from a healthy idle stream.
    private class Stats {
        private val lock = Any()
        private var startMs = android.os.SystemClock.elapsedRealtime()
        private var inFrames = 0L
        private var inBytes = 0L
        private var keyframes = 0L
        private var pushedFrames = 0L
        private var pushedBytes = 0L
        private var dropNoClient = 0L
        private var dropClosed = 0L
        private var dropEmpty = 0L
        private var pushFail = 0L
        private var pushSlow = 0L
        private var sinkErrors = 0L
        private var keyReqs = 0L
        @Volatile var clientUpMs = 0L

        fun onIn(bytes: Int, isKey: Boolean) { synchronized(lock) {
            inFrames++; inBytes += bytes; if (isKey) keyframes++
        } }
        fun onEmpty() { synchronized(lock) { dropEmpty++ } }
        fun onNoClient(bytes: Int) { synchronized(lock) { dropNoClient++; inBytes += bytes } }
        fun onDropClosed() { synchronized(lock) { dropClosed++ } }
        fun onPush(bytes: Int, slow: Boolean) { synchronized(lock) {
            pushedFrames++; pushedBytes += bytes; if (slow) pushSlow++
        } }
        fun onPushFail(bytes: Int) { synchronized(lock) { pushFail++; pushedBytes += bytes } }
        fun onSinkError() { synchronized(lock) { sinkErrors++ } }
        fun onKeyRequest() { synchronized(lock) { keyReqs++ } }

        fun flush(): String = synchronized(lock) {
            val now = android.os.SystemClock.elapsedRealtime()
            val span = now - startMs
            if (span < STAT_WINDOW_MS) return ""
            val secs = span / 1000.0
            val line = String.format(
                java.util.Locale.US,
                "NPVS-STAT win=%.1fs in=%df/%dkB key=%d push=%df/%dkB " +
                    "noCli=%d closed=%d empty=%d pushFail=%d slow=%d sinkErr=%d " +
                    "keyReq=%d cliUp=%ds",
                secs, inFrames, inBytes / 1024, keyframes,
                pushedFrames, pushedBytes / 1024,
                dropNoClient, dropClosed, dropEmpty, pushFail, pushSlow, sinkErrors,
                keyReqs, clientUpMs / 1000
            )
            inFrames = 0; inBytes = 0; keyframes = 0
            pushedFrames = 0; pushedBytes = 0
            dropNoClient = 0; dropClosed = 0; dropEmpty = 0
            pushFail = 0; pushSlow = 0; sinkErrors = 0; keyReqs = 0
            startMs = now
            line
        }
    }

    private val stats = Stats()

    fun installLogSink(sink: (String) -> Unit) { logSink = sink }

    /** 0 = H264, 1 = H265. Called from the media thread. */
    fun relayConfig(codecName: String, codecData: ByteArray) {
        val codecByte: Byte = if (codecName.equals("H265", ignoreCase = true)) 1 else 0
        val cfg = Config(codecByte, codecData.copyOf())
        configRef.set(cfg)
        logLine("NPVS-EVT config codec=$codecByte bytes=${codecData.size}")
        sinks.forEach { s ->
            runCatching { s.onConfig(cfg.codecByte, cfg.data.copyOf()) }
                .onFailure { stats.onSinkError(); logLine("NPVS-WARN sinkErr where=config err=${it.javaClass.simpleName}:${it.message}") }
        }
    }

    /** Frames are already Annex-B; copy once only when someone is listening. */
    fun relayFrame(frame: ByteArray) {
        val codec = if (configRef.get()?.codecByte == 1.toByte()) VideoCodec.H265 else VideoCodec.H264
        val isKey = runCatching { MediaCodecSupport.isRandomAccess(frame, codec) }.getOrDefault(false)
        if (frame.isEmpty()) {
            stats.onEmpty()
            logLine("NPVS-WARN emptyFrame from=media")
            return
        }
        val copy = frame.copyOf()
        stats.onIn(copy.size, isKey)
        if (sinks.isEmpty()) { stats.onNoClient(copy.size); return }
        sinks.forEach { s ->
            runCatching { s.onFrame(copy.copyOf()) }
                .onFailure { stats.onSinkError(); logLine("NPVS-WARN sinkErr where=frame err=${it.javaClass.simpleName}:${it.message}") }
        }
    }

    fun subscribe(sink: Sink) {
        sinks.addIfAbsent(sink)
        configRef.get()?.let { cfg -> runCatching { sink.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
        logLine("NPVS-EVT subscribe sinks=${sinks.size}")
    }

    fun unsubscribe(sink: Sink) { sinks.remove(sink) }

    /** Ask the iPhone for one fresh IDR. Best-effort, throttled upstream. */
    private fun requestKeyFrame(reason: String) {
        val requester = keyFrameRequester ?: return
        stats.onKeyRequest()
        val ok = runCatching { requester.invoke() }.isSuccess
        logLine("NPVS-EVT keyframeReq reason=$reason sent=$ok")
    }

    fun reset() {
        configRef.set(null)
        sinks.clear()
        keyFrameRequester = null
        logLine("NPVS-EVT reset")
    }

    /** The push server; one client at a time so frame pacing stays deterministic. */
    class Server(
        private val bindAddress: InetAddress,
        private val port: Int = DEFAULT_PORT,
        private val onLog: (String) -> Unit = {},
    ) : AutoCloseable, Sink {
        private val running = AtomicBoolean(false)
        private var server: ServerSocket? = null
        private var worker: Thread? = null
        private var statWorker: Thread? = null
        @Volatile private var clientOut: DataOutputStream? = null
        @Volatile private var clientSocket: Socket? = null
        @Volatile private var clientSinceMs = 0L
        @Volatile private var clientAddr = ""

        fun start() {
            if (!running.compareAndSet(false, true)) return
            installLogSink { message -> log(message) }
            worker = Thread({ acceptLoop() }, "npvs-push").apply { isDaemon = true; start() }
            statWorker = Thread({
                while (running.get()) {
                    try { Thread.sleep(1000) } catch (_: InterruptedException) { break }
                    val line = stats.flush()
                    if (line.isNotEmpty()) log(line)
                }
            }, "npvs-stats").apply { isDaemon = true; start() }
        }

        override fun onConfig(codecByte: Byte, data: ByteArray) {
            push(kind = 1, codec = codecByte.toInt(), body = data)
        }

        override fun onFrame(frame: ByteArray) { push(kind = 2, codec = 0, body = frame) }

        private fun push(kind: Int, codec: Int, body: ByteArray) {
            val out = clientOut
            val socket = clientSocket
            if (out == null || socket == null) { return }
            if (socket.isClosed) { stats.onDropClosed(); return }
            val t0 = android.os.SystemClock.elapsedRealtime()
            try {
                synchronized(out) {
                    out.write(MAGIC)
                    out.writeByte(kind)
                    out.writeByte(codec)
                    out.writeInt(body.size)
                    out.write(body)
                    out.flush()
                }
                val ms = android.os.SystemClock.elapsedRealtime() - t0
                if (ms >= SLOW_PUSH_MS) {
                    logLine("NPVS-WARN pushSlow ms=$ms kind=$kind bytes=${body.size} client=$clientAddr")
                }
                stats.onPush(body.size, ms >= SLOW_PUSH_MS)
            } catch (e: IOException) {
                stats.onPushFail(body.size)
                logLine("NPVS-WARN pushFail kind=$kind bytes=${body.size} err=${e.javaClass.simpleName}:${e.message}")
                closeClient("pushFail")
            } catch (e: Throwable) {
                stats.onPushFail(body.size)
                logLine("NPVS-WARN pushFail kind=$kind bytes=${body.size} err=${e.javaClass.simpleName}:${e.message}")
                closeClient("pushCrash")
            }
        }

        private fun acceptLoop() {
            try {
                val srv = ServerSocket()
                srv.reuseAddress = true
                srv.bind(InetSocketAddress(bindAddress, port))
                server = srv
                log("NPVS-EVT listen addr=${srv.localSocketAddress}:${srv.localPort}")
                subscribe(this)
                while (running.get()) {
                    val socket = try {
                        srv.accept()
                    } catch (e: IOException) {
                        if (running.get()) log("NPVS-WARN acceptErr err=${e.message}")
                        break
                    }
                    closeClient("newClient")
                    socket.tcpNoDelay = true
                    clientSocket = socket
                    clientOut = DataOutputStream(socket.getOutputStream())
                    clientAddr = socket.remoteSocketAddress?.toString() ?: "?"
                    clientSinceMs = android.os.SystemClock.elapsedRealtime()
                    stats.clientUpMs = 0L
                    log("NPVS-EVT client+ addr=$clientAddr")

                    // Send current config so the client can build a decoder, then
                    // ask the iPhone for one fresh IDR so decoding can start quickly.
                    // No cache/replay: the client waits for the IDR itself.
                    configRef.get()?.let { cfg ->
                        push(kind = 1, codec = cfg.codecByte.toInt(), body = cfg.data.copyOf())
                    }
                    requestKeyFrame("clientConnect")

                    // Watchdog: this socket is push-only, so blocking on read()
                    // detects a peer close without mistaking a healthy quiet stream
                    // for a disconnect (no soTimeout — see history).
                    val watchdog = Thread({
                        var reason = "unknown"
                        try {
                            val buf = ByteArray(16)
                            val input = try { socket.getInputStream() } catch (_: IOException) { null }
                            if (input == null) reason = "noInput"
                            while (running.get() && !socket.isClosed && input != null) {
                                try {
                                    if (input.read(buf) < 0) { reason = "eof"; break }
                                } catch (_: IOException) { reason = "ioErr"; break }
                            }
                        } finally {
                            if (clientSocket === socket) {
                                val up = android.os.SystemClock.elapsedRealtime() - clientSinceMs
                                stats.clientUpMs = up
                                closeClient("watchdog:$reason")
                                log("NPVS-EVT client- reason=$reason up=${up / 1000}s")
                            }
                        }
                    }, "npvs-watchdog").apply { isDaemon = true }
                    watchdog.start()
                }
            } catch (e: Throwable) {
                log("NPVS-WARN fatal err=${e.javaClass.simpleName}:${e.message}")
            } finally {
                unsubscribe(this)
                closeClient("loopEnd")
                runCatching { server?.close() }
                server = null
                running.set(false)
            }
        }

        private fun closeClient(reason: String) {
            runCatching { clientSocket?.close() }
            clientSocket = null
            clientOut = null
            if (reason != "newClient") {
                clientAddr = ""
                clientSinceMs = 0L
            }
        }

        override fun close() {
            running.set(false)
            unsubscribe(this)
            closeClient("close")
            runCatching { server?.close() }
            server = null
            worker?.let { runCatching { it.interrupt() } }
            worker = null
            statWorker?.let { runCatching { it.interrupt() } }
            statWorker = null
            val line = stats.flush()
            if (line.isNotEmpty()) log(line)
        }

        private fun log(message: String) {
            Log.i(TAG, message)
            runCatching { onLog(message) }
        }

        private companion object {
            val MAGIC = "NPVS".toByteArray(Charsets.US_ASCII)
        }
    }
}
