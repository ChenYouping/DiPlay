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
 * Optional mirror of the main CarPlay screen stream to a secondary client over the NCM link.
 *
 * It taps the already-decrypted Annex-B H.264/H.265 frames at [relayConfig]/[relayFrame] and
 * forwards them, unchanged, on a dedicated TCP port (default 7101). The primary AirPlay
 * listener and the [DiplayTcpChannel] throughput channel are untouched.
 *
 * Wire format (client connects, server pushes continuously; client never sends):
 *
 *   magic  = 4 bytes ASCII "NPVS"
 *   kind   = 1 byte: 1 = config (codec + codecData), 2 = video frame
 *   codec  = 1 byte: 0 = H264, 1 = H265 (meaningful for config; 0 for frame)
 *   length = 4 bytes signed big-endian int
 *   body   = [length] bytes (avcC/hvcC for config; Annex-B NALUs for frame)
 *
 * ---------------------------------------------------------------------------
 * Observability
 * ---------------------------------------------------------------------------
 * Three log levels, all routed through [logLine] so they reach BOTH logcat and
 * the exportable diagnostic report (onLog -> session log):
 *
 *   NPVS-EVT   state transitions (listen/client/replay/config/reset)
 *   NPVS-STAT  a 5-second heartbeat window with counters on every hop
 *   NPVS-WARN  silent drops, swallowed exceptions, slow/failed socket writes
 *
 * Field names deliberately avoid body=/payload=/hex=/token because the report
 * redactor strips any line containing those substrings.
 */
object VideoFrameRelay {
    private const val TAG = "xcertplay-npvs"
    const val DEFAULT_PORT = 7101
    private const val STAT_WINDOW_MS = 5_000L
    private const val SLOW_PUSH_MS = 50L

    private data class Config(val codecByte: Byte, val data: ByteArray)
    private val configRef = AtomicReference<Config?>(null)

    // A late client misses the WHOLE reference chain, not just one IDR: a lone
    // IDR decodes as the first frame, but the following P frames still refer to
    // frames delivered before the client joined and fail with -12909 forever.
    // Cache all frames from the most recent IDR onwards and replay the entire GOP.
    private val gopLock = Any()
    private data class SeqFrame(val seq: Long, val data: ByteArray)
    private val gopFrames = ArrayList<SeqFrame>()
    private var gopBytes = 0
    private var frameSeq = 0L
    private const val MAX_GOP_BYTES = 8 * 1024 * 1024

    interface Sink {
        fun onConfig(codecByte: Byte, data: ByteArray)
        fun onFrame(frame: ByteArray, seq: Long)
    }

    private val sinks = CopyOnWriteArrayList<Sink>()

    // Log sink installed by Server.start(); lets object-level code (relayFrame,
    // stats timer) emit into the same exportable report as the Server events.
    @Volatile private var logSink: (String) -> Unit = {}
    private fun logLine(message: String) {
        Log.i(TAG, message)
        runCatching { logSink(message) }
    }

    // -----------------------------------------------------------------------
    // Windowed statistics — one NPVS-STAT line every 5 s, even with no frames,
    // so a silent upstream (rx=0) is distinguishable from a healthy idle.
    // -----------------------------------------------------------------------
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
        private var gopTrim = 0L
        private var replayFrames = 0L
        private var replayDrain = 0L
        private var replaySkip = 0L
        @Volatile var clientUpMs = 0L
        @Volatile var gopCount = 0
        @Volatile var gopSize = 0
        @Volatile var queueMax = 0

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
        fun onGopTrim() { synchronized(lock) { gopTrim++ } }
        fun onReplay(frames: Int, drain: Int, skip: Int) { synchronized(lock) {
            replayFrames += frames; replayDrain += drain; replaySkip += skip
        } }
        fun onQueueDepth(n: Int) { if (n > queueMax) queueMax = n }

        fun flush(): String = synchronized(lock) {
            val now = android.os.SystemClock.elapsedRealtime()
            val span = now - startMs
            if (span < STAT_WINDOW_MS) return ""
            val secs = span / 1000.0
            val line = String.format(
                java.util.Locale.US,
                "NPVS-STAT win=%.1fs in=%df/%dkB key=%d push=%df/%dkB " +
                    "noCli=%d closed=%d empty=%d pushFail=%d slow=%d sinkErr=%d " +
                    "trim=%d replay=%d/drain%d/skip%d gop=%df/%dkB qMax=%d cliUp=%ds",
                secs, inFrames, inBytes / 1024, keyframes,
                pushedFrames, pushedBytes / 1024,
                dropNoClient, dropClosed, dropEmpty, pushFail, pushSlow, sinkErrors,
                gopTrim, replayFrames, replayDrain, replaySkip,
                gopCount, gopSize / 1024, queueMax, clientUpMs / 1000
            )
            inFrames = 0; inBytes = 0; keyframes = 0
            pushedFrames = 0; pushedBytes = 0
            dropNoClient = 0; dropClosed = 0; dropEmpty = 0
            pushFail = 0; pushSlow = 0; sinkErrors = 0; gopTrim = 0
            replayFrames = 0; replayDrain = 0; replaySkip = 0
            queueMax = 0
            startMs = now
            line
        }
    }

    private val stats = Stats()

    /** Called by Server.start() so object-level logs reach the report. */
    private fun installLogSink(sink: (String) -> Unit) { logSink = sink }

    /** 0 = H264, 1 = H265. Called from the media thread. */
    fun relayConfig(codecName: String, codecData: ByteArray) {
        val codecByte: Byte = if (codecName.equals("H265", ignoreCase = true)) 1 else 0
        val cfg = Config(codecByte, codecData.copyOf())
        configRef.set(cfg)
        synchronized(gopLock) { gopFrames.clear(); gopBytes = 0 }
        stats.gopCount = 0; stats.gopSize = 0
        logLine("NPVS-EVT config codec=$codecByte bytes=${codecData.size} gopReset")
        sinks.forEach { s ->
            runCatching { s.onConfig(cfg.codecByte, cfg.data.copyOf()) }
                .onFailure { stats.onSinkError(); logLine("NPVS-WARN sinkErr where=config err=${it.javaClass.simpleName}:${it.message}") }
        }
    }

    /** Frames are already Annex-B; copy once only when someone is listening. */
    fun relayFrame(frame: ByteArray) {
        val codec = if (configRef.get()?.codecByte == 1.toByte()) VideoCodec.H265 else VideoCodec.H264
        val isKey = MediaCodecSupport.isRandomAccess(frame, codec)
        if (frame.isEmpty()) {
            stats.onEmpty()
            logLine("NPVS-WARN emptyFrame from=media")
            return
        }
        val copy = frame.copyOf()
        val seq: Long
        synchronized(gopLock) {
            if (isKey) { gopFrames.clear(); gopBytes = 0 }
            if (gopBytes + copy.size > MAX_GOP_BYTES && gopFrames.size > 1) {
                val dropped = gopFrames.removeAt(1)
                gopBytes -= dropped.data.size
                stats.onGopTrim()
            }
            frameSeq++
            seq = frameSeq
            gopFrames.add(SeqFrame(seq, copy))
            gopBytes += copy.size
            stats.gopCount = gopFrames.size
            stats.gopSize = gopBytes
        }
        stats.onIn(copy.size, isKey)
        if (sinks.isEmpty()) { stats.onNoClient(copy.size); return }
        sinks.forEach { s ->
            runCatching { s.onFrame(copy.copyOf(), seq) }
                .onFailure { stats.onSinkError(); logLine("NPVS-WARN sinkErr where=frame err=${it.javaClass.simpleName}:${it.message}") }
        }
    }

    private fun snapshotGop(): List<SeqFrame> = synchronized(gopLock) { gopFrames.map { SeqFrame(it.seq, it.data.copyOf()) } }

    fun subscribe(sink: Sink) {
        // Note: subscribe happens at server start (no client socket yet), so the
        // replay here is normally discarded by push(); the real replay is in
        // acceptLoop. Kept for non-Server sinks and for symmetry.
        sinks.addIfAbsent(sink)
        configRef.get()?.let { cfg -> runCatching { sink.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
        val replay = snapshotGop()
        replay.forEach { runCatching { sink.onFrame(it.data, it.seq) } }
        logLine("NPVS-EVT subscribe sinks=${sinks.size} replay=${replay.size}")
    }

    fun unsubscribe(sink: Sink) { sinks.remove(sink) }

    fun reset() {
        configRef.set(null)
        synchronized(gopLock) { gopFrames.clear(); gopBytes = 0 }
        sinks.clear()
        stats.gopCount = 0; stats.gopSize = 0
        logLine("NPVS-EVT reset gop cleared, sinks cleared")
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
        @Volatile private var replaying = false
        private val replayQueue = java.util.ArrayDeque<SeqFrame>()
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

        override fun onFrame(frame: ByteArray, seq: Long) {
            if (replaying) {
                val depth: Int
                synchronized(replayQueue) {
                    replayQueue.addLast(SeqFrame(seq, frame.copyOf()))
                    depth = replayQueue.size
                }
                stats.onQueueDepth(depth)
            } else {
                push(kind = 2, codec = 0, body = frame)
            }
        }

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
                    // NOTE: do NOT set soTimeout here. The watchdog below blocks on
                    // input.read() waiting for a client that never sends; a read
                    // timeout would fire SocketTimeoutException and falsely close a
                    // perfectly healthy stream every few seconds.
                    clientSocket = socket
                    clientOut = DataOutputStream(socket.getOutputStream())
                    clientAddr = socket.remoteSocketAddress?.toString() ?: "?"
                    clientSinceMs = android.os.SystemClock.elapsedRealtime()
                    log("NPVS-EVT client+ addr=$clientAddr")

                    synchronized(replayQueue) { replayQueue.clear() }
                    replaying = true
                    val t0 = android.os.SystemClock.elapsedRealtime()
                    try {
                        configRef.get()?.let { cfg ->
                            push(kind = 1, codec = cfg.codecByte.toInt(), body = cfg.data.copyOf())
                        }
                        val gop = snapshotGop()
                        gop.forEach { push(kind = 2, codec = 0, body = it.data) }
                        val lastReplayed = gop.lastOrNull()?.seq ?: -1L
                        val pending: List<SeqFrame>
                        synchronized(replayQueue) {
                            pending = replayQueue.toList()
                            replayQueue.clear()
                        }
                        var skipped = 0
                        var drained = 0
                        pending.forEach { sf ->
                            if (sf.seq <= lastReplayed) skipped++ else {
                                push(kind = 2, codec = 0, body = sf.data)
                                drained++
                            }
                        }
                        stats.onReplay(gop.size, drained, skipped)
                        log("NPVS-EVT replay gop=${gop.size} seqEnd=$lastReplayed drain=$drained skip=$skipped ms=${android.os.SystemClock.elapsedRealtime() - t0}")
                    } finally {
                        replaying = false
                    }

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
