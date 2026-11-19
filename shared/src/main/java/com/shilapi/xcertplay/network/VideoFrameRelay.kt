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
 * The server sends a config message first (once a config is known) and then every frame that
 * arrives after the client subscribes. Late subscribers simply wait for the next config/frame,
 * which the iPhone refreshes periodically (keyframes), so no catch-up buffer is needed.
 */
object VideoFrameRelay {
    private const val TAG = "xcertplay-npvs"
    const val DEFAULT_PORT = 7101

    private data class Config(val codecByte: Byte, val data: ByteArray)
    private val configRef = AtomicReference<Config?>(null)
    // A late client misses the WHOLE reference chain, not just one IDR: a lone
    // IDR decodes as the first frame, but every following P frame still refers to
    // frames delivered before the client joined and fails with
    // kVTVideoDecoderBadDataErr (-12909) forever. Cache all frames from the most
    // recent IDR onwards and replay the entire GOP so the decoder gets a complete,
    // decodable chain that continues seamlessly into the live stream.
    private val gopLock = Any()
    // Frame + monotonically increasing sequence. The sequence lets a connecting
    // client skip any frame that was already sent in the GOP replay but also
    // landed in the live queue — a duplicate P frame corrupts VideoToolbox's
    // reference chain and causes permanent -12909 failures.
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

    /** 0 = H264, 1 = H265. Called from the media thread. */
    fun relayConfig(codecName: String, codecData: ByteArray) {
        val codecByte: Byte = if (codecName.equals("H265", ignoreCase = true)) 1 else 0
        val cfg = Config(codecByte, codecData.copyOf())
        configRef.set(cfg)
        // Encoding parameters changed: cached GOP belongs to the old config.
        synchronized(gopLock) { gopFrames.clear(); gopBytes = 0 }
        sinks.forEach { runCatching { it.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
    }

    /** Frames are already Annex-B; copy once only when someone is listening. */
    fun relayFrame(frame: ByteArray) {
        // Maintain a replayable GOP: start a new one at every IDR (a random access
        // point), otherwise append to the current one. A connecting client is
        // replayed the whole GOP so its decoder has every reference P frames need.
        val codec = if (configRef.get()?.codecByte == 1.toByte()) VideoCodec.H265 else VideoCodec.H264
        val isKey = MediaCodecSupport.isRandomAccess(frame, codec)
        val copy = frame.copyOf()
        val seq: Long
        synchronized(gopLock) {
            if (isKey) { gopFrames.clear(); gopBytes = 0 }
            // Bound memory: if a single GOP grows huge (rare), drop the oldest
            // entries but keep the head IDR so the chain stays decodable.
            if (gopBytes + copy.size > MAX_GOP_BYTES && gopFrames.size > 1) {
                val dropped = gopFrames.removeAt(1)
                gopBytes -= dropped.data.size
            }
            frameSeq++
            seq = frameSeq
            gopFrames.add(SeqFrame(seq, copy))
            gopBytes += copy.size
        }
        if (sinks.isEmpty()) return
        sinks.forEach { runCatching { it.onFrame(copy.copyOf(), seq) } }
    }

    /** Immutable snapshot of the current GOP for late-client catch-up. */
    private fun snapshotGop(): List<SeqFrame> = synchronized(gopLock) { gopFrames.map { SeqFrame(it.seq, it.data.copyOf()) } }

    fun subscribe(sink: Sink) {
        sinks.addIfAbsent(sink)
        configRef.get()?.let { cfg -> runCatching { sink.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
        // Replay the buffered GOP (IDR + all following P frames) so the decoder
        // has a complete reference chain instead of one orphan IDR.
        val replay = snapshotGop()
        replay.forEach { runCatching { sink.onFrame(it.data, it.seq) } }
        if (replay.isNotEmpty()) Log.i(TAG, "subscribe: replayed GOP of ${replay.size} frames")
    }

    fun unsubscribe(sink: Sink) {
        sinks.remove(sink)
    }

    fun reset() {
        configRef.set(null)
        synchronized(gopLock) { gopFrames.clear(); gopBytes = 0 }
        sinks.clear()
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
        @Volatile private var clientOut: DataOutputStream? = null
        @Volatile private var clientSocket: Socket? = null
        // While replaying the buffered GOP to a fresh client, live frames are
        // queued instead of written, so they cannot interleave into the replayed
        // reference chain and corrupt it. They are flushed in order afterwards.
        @Volatile private var replaying = false
        private val replayQueue = java.util.ArrayDeque<SeqFrame>()

        fun start() {
            if (!running.compareAndSet(false, true)) return
            worker = Thread({ acceptLoop() }, "npvs-push").apply { isDaemon = true; start() }
        }

        override fun onConfig(codecByte: Byte, data: ByteArray) {
            push(kind = 1, codec = codecByte.toInt(), body = data)
        }

        override fun onFrame(frame: ByteArray, seq: Long) {
            if (replaying) {
                synchronized(replayQueue) { replayQueue.addLast(SeqFrame(seq, frame.copyOf())) }
            } else {
                push(kind = 2, codec = 0, body = frame)
            }
        }

        private fun push(kind: Int, codec: Int, body: ByteArray) {
            val out = clientOut ?: return
            val socket = clientSocket ?: return
            if (socket.isClosed) return
            try {
                synchronized(out) {
                    out.write(MAGIC)
                    out.writeByte(kind)
                    out.writeByte(codec)
                    out.writeInt(body.size)
                    out.write(body)
                    out.flush()
                }
            } catch (e: IOException) {
                log("push failed, dropping client: ${e.message}")
                closeClient()
            }
        }

        private fun acceptLoop() {
            try {
                val srv = ServerSocket()
                srv.reuseAddress = true
                srv.bind(InetSocketAddress(bindAddress, port))
                server = srv
                log("NPVS listening on ${srv.localSocketAddress}:${srv.localPort}")
                subscribe(this)
                while (running.get()) {
                    val socket = try {
                        srv.accept()
                    } catch (e: IOException) {
                        if (running.get()) log("NPVS accept error: ${e.message}")
                        break
                    }
                    closeClient()
                    socket.tcpNoDelay = true
                    clientSocket = socket
                    clientOut = DataOutputStream(socket.getOutputStream())
                    log("NPVS client connected: ${socket.remoteSocketAddress}")
                    // Replay config + the whole buffered GOP into the new client while
                    // holding back live frames. A single IDR is not enough: the P frames
                    // right after it reference pictures that were delivered before the
                    // client joined, so VideoToolbox rejects all of them (-12909) and the
                    // picture freezes on that one IDR. Replaying the complete GOP hands
                    // the decoder a full, self-consistent reference chain.
                    synchronized(replayQueue) { replayQueue.clear() }
                    replaying = true
                    try {
                        configRef.get()?.let { cfg ->
                            push(kind = 1, codec = cfg.codecByte.toInt(), body = cfg.data.copyOf())
                        }
                        val gop = snapshotGop()
                        gop.forEach { push(kind = 2, codec = 0, body = it.data) }
                        // Highest sequence already delivered via replay. Any queued
                        // frame with seq <= this was in the snapshot too — sending it
                        // again duplicates a P frame and permanently breaks the
                        // decoder's reference chain (all subsequent frames -12909).
                        val lastReplayed = gop.lastOrNull()?.seq ?: -1L
                        log("NPVS replayed GOP (${gop.size} frames, lastSeq=$lastReplayed) to new client")
                        // Drain only genuinely new live frames, strictly in order.
                        val pending: List<SeqFrame>
                        synchronized(replayQueue) {
                            pending = replayQueue.toList()
                            replayQueue.clear()
                        }
                        var skipped = 0
                        pending.forEach { sf ->
                            if (sf.seq <= lastReplayed) { skipped++ } else {
                                push(kind = 2, codec = 0, body = sf.data)
                            }
                        }
                        if (skipped > 0 || pending.isNotEmpty()) {
                            log("NPVS live drain: ${pending.size} queued, skipped $skipped duplicates")
                        }
                    } finally {
                        replaying = false
                    }
                    // Keep the accepted socket alive; push() writes from the media thread.
                    // A reader watchdog detects disconnects that writes alone would miss.
                    val watchdog = Thread({
                        val buf = ByteArray(16)
                        val input = try { socket.getInputStream() } catch (_: IOException) { null }
                        while (running.get() && !socket.isClosed && input != null) {
                            try {
                                if (input.read(buf) < 0) break
                            } catch (_: IOException) {
                                break
                            }
                        }
                        if (clientSocket === socket) closeClient()
                    }, "npvs-watchdog").apply { isDaemon = true }
                    watchdog.start()
                }
            } catch (e: Throwable) {
                log("NPVS failed: ${e.message}")
            } finally {
                unsubscribe(this)
                closeClient()
                runCatching { server?.close() }
                server = null
                running.set(false)
            }
        }

        private fun closeClient() {
            runCatching { clientSocket?.close() }
            clientSocket = null
            clientOut = null
        }

        override fun close() {
            running.set(false)
            unsubscribe(this)
            closeClient()
            runCatching { server?.close() }
            server = null
            worker?.let { runCatching { it.interrupt() } }
            worker = null
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
