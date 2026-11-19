package com.shilapi.xcertplay.network

import android.util.Log
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

    interface Sink {
        fun onConfig(codecByte: Byte, data: ByteArray)
        fun onFrame(frame: ByteArray)
    }

    private val sinks = CopyOnWriteArrayList<Sink>()

    /** 0 = H264, 1 = H265. Called from the media thread. */
    fun relayConfig(codecName: String, codecData: ByteArray) {
        val codecByte: Byte = if (codecName.equals("H265", ignoreCase = true)) 1 else 0
        val cfg = Config(codecByte, codecData.copyOf())
        configRef.set(cfg)
        sinks.forEach { runCatching { it.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
    }

    /** Frames are already Annex-B; copy once only when someone is listening. */
    fun relayFrame(frame: ByteArray) {
        if (sinks.isEmpty()) return
        val copy = frame.copyOf()
        sinks.forEach { runCatching { it.onFrame(copy.copyOf()) } }
    }

    fun subscribe(sink: Sink) {
        sinks.addIfAbsent(sink)
        configRef.get()?.let { cfg -> runCatching { sink.onConfig(cfg.codecByte, cfg.data.copyOf()) } }
    }

    fun unsubscribe(sink: Sink) {
        sinks.remove(sink)
    }

    fun reset() {
        configRef.set(null)
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

        fun start() {
            if (!running.compareAndSet(false, true)) return
            worker = Thread({ acceptLoop() }, "npvs-push").apply { isDaemon = true; start() }
        }

        override fun onConfig(codecByte: Byte, data: ByteArray) {
            push(kind = 1, codec = codecByte, body = data)
        }

        override fun onFrame(frame: ByteArray) {
            push(kind = 2, codec = 0, body = frame)
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
