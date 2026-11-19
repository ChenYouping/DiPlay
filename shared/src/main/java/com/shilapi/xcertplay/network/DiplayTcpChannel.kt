package com.shilapi.xcertplay.network

import android.util.Log
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A small, self-contained TCP echo/throughput channel bound to the CarPlay tun address.
 *
 * This is intentionally independent of the AirPlay listener: it owns its own port (default
 * 7100) so a third-party client can measure raw NCM throughput or exchange arbitrary bytes
 * without speaking RTSP. Framing is a fixed header so both ends can delimit messages over TCP:
 *
 *   magic  = 4 bytes ASCII "NPCM"
 *   op     = 1 byte  (1 = ECHO, 2 = INFO)
 *   length = 4 bytes signed big-endian int (0 .. MAX_PAYLOAD)
 *   body   = [length] bytes
 *
 * Response mirrors the header with magic "NPCK" and an extra status byte:
 *
 *   magic  = 4 bytes ASCII "NPCK"
 *   status = 1 byte  (0 = ok, nonzero = error)
 *   length = 4 bytes signed big-endian int
 *   body   = [length] bytes
 *
 * ECHO returns the exact request body. INFO returns a UTF-8 text line describing the channel.
 * Only one client at a time is served; a new connection supersedes the previous one, which keeps
 * throughput measurements deterministic.
 */
class DiplayTcpChannel(
    private val bindAddress: InetAddress,
    private val port: Int = DEFAULT_PORT,
    private val onLog: (String) -> Unit = {},
) : AutoCloseable {

    private val running = AtomicBoolean(false)
    private var server: ServerSocket? = null
    private var worker: Thread? = null
    @Volatile private var client: Socket? = null

    fun start() {
        if (!running.compareAndSet(false, true)) return
        worker = Thread({ acceptLoop() }, "ncm-tcp-channel").apply { isDaemon = true; start() }
    }

    val boundPort: Int get() = server?.localPort ?: port

    private fun acceptLoop() {
        try {
            val srv = ServerSocket()
            srv.reuseAddress = true
            srv.bind(InetSocketAddress(bindAddress, port))
            server = srv
            log("NPCM channel listening on ${srv.localSocketAddress}:${srv.localPort}")
            while (running.get()) {
                val socket = try {
                    srv.accept()
                } catch (e: SocketException) {
                    if (running.get()) log("NPCM accept error: ${e.message}")
                    break
                } catch (e: IOException) {
                    if (running.get()) log("NPCM accept error: ${e.message}")
                    break
                }
                client?.let { runCatching { it.close() } }
                client = socket
                log("NPCM client connected: ${socket.remoteSocketAddress}")
                serveClient(socket)
                runCatching { socket.close() }
                if (client === socket) client = null
                if (!running.get()) break
            }
        } catch (e: Throwable) {
            log("NPCM channel failed: ${e.message}")
        } finally {
            runCatching { server?.close() }
            server = null
            running.set(false)
        }
    }

    private fun serveClient(socket: Socket) {
        try {
            socket.tcpNoDelay = true
            val input = DataInputStream(socket.getInputStream())
            val output = DataOutputStream(socket.getOutputStream())
            while (running.get() && !socket.isClosed) {
                val magic = readFullyOrNull(input, 4) ?: break
                if (!magic.contentEquals(MAGIC_REQUEST)) {
                    log("NPCM bad magic from ${socket.remoteSocketAddress}")
                    break
                }
                val op = input.readUnsignedByte()
                val length = input.readInt()
                if (length < 0 || length > MAX_PAYLOAD) {
                    log("NPCM bad length $length")
                    break
                }
                val body = ByteArray(length)
                input.readFully(body)

                when (op) {
                    OP_ECHO -> writeResponse(output, STATUS_OK, body)
                    OP_INFO -> {
                        val info = "DiPlay-NPCM/1.0 bind=$bindAddress port=${server?.localPort}".toByteArray(Charsets.UTF_8)
                        writeResponse(output, STATUS_OK, info)
                    }
                    else -> writeResponse(output, STATUS_BAD_OP, ByteArray(0))
                }
                output.flush()
            }
        } catch (e: EOFException) {
            log("NPCM client closed")
        } catch (e: IOException) {
            if (running.get()) log("NPCM io error: ${e.message}")
        } finally {
            runCatching { socket.close() }
        }
    }

    private fun writeResponse(output: DataOutputStream, status: Int, body: ByteArray) {
        output.write(MAGIC_RESPONSE)
        output.writeByte(status)
        output.writeInt(body.size)
        output.write(body)
    }

    private fun readFullyOrNull(input: DataInputStream, count: Int): ByteArray? = try {
        val buf = ByteArray(count)
        input.readFully(buf)
        buf
    } catch (_: EOFException) {
        null
    }

    override fun close() {
        running.set(false)
        runCatching { client?.close() }
        client = null
        runCatching { server?.close() }
        server = null
        worker?.let { runCatching { it.interrupt() } }
        worker = null
    }

    private fun log(message: String) {
        Log.i(TAG, message)
        runCatching { onLog(message) }
    }

    companion object {
        private const val TAG = "xcertplay-npcm"
        const val DEFAULT_PORT = 7100
        const val MAX_PAYLOAD = 4 * 1024 * 1024

        private val MAGIC_REQUEST = "NPCM".toByteArray(Charsets.US_ASCII)
        private val MAGIC_RESPONSE = "NPCK".toByteArray(Charsets.US_ASCII)
        const val OP_ECHO = 1
        const val OP_INFO = 2
        const val STATUS_OK = 0
        const val STATUS_BAD_OP = 1
    }
}
