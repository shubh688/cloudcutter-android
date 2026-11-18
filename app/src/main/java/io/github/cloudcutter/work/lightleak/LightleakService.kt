/*
 * Copyright (c) Kuba Szczodrzyński 2022-10-7.
 */

package io.github.cloudcutter.work.lightleak

import android.app.Service
import android.content.Intent
import android.net.Network
import android.os.Binder
import android.util.Log
import io.github.cloudcutter.ext.crc32
import io.github.cloudcutter.ext.getBroadcastAddress
import io.github.cloudcutter.ext.bindDatagramSocket
import io.github.cloudcutter.ext.sendOnNetwork
import io.github.cloudcutter.util.RequestBus
import io.github.cloudcutter.work.exceptions.PacketReadException
import io.github.cloudcutter.work.exceptions.ServiceDisconnectedException
import io.github.cloudcutter.work.lightleak.command.CommandRequest
import io.github.cloudcutter.work.lightleak.command.FlashReadCommand
import io.github.cloudcutter.work.protocol.proper.FlashReadPacket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.ceil

class LightleakService : Service(), CoroutineScope {
	companion object {
		private const val TAG = "LightleakService"
		private const val DIAG = "CLOUDCUTTER_UDP_DIAG_V1"
	}

	override val coroutineContext = Job() + Dispatchers.IO
	private val bus = RequestBus<CommandRequest>()
	private val packets = Channel<Pair<Int, ByteArray>>(Channel.UNLIMITED)

	private var data: LightleakData? = null
	private var udpListener: Job? = null
	@Volatile private var udpSocket: DatagramSocket? = null
	private val log
		get() = data?.serviceLog

	private var newRequestId = 1
		get() {
			field++
			return field
		}

	inner class ServiceBinder : Binder() {
		fun setData(data: LightleakData) {
			this@LightleakService.data = data
		}

		fun setNetwork(network: Network?) {
			data?.wifiNetwork = network
			startUdpListener(network)
		}

		suspend fun <T : CommandRequest, D> request(command: T) = bus.request<D>(command)
	}

	override fun onCreate() {
		Log.d(TAG, "Creating $TAG")
		bus.subscribe(this, this::onCommand)
	}

	override fun onBind(intent: Intent) = ServiceBinder()

	override fun onUnbind(intent: Intent): Boolean {
		udpSocket?.close()
		udpListener?.cancel()
		this@LightleakService.data = null
		return false
	}

	override fun onDestroy() {
		Log.d(TAG, "Destroying $TAG")
		udpSocket?.close()
		this.cancel()
	}

	private fun startUdpListener(network: Network?) {
		udpSocket?.close()
		udpListener?.cancel()
		if (network == null) {
			Log.i(TAG, "$DIAG Wi-Fi network unavailable; UDP listener stopped")
			return
		}
		udpListener = launch {
			val socket = DatagramSocket(null)
			try {
				network.bindDatagramSocket(socket)
				socket.reuseAddress = true
				socket.bind(InetSocketAddress("0.0.0.0", 6667))
				udpSocket = socket
				Log.i(TAG, "$DIAG listener bound to UDP 0.0.0.0:6667 on $network")
				while (isActive) {
					val buffer = ByteArray(65535)
					val datagram = DatagramPacket(buffer, buffer.size)
					socket.receive(datagram)
					if (datagram.length < 8) continue
					val packetBytes = datagram.data.copyOfRange(datagram.offset, datagram.offset + datagram.length)
					val header = ByteBuffer.wrap(packetBytes).order(ByteOrder.LITTLE_ENDIAN)
					val requestId = header.int
					val receivedCrc = header.int
					val payload = packetBytes.copyOfRange(8, packetBytes.size)
					val actualCrc = payload.crc32()
					log?.invoke("$DIAG UDP RX id=$requestId bytes=${payload.size} crcOK=${receivedCrc == actualCrc} from=${datagram.address.hostAddress}:${datagram.port}")
					if (receivedCrc == actualCrc) packets.send(requestId to payload)
					else log?.invoke("$DIAG CRC mismatch: received=$receivedCrc actual=$actualCrc")
				}
			} catch (e: Exception) {
				if (isActive) {
					Log.e(TAG, "$DIAG UDP listener exception", e)
					log?.invoke("$DIAG UDP listener exception: ${e.javaClass.simpleName}: ${e.message}")
				}
			} finally {
				if (udpSocket === socket) udpSocket = null
				socket.close()
			}
		}
	}

	private suspend fun onCommand(command: CommandRequest): Any {
		log?.invoke("Running command: $command")
		return when (command) {
			is FlashReadCommand -> flashRead(
				start = command.offset,
				end = command.offset + command.length,
				output = command.output,
			)
			else -> throw NotImplementedError("Unknown command $command")
		}
	}

	private suspend fun flashRead(start: Int, end: Int, output: File): List<ByteArray> {
		val readPacketSize = 1024
		val readPacketsMax = 8
		val readTimeoutMs = 10

		val size = end - start
		val packetCount = ceil(size / readPacketSize.toFloat()).toInt()
		val packetList = MutableList<ByteArray?>(packetCount) { null }
		val packetOffsets = MutableList(packetCount) { start + readPacketSize * it }

		var pauseCount = 0
		var failedCount = 0
		var exception: Exception? = null
		while (isActive) {
			val readStartIndex = packetList.indexOf(null)
			if (readStartIndex == -1)
				break
			val readEndIndex = packetList
				.subList(readStartIndex + 1, packetCount)
				.indexOfFirst { it != null }

			val readPacketCount = when (readEndIndex) {
				-1 -> packetCount - readStartIndex
				else -> readEndIndex + 1
			}.coerceAtMost(readPacketsMax)

			val requestId = newRequestId
			val readOffset = packetOffsets[readStartIndex]
			// read flash
			log?.invoke("Reading data #$readStartIndex, offset=0x${readOffset.toString(16)}, count=$readPacketCount")
			val packet = FlashReadPacket(
				profile = data?.profileData ?: throw ServiceDisconnectedException(),
				requestId = requestId,
				offset = readOffset,
				length = readPacketCount * readPacketSize,
				maxLength = readPacketSize,
			).also { it.returnIp = data?.localAddress ?: getBroadcastAddress() }
			if (failedCount < 3 || failedCount % 25 == 0) {
				log?.invoke("$DIAG UDP TX id=$requestId offset=0x${readOffset.toString(16)} " +
					"bytes=${readPacketCount * readPacketSize} " +
					"destination=${(data?.gatewayAddress ?: getBroadcastAddress()).hostAddress}:6669 " +
					"returnIp=${(data?.localAddress ?: getBroadcastAddress()).hostAddress}")
			}
			try {
				val network = data?.wifiNetwork ?: throw IllegalStateException("Connected Wi-Fi network is unavailable")
				packet.sendOnNetwork(data?.gatewayAddress ?: getBroadcastAddress(), network)
			} catch (e: Throwable) {
				log?.invoke("$DIAG UDP TX error: ${e.javaClass.simpleName}: ${e.message}")
				Log.e(TAG, "$DIAG UDP TX error", e)
				delay(500)
			}

			// wait a moment
			if (pauseCount++ == 100)
				delay(500)
//			else
//				delay(20)

			// receive all currently buffered packets
			var packetsReceived = 0
			val endTime = System.currentTimeMillis() + readTimeoutMs
			while (packetsReceived < readPacketsMax && System.currentTimeMillis() < endTime) {
				val result = packets.tryReceive()
				if (!result.isSuccess) {
					delay(5)  // Let listener coroutine run; avoid tight polling
					continue
				}
				val bytes = result.getOrThrow().second
				val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
				val offset = buf.int
				val length = buf.int
				val index = packetOffsets.indexOf(offset)
				log?.invoke("$DIAG decoded reply index=$index offset=0x${offset.toString(16)} " +
					"declaredLength=$length bytes=${bytes.size}")
				if (index < 0 || length < 0 || length > bytes.size - 8) {
					log?.invoke("$DIAG ignored unexpected/malformed reply")
					continue
				}
				packetList[index] = bytes.sliceArray(8 until length + 8)
				packetsReceived++
			}

			// check if all packets were received
			val progress = packetList.count { it != null }
			if (packetsReceived > 0) {
				data?.progressValue?.postValue(progress * 100 / packetCount)
				data?.progressBytes?.postValue(progress * readPacketSize)
				failedCount = 0
			} else if (failedCount++ >= 200) {
				exception = PacketReadException()
				break
			}
			if (progress == packetCount)
				break
		}
		data?.progressValue?.postValue(null)
		data?.progressBytes?.postValue(null)

		withContext(Dispatchers.IO) {
			RandomAccessFile(output, "rw").use {
				packetList.forEachIndexed { index, chunk ->
					chunk ?: return@forEachIndexed
					it.seek((start + index * readPacketSize).toLong())
					it.write(chunk)
				}
			}
		}

		if (exception != null)
			throw exception

		return packetList.filterNotNull()
	}
}
