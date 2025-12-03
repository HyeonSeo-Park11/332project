package global

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.collection.mutable

object ConnectionManager {
    val maxGrpcMessageSize: Int = 1024 * 1024 * 1024  // 1GB

    private var workerChannels: mutable.Map[String, ManagedChannel] = mutable.Map()

    def createChannel(ip: String, port: Int): ManagedChannel = {
        ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(maxGrpcMessageSize).usePlaintext().build()
    }

    def registerWorkerChannel(ip: String, port: Int): Unit = this.synchronized {
        workerChannels.get(ip).foreach { existingChannel =>
            if (!existingChannel.isShutdown) {
                existingChannel.shutdown()
            }
        }
        workerChannels(ip) = createChannel(ip, port)
    }

    def getWorkerChannel(ip: String): ManagedChannel = this.synchronized {
        workerChannels.getOrElse(ip, throw new NoSuchElementException(s"Worker channel for $ip not found. It should always exist."))
    }

    def shutdownAllChannels(): Unit = this.synchronized {
        workerChannels.values.foreach(_.shutdown())
        workerChannels.clear()
    }
}
