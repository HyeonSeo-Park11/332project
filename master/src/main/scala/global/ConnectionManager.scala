package global

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.collection.mutable

object ConnectionManager {
    val maxGrpcMessageSize: Int = 1024 * 1024 * 1024  // 1GB

    private var workerChannels: mutable.Map[String, ManagedChannel] = mutable.Map()

    def createChannel(ip: String, port: Int): ManagedChannel = {
        ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(maxGrpcMessageSize).usePlaintext().build()
    }

    def initWorkerChannels(workers: Seq[(String, Int)]): Unit = this.synchronized {
        assert( workers.map(_._1).toSet.size == workers.size, "Worker IPs must be unique" )
        workers.foreach { case (ip, port) => 
            workerChannels += ip -> createChannel(ip, port)
        }
    }

    def setWorkerChannel(ip: String, port: Int): Unit = this.synchronized {
        getWorkerChannel(ip).shutdown()
        workerChannels(ip) = createChannel(ip, port)
    }

    def getWorkerChannel(ip: String): ManagedChannel = this.synchronized {
        assert( workerChannels.contains(ip), s"Worker channel should always exist, even if invalid port" )
        workerChannels(ip)
    }

    def shutdownAllChannels(): Unit = this.synchronized {
        workerChannels.values.foreach(_.shutdown())
    }
}
