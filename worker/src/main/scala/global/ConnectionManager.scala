package global

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

object ConnectionManager {
    private val maxGrpcMessageSize: Int = 1024 * 1024 * 1024  // 1GB

    private var masterChannel: ManagedChannel = _

    def createChannel(ip: String, port: Int): ManagedChannel = {
        ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(maxGrpcMessageSize).usePlaintext().build()
    }

    def initMasterChannel(ip: String, port: Int): Unit = {
        masterChannel = createChannel(ip, port)
    }
    
    def getMasterChannel(): ManagedChannel = {
        masterChannel
    }

    def shutdownAllChannels(): Unit = {
        masterChannel.shutdown()
    }
}