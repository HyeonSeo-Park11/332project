package main

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse, SampleData, SampleResponse}
import com.google.protobuf.ByteString
import global.ConnectionManager

class MasterManager(implicit ec: ExecutionContext) {
  private val stub = MasterServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def sampling(workerIp: String, keys: Array[ByteString]): Boolean = {
    val request = SampleData(
      workerIp = workerIp,
      keys = keys
    )

    val responseFuture = stub.sampling(request)
    
    try {
      val response = Await.result(responseFuture, 30.seconds)
      response.success
    } catch {
      case e: Exception =>
        println(s"Error sending samples to master: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }
}
