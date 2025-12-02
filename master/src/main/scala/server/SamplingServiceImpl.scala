package server

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import master.MasterService.{SampleData, SampleResponse, SamplingServiceGrpc}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, WorkerRangeAssignment, WorkerNetworkInfo, RangeAssignment}

class SamplingServiceImpl(implicit ec: ExecutionContext) extends SamplingServiceGrpc.SamplingService {
  override def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = MasterState.addSamples(request.workerIp, keys)

    // If all workers have sent samples, spawn a thread to calculate ranges and assign them
    if (MasterState.getSampleSize == MasterState.getWorkersNum && success) {
      Future {
        MasterState.calculateRanges()
        if (MasterState.isRangesReady) {
          assignRangesToWorkers()
        }
      }
    }

    Future.successful(
      SampleResponse(success = success)
    )
  }

  private def assignRangesToWorkers(): Unit = {
    val workers = MasterState.getRegisteredWorkers.toSeq.sortBy(_._1)
    val ranges = MasterState.getRanges

    println("Assigning ranges to workers...")

    // Initialize worker channels before assigning ranges
    ConnectionManager.initWorkerChannels(workers.map { case (ip, info) => (ip, info.port) })
    
    val request = WorkersRangeAssignment(
      assignments = ranges.map { case ((workerIp, workerPort), (start, end)) =>
        WorkerRangeAssignment(
          worker = Some(WorkerNetworkInfo(ip = workerIp, port = workerPort)),
          range = Some(RangeAssignment(start = start, end = end))
        )
      }.toSeq
    )
    
    for ((ip, info) <- workers) {
      async {
        val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
        val response = await(stub.assignRanges(request))
        response.success
      }.recover { case e =>
        println(s"Failed to assign range to worker $ip:${info.port}: ${e.getMessage}")
        false
      }
    }
  }
}
