package server

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.concurrent.Await
import master.MasterService.{SampleData, SampleResponse}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, WorkerRangeAssignment, WorkerNetworkInfo, RangeAssignment}

class SamplingServiceImpl(implicit ec: ExecutionContext) {
  def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = MasterState.addSamples(request.workerIp, keys)

    // If all workers have sent samples, calculate ranges
    if (MasterState.getSampleSize == MasterState.getWorkersNum && success) {
      MasterState.calculateRanges()
      // If ranges are ready, trigger range assignment
      if (MasterState.isRangesReady) {
        // Spawn a separate thread to assign ranges to workers
        Future {
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

    for ((ip, info) <- workers) {
      val assign = Future {
        val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
        val request = WorkersRangeAssignment(
          assignments = ranges.map { case ((workerIp, workerPort), (start, end)) =>
            WorkerRangeAssignment(
              worker = Some(WorkerNetworkInfo(ip = workerIp, port = workerPort)),
              range = Some(RangeAssignment(start = start, end = end))
            )
          }.toSeq
        )

        val responseFuture = stub.assignRanges(request)
        try {
          val response = Await.result(responseFuture, 10.seconds)
          response.success
        } catch {
          case e: Exception =>
            println(s"Error assigning range to worker: ${e.getMessage}")
            false
        }
      }
      assign.recover {
        case e: Exception =>
          println(s"Failed to assign range to worker $ip:${info.port}: ${e.getMessage}")
      }
    }
  }
}
