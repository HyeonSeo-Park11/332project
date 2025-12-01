package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{FinalMergePhaseReport, FinalMergePhaseAck}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{WorkerServiceGrpc, TerminateCommand}

class FinalMergeServiceImpl(implicit ec: ExecutionContext) {
  def reportFinalMergeCompletion(request: FinalMergePhaseReport): Future[FinalMergePhaseAck] = Future {
    MasterState.markFinalMergeCompleted(request.workerIp)
    println(s"Worker ${request.workerIp} completed final merge")

    if (MasterState.allFinalMergeCompleted) {
      terminateWorkers()
    }

    FinalMergePhaseAck(success = true)
  }

  private def terminateWorkers(): Future[Unit] = {
    assert(!MasterState.isTerminated, "Termination should not double triggered without fault tolerance")

    MasterState.markTerminated()
    println("All workers reported final merge completion. Triggering termination phase...")

    val workers = MasterState.getRegisteredWorkers
    val terminateFutures = workers.map { case (ip, info) =>
      val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
      val request = TerminateCommand(reason = "")
      stub.terminate(request).map(_ => ()).recover {
        case e: Exception => 
          println(s"Failed to send terminate command to worker $ip:${info.port}: ${e.getMessage}")
      }
    }

    Future.sequence(terminateFutures).flatMap { _ =>
      Future {
        println("All workers should be terminated. Initiating master shutdown...")
        Thread.sleep(5000)
        ConnectionManager.shutdownAllChannels()
        System.exit(0)
      }
    }
  }
}
