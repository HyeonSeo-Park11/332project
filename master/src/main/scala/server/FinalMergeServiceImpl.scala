package server

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{FinalMergePhaseReport, FinalMergePhaseAck, FinalMergeServiceGrpc}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{TerminationServiceGrpc, TerminateCommand}
import common.utils.RetryUtils.retry
import scala.async.Async.{async, await}

class FinalMergeServiceImpl(implicit ec: ExecutionContext) extends FinalMergeServiceGrpc.FinalMergeService {
  private val logger = LoggerFactory.getLogger(getClass)
  
  override def reportFinalMergeCompletion(request: FinalMergePhaseReport): Future[FinalMergePhaseAck] = Future {
    MasterState.markFinalMergeCompleted(request.workerIp)
    logger.info(s"Worker ${request.workerIp} completed final merge")

    if (MasterState.allFinalMergeCompleted) {
      terminateWorkers()
    }

    FinalMergePhaseAck(success = true)
  }

  private def terminateWorkers(): Future[Unit] = {
    if (MasterState.isTerminated) return Future.successful(())

    MasterState.markTerminated()
    logger.info("All workers reported final merge completion. Triggering termination phase...")

    val workers = MasterState.getRegisteredWorkers
    val terminateFutures = workers.map { case (ip, info) =>
      retry {
        async {
          val stub = TerminationServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
          val request = TerminateCommand(reason = "")
          await { stub.terminate(request) }
          logger.info(s"Termination acknowledged by worker $ip:${info.port}")
        }
      }
    }

    Future.sequence(terminateFutures).map { _ =>
      logger.info("All workers should be terminated. Signaling master shutdown...")
      MasterState.signalShutdown()
    }.recover {
      case e: Exception =>
        logger.error(s"Error during termination phase: ${e.getMessage}. Forcing shutdown...")
        MasterState.signalShutdown()
    }
  }
}
