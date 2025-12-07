package main

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import global.ConnectionManager
import master.MasterService.{FinalMergeServiceGrpc, FinalMergePhaseReport}
import scala.async.Async.{async, await}
import common.utils.SystemUtils
import io.grpc.Server
import state.TerminationState

class TerminationManager(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val masterStub = FinalMergeServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def shutdownServerSafely(server: Server): Future[Unit] = async {
    if(TerminationState.isTerminated) {
      // If fault occured right before returning TerminateACK in TerminationServiceImpl,
      // Worker should wait for a seconds before turning off to allow the master to exit retry.
      logger.info("Termination already marked. Skipping final merge report to master.")
      Thread.sleep(30000)
    } else {
      val request = FinalMergePhaseReport(workerIp = SystemUtils.getLocalIp)
      await { masterStub.reportFinalMergeCompletion(request) }
    }
    await { TerminationState.waitForTerminate }

    ConnectionManager.shutdownAllChannels()
    server.shutdown()
    server.awaitTermination()
    ()
  }
}