package main

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import global.ConnectionManager
import master.MasterService.{FinalMergeServiceGrpc, FinalMergePhaseReport}
import scala.async.Async.{async, await}
import common.utils.SystemUtils
import io.grpc.Server

class TerminationManager(implicit ec: ExecutionContext) {
  private val masterStub = FinalMergeServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def shutdownServerSafely(server: Server): Future[Unit] = async {
    val request = FinalMergePhaseReport(workerIp = SystemUtils.getLocalIp.get)
    await { masterStub.reportFinalMergeCompletion(request) }
    await { global.WorkerState.waitForTerminate }

    ConnectionManager.shutdownAllChannels()
    server.shutdown()
    server.awaitTermination()
    ()
  }
}