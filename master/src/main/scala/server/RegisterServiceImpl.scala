package server

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService
import master.MasterService.{WorkerInfo, RegisterWorkerResponse}
import global.{MasterState, ConnectionManager}
import scala.async.Async.{async, await}
import worker.WorkerService
import worker.WorkerService.WorkerNetworkInfo
import worker.WorkerService.{IntroduceAck => logger}

class RegisterServiceImpl(implicit ec: ExecutionContext) extends MasterService.RegisterServiceGrpc.RegisterService {
  private val logger = LoggerFactory.getLogger(getClass)

  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = Future {
    val (isRegistered, faultOccured) = MasterState.registerWorker(request)
    if (!isRegistered) {
      RegisterWorkerResponse(success = false)
    }
    else {
      ConnectionManager.registerWorkerChannel(request.ip, request.port)
      // Without second condition, revived worker tries to re-register toward terminated worker whose server is closed.
      if (faultOccured && !MasterState.isTerminated) {
        MasterState.getRegisteredWorkers.filter(_._1 != request.ip).map { case (workerIp, _) => async {
            val stub = WorkerService.RegisterServiceGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
            await { stub.introduceNewWorker(new WorkerNetworkInfo(request.ip, request.port)) }
          }.recover {
          case e: Throwable =>
            logger.info("introduceNewWorker call from master to worker was failed: ", e)
        }
        }
      }
      RegisterWorkerResponse(success = true)
    }
  }
}
