package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{WorkerInfo, RegisterWorkerResponse}
import global.MasterState

class RegisterServiceImpl(implicit ec: ExecutionContext) {
  def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = {
    MasterState.registerWorker(request)

    Future.successful(
      RegisterWorkerResponse(success = true)
    )
  }
}
