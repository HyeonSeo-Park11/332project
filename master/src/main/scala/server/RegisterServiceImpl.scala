package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{WorkerInfo, RegisterWorkerResponse, RegisterServiceGrpc}
import global.MasterState

class RegisterServiceImpl(implicit ec: ExecutionContext) extends RegisterServiceGrpc.RegisterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = Future {
    MasterState.registerWorker(request)
    RegisterWorkerResponse(success = true)
  }
}
