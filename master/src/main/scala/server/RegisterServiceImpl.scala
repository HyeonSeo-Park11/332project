package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{WorkerInfo, RegisterWorkerResponse, RegisterServiceGrpc}
import global.{MasterState, ConnectionManager}

class RegisterServiceImpl(implicit ec: ExecutionContext) extends RegisterServiceGrpc.RegisterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = Future {
    MasterState.registerWorker(request)
    ConnectionManager.registerWorkerChannel(request.ip, request.port)
    RegisterWorkerResponse(success = true)
  }
}
