import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import utils.MasterOptionUtils
import server.{RegisterServiceImpl, SamplingServiceImpl, SyncAndShuffleServiceImpl, FinalMergeServiceImpl}
import global.{MasterState, ConnectionManager}
import master.MasterService.MasterServiceGrpc
import common.utils.SystemUtils
import io.grpc.ServerServiceDefinition

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val workersNum = MasterOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }

  MasterState.setWorkersNum(workersNum)

  val ip = SystemUtils.getLocalIp.getOrElse {
    println("Failed to get local IP address")
    sys.exit(1)
  }

  // Create service implementations
  val registerService = new RegisterServiceImpl()
  val samplingService = new SamplingServiceImpl()
  val syncAndShuffleService = new SyncAndShuffleServiceImpl()
  val finalMergeService = new FinalMergeServiceImpl()

  // Bind all services to the same gRPC service definition
  val serviceDefinition = MasterServiceGrpc.bindService(new MasterServiceGrpc.MasterService {
    override def registerWorker(request: master.MasterService.WorkerInfo) = 
      registerService.registerWorker(request)
    override def sampling(request: master.MasterService.SampleData) = 
      samplingService.sampling(request)
    override def reportSyncCompletion(request: master.MasterService.SyncPhaseReport) = 
      syncAndShuffleService.reportSyncCompletion(request)
    override def reportFinalMergeCompletion(request: master.MasterService.FinalMergePhaseReport) = 
      finalMergeService.reportFinalMergeCompletion(request)
  }, ec)

  val server = ServerBuilder
    .forPort(0)
    .addService(serviceDefinition)
    .build()

  server.start()

  val port = server.getPort
  println(s"$ip:$port")

  Await.result(MasterState.awaitShutdown, Duration.Inf)
  
  println("Shutdown signal received. Initiating graceful shutdown...")
  server.shutdown()
  server.awaitTermination()
  
  // Cleanup after server termination
  ConnectionManager.shutdownAllChannels()
  println("Master shutdown complete.")
}