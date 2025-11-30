import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import utils.{WorkerOptionUtils, PathUtils}
import server.{WorkerServiceImpl}
import global.WorkerState
import worker.WorkerService.WorkerServiceGrpc
import common.utils.SystemUtils
import global.ConnectionManager
import main.{RegisterManager, SampleManager, MemorySortManager, FileMergeManager, LabelingManager, SynchronizationManager}
import scala.async.Async.{async, await}
import java.nio.file.Files

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }
  
  val (masterIp, masterPort) = {
    val parts = masterAddr.split(":")
    (parts(0), parts(1).toInt)
  }

  val invalidInputDirs = inputDirs.filter{ dir => !PathUtils.exists(dir) || !PathUtils.isDirectory(dir) }

  if (invalidInputDirs.nonEmpty) {
    invalidInputDirs.foreach { dir =>
      Console.err.println(s"Input directory does not exist or is not a directory: $dir(${PathUtils.toAbsolutePath(dir)})")
    }
    sys.exit(1)
  }

  PathUtils.createDirectoryIfNotExists(outputDir)

  WorkerState.setMasterAddr(masterIp, masterPort)
  WorkerState.setInputDirs(inputDirs)
  WorkerState.setOutputDir(outputDir)

  val server = ServerBuilder
    .forPort(0)
    .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(), ec))
    .build()

  server.start()

  val mainWaiting = async {
    ConnectionManager.initMasterChannel(masterIp, masterPort)

    new RegisterManager().start(server.getPort)

    new SampleManager().start()

    val files = await { new MemorySortManager(inputDirs, outputDir).start }

    val (_, sortedFiles) = await {
      WorkerState.waitForAssignment.zip(
        new FileMergeManager(files, outputDir).start
      )
    }

    val assignedRange = WorkerState.getAssignedRange.getOrElse(throw new RuntimeException("Assigned range is not available"))

    val labeledFiles = await { new LabelingManager(sortedFiles, assignedRange, outputDir).start }

    await { new SynchronizationManager(labeledFiles).start() }

    ConnectionManager.shutdownAllChannels()

    server.awaitTermination()
  }

  Await.result(mainWaiting, Duration.Inf)
}
