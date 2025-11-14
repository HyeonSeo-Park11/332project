package main

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking, Promise}
import global.WorkerState
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import global.ConnectionManager
import utils.PathUtils
import io.grpc.stub.StreamObserver
import java.nio.channels.FileChannel

class ShuffleManager(implicit ec: ExecutionContext) {
    val maxTries = 10

	def start(shufflePlans: Map[String, Seq[String]]): Future[Map[String, Seq[String]]] = async {  // TODO: make input as optional, if none, restore
        PathUtils.createDirectoryIfNotExists(s"${WorkerState.getOutputDir.get}/${WorkerState.shuffleDirName}")
        val workerFutures = shufflePlans.map {
            case (workerIp, fileList) => processFilesSequentially(workerIp, fileList)
        }
        await { Future.sequence(workerFutures) }
        shufflePlans
    }

    private def processFilesSequentially(workerIp: String, fileList: Seq[String]): Future[Unit] = async {
        fileList match {
            case Nil => ()
            case head :: tail => {
                await { processFileWithRetry(workerIp, head) }
                await { processFilesSequentially(workerIp, tail) }
            }
        }
    }

    private def processFile(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        val promise = Promise[Unit]()

        val stub = ShuffleGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
        val targetPath = Paths.get(s"${WorkerState.getOutputDir.get}/${WorkerState.shuffleDirName}/$filename")
        val fileChannel: FileChannel = FileChannel.open(
            targetPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
        
        println(s"[$workerIp, $filename] request")
        stub.downloadFile(DownloadRequest(filename = filename), new StreamObserver[DownloadResponse] {
            override def onNext(response: DownloadResponse): Unit = {
                blocking {
                        val writeBuffer = response.data.asReadOnlyByteBuffer()
                        while (writeBuffer.hasRemaining) {
                            fileChannel.write(writeBuffer)
                        }
                    }
            }

            override def onError(t: Throwable): Unit = promise.failure(t)
            
            override def onCompleted(): Unit = {
                println(s"[$workerIp, $filename] response completed")
                fileChannel.close()
                promise.success(())
            }
        })

        promise.future
    }

    private def processFileWithRetry(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        processFile(workerIp, filename).recoverWith {
            case _ if tries < maxTries => {  // 방금 시도한게 n번째 시도이면 더이상 시도하지 않음
                println(s"Retrying [$workerIp, $filename], attempt #$tries")
                blocking { Thread.sleep(math.pow(2, tries).toLong * 1000) }
                processFileWithRetry(workerIp, filename, tries + 1)
            }
        }
    }
}