package main

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import global.WorkerState
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import global.ConnectionManager
import utils.PathUtils

class ShuffleManager(implicit ec: ExecutionContext) {
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
                await { processFile(workerIp, head) }
                await { processFilesSequentially(workerIp, tail) }
            }
        }
    }

    private def processFile(workerIp: String, filename: String): Future[Unit] = async {
        println(s"[$workerIp, $filename] SEND")
        val stub = ShuffleGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
        val bytes: DownloadResponse = await { stub.downloadFile(DownloadRequest(filename = filename)) }
        val targetPath = Paths.get(s"${WorkerState.getOutputDir.get}/${WorkerState.shuffleDirName}/$filename")
        val _ = blocking { Files.write(targetPath, bytes.data.toByteArray, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING) }
        println(s"[$workerIp, $filename] RECEIVE")
    }

}