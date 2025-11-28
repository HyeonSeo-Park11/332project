package main

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import global.WorkerState
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import global.ConnectionManager

class ShuffleManager(implicit ec: ExecutionContext) {
    private def processFile(workerIp: String, filename: String): Future[Unit] = async {
        println(s"[$workerIp, $filename] SEND")
        val stub = ShuffleGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
        val bytes: DownloadResponse = await { stub.downloadFile(DownloadRequest(filename = filename)) }
        val targetPath = Paths.get(s"${WorkerState.getOutputDir.get}/${WorkerState.shuffleDirName}/$filename")
        val _ = blocking { Files.write(targetPath, bytes.data.toByteArray, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING) }
        println(s"[$workerIp, $filename] RECEIVE")
    }

}