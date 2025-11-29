package main

import common.utils.SystemUtils
import global.{ConnectionManager, WorkerState}
import master.MasterService.{MasterServiceGrpc, SyncPhaseReport}
import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{FileListMessage, FileMetadata, WorkerServiceGrpc}

class SynchronizationManager(labeledFiles: Map[(String, Int), List[String]])(implicit ec: ExecutionContext) {
  WorkerState.setAssignedFiles(labeledFiles)
  private val masterStub = MasterServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def start(): Future[Unit] = {
    val selfIp = SystemUtils.getLocalIp.getOrElse(
      throw new IllegalStateException("[Sync] Failed to determine local IP. Abort synchronization.")
    )

    async {
      val outgoingPlans = getOutgoingPlans(selfIp)
      await(transmitPlans(outgoingPlans, selfIp))
      await(notifyMasterOfCompletion(selfIp))
      await(WorkerState.waitForShuffleCommand)

      println("[Sync] Master authorized shuffle phase. Ready for file transfers.")
    }
  }

  /*
  Consume worker-provided assignments and drop entries that point back to the current worker or are empty.
  */
  private def getOutgoingPlans(selfIp: String): Map[(String, Int), Seq[String]] = {
    WorkerState.getAssignedFiles.collect {
        case (endpoint, files) if endpoint._1 != selfIp && files.nonEmpty =>
          endpoint -> files.toSeq
      }
  }

  /*
  Transmit destination-specific transfer plans to actual gRPC calls.
  For each worker, send the file metadata list via gRPC.
  Await all transmissions to complete before returning.
  */
  private def transmitPlans(plans: Map[(String, Int), Seq[String]],selfIp: String): Future[Unit] = {
    if (plans.isEmpty) {
      println("[Sync] No outgoing files to report.")
      Future.successful(())
    } 
    else {
      val sendFutures = plans.toSeq.map { case ((ip, port), files) =>
        val metadata = files.map(fileName => FileMetadata(fileName = fileName))
        val fileNames = files.mkString(", ")
        println(s"[Sync][SendList] $selfIp -> $ip:$port files: [$fileNames]")

        deliverFileList(ip, selfIp, metadata).map { success =>
            if (success) {
              println(s"[Sync] Delivered ${files.size} file descriptors to $ip:$port")
            } else {
              println(s"[Sync] Failed to deliver file descriptors to $ip:$port")
            }
          }.recover { case e =>
            println(s"[Sync] Error delivering file descriptors to $ip:$port: ${e.getMessage}")
          }
      }

      Future.sequence(sendFutures).map(_ => ())
    }
  }

  private def deliverFileList(targetIp: String,senderIp: String,files: Seq[FileMetadata]): Future[Boolean] = {
    val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(targetIp))
    val request = FileListMessage(
      senderIp = senderIp,
      files = files
    )

    stub.deliverFileList(request).map(_.success).recover {
      case e: Exception =>
        println(s"[Sync] Error delivering file list to $targetIp: ${e.getMessage}")
        false
    }
  }

  private def notifyMasterOfCompletion(workerIp: String): Future[Unit] = async {
    WorkerState.getMasterAddr match {
      case Some(_) =>
        val request = SyncPhaseReport(workerIp = workerIp)
        val success = await(masterStub.reportSyncCompletion(request)).success
        if (success) {
          println("[Sync] Synchronization completed. Waiting for master's shuffle command...")
        } else {
          println("[Sync] Master rejected synchronization completion report.")
        }

      case None =>
        throw new IllegalStateException("[Sync] Master address is unknown. Unable to report synchronization completion.")
    }
  }
}
