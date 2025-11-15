package worker.sync

import master.MasterClient
import worker.Worker
import worker.WorkerService.FileMetadata
import common.utils.SystemUtils
import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}

object SynchronizationManager {
  def runSyncPhase()(implicit ec: ExecutionContext): Future[Unit] = {
    val selfIp = SystemUtils.getLocalIp.getOrElse(
      throw new IllegalStateException("[Sync] Failed to determine local IP. Abort synchronization.")
    )

    async {
      val outgoingPlans = getOutgoingPlans(selfIp)
      await(transmitPlans(outgoingPlans, selfIp))
      await(notifyMasterOfCompletion(selfIp))
      await(Worker.waitForShuffleCommand)

      println("[Sync] Master authorized shuffle phase. Ready for file transfers.")
    }
  }

  /*
  Consume worker-provided assignments and drop entries that point back to the current worker or are empty.
  */
  private def getOutgoingPlans(selfIp: String): Map[(String, Int), Seq[String]] = {
    Worker.getAssignedFiles.collect {
        case (endpoint, files) if endpoint._1 != selfIp && files.nonEmpty =>
          endpoint -> files.toSeq
      }
  }

  /*
  Transmit destination-specific transfer plans to actual gRPC calls.
  For each worker, create a PeerWorkerClient and send the file metadata list.
  Await all transmissions to complete before returning.
  */
  private def transmitPlans(
    plans: Map[(String, Int), Seq[String]],
    selfIp: String
  )(implicit ec: ExecutionContext): Future[Unit] = {
    if (plans.isEmpty) {
      println("[Sync] No outgoing files to report.")
      Future.successful(())
    } 
    else {
      val sendFutures = plans.toSeq.map { case ((ip, port), files) =>
        val client = new PeerWorkerClient(ip, port)
        val metadata = files.map(fileName => FileMetadata(fileName = fileName))
        val fileNames = files.mkString(", ")
        println(s"[Sync][SendList] $selfIp -> $ip:$port files: [$fileNames]")

        client.deliverFileList(selfIp, metadata).map { success =>
            if (success) {
              println(s"[Sync] Delivered ${files.size} file descriptors to $ip:$port")
            } else {
              println(s"[Sync] Failed to deliver file descriptors to $ip:$port")
            }
            client.shutdown()
          }.recover { case e =>
            println(s"[Sync] Error delivering file descriptors to $ip:$port: ${e.getMessage}")
            client.shutdown()
          }
      }

      Future.sequence(sendFutures).map(_ => ())
    }
  }

  private def notifyMasterOfCompletion(workerIp: String)(implicit ec: ExecutionContext): Future[Unit] = async {
    Worker.getMasterAddr match {
      case Some((masterIp, masterPort)) =>
        val client = new MasterClient(masterIp, masterPort)
        val responseFuture = client.reportSyncCompletion(workerIp)
        
        // Ensure shutdown is called when the future completes (success or failure)
        responseFuture.onComplete(_ => client.shutdown())

        val success = await(responseFuture)
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
