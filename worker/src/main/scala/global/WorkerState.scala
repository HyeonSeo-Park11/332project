package global

import com.google.protobuf.ByteString
import scala.concurrent.{Future, Promise}

// Worker Singleton
object WorkerState {
  val memSortDirName = "sorted"
  val fileMergeDirName = "merged"

  private var masterIp: Option[String] = None
  private var masterPort: Option[Int] = None
  private var inputDirs: Seq[String] = Nil
  private var outputDir: Option[String] = None
  private var assignedRange: Option[Map[(String, Int), (ByteString, ByteString)]] = None
  private val assignPromise = Promise[Unit]()

  def setMasterAddr(ip: String, port: Int): Unit = this.synchronized {
    masterIp = Some(ip)
    masterPort = Some(port)
  }

  def getMasterAddr: Option[(String, Int)] = this.synchronized {
    for {
      ip <- masterIp
      port <- masterPort
    } yield (ip, port)
  }

  def setInputDirs(dirs: Seq[String]): Unit = this.synchronized {
    inputDirs = dirs
  }

  def getInputDirs: Seq[String] = this.synchronized {
    inputDirs
  }

  def setOutputDir(dir: String): Unit = this.synchronized {
    outputDir = Some(dir)
  }

  def getOutputDir: Option[String] = this.synchronized {
    outputDir
  }

  def setAssignedRange(assignments: Map[(String, Int), (ByteString, ByteString)]): Unit = this.synchronized {
    assignedRange = Some(assignments)
    assignPromise.trySuccess(())
  }

  def getAssignedRange: Option[Map[(String, Int), (ByteString, ByteString)]] = this.synchronized {
    assignedRange
  }

  def waitForAssignment(): Future[Unit] = {
    assignPromise.future
  }
}
