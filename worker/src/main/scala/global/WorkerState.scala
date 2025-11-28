package global

import scala.concurrent.{Future, Promise}
import common.data.Data.Record
import com.google.protobuf.ByteString

// Worker Singleton
object WorkerState {
  val memSortDirName = "sorted"
  val fileMergeDirName = "merged"
  val labelingDirName = "labeled"
  val shuffleDirName = "shuffled"

  private var masterIp: Option[String] = None
  private var masterPort: Option[Int] = None
  private var inputDirs: Seq[String] = Nil
  private var outputDir: Option[String] = None
  private var assignedRange: Option[Map[(String, Int), Record]] = None
  private val assignPromise = Promise[Unit]()
  private var assignedFiles: Map[(String, Int), List[String]] = Map.empty
  private var shufflePlans = Map[String, Seq[String]]()
  private val shuffleStartPromise: Promise[Unit] = Promise[Unit]()

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

  def setAssignedRange(assignments: Map[(String, Int), Record]): Unit = this.synchronized {
    assignedRange = Some(assignments)
    assignPromise.trySuccess(())
  }

  def getAssignedRange: Option[Map[(String, Int), Record]] = this.synchronized {
    assignedRange
  }

  def waitForAssignment(): Future[Unit] = {
    assignPromise.future
  }
  
  def setAssignedFiles(files: Map[(String, Int), List[String]]): Unit = this.synchronized {
    assignedFiles = files
  }

  def getAssignedFiles: Map[(String, Int), List[String]] = this.synchronized {
    assignedFiles
  }

  def addShufflePlan(senderIp: String, files: Seq[String]): Unit = this.synchronized {
    val existing = shufflePlans.getOrElse(senderIp, Seq.empty)
    shufflePlans += senderIp -> (existing ++ files)
  }

  def getShufflePlans: Map[String, Seq[String]] = this.synchronized {
    shufflePlans
  }

  def markShuffleStarted(): Unit = this.synchronized {
    shuffleStartPromise.trySuccess(())
  }

  def waitForShuffleCommand: Future[Unit] = shuffleStartPromise.future

  def hasReceivedShuffleCommand: Boolean = shuffleStartPromise.isCompleted
}
