package global

import java.io._
import global.WorkerState
import utils.FileManager
import scala.util.Using

object StateRestoreManager {
    val stateFileName: String = "worker_state"
    implicit val outputSubDir: FileManager.OutputSubDir = FileManager.OutputSubDir(FileManager.stateRestoreDirName)

    def isClean(): Boolean = {
        !new File(FileManager.getFilePathFromOutputDir(stateFileName)).exists()
    }

    def storeState(): Unit = {
        FileManager.createDirectoryIfNotExists(FileManager.getFilePathFromOutputDir(""))

        Using(new ObjectOutputStream(new FileOutputStream(FileManager.getFilePathFromOutputDir(stateFileName)))) { oos =>
            val instance = WorkerState.synchronized { WorkerState.instance }
            oos.writeObject(instance)
        }.get
    }

    def restoreState() = {
        assert(!isClean())

        Using(new ObjectInputStream(new FileInputStream(FileManager.getFilePathFromOutputDir(stateFileName)))) { ois =>
            val instance = ois.readObject().asInstanceOf[WorkerState]
            WorkerState.synchronized { WorkerState.instance = instance }
        }.get
    }

    def clear(): Unit = {
        FileManager.delete(FileManager.getFilePathFromOutputDir(stateFileName))
    }
}