package utils

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption, StandardCopyOption}
import java.nio.channels.FileChannel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}
import scala.annotation.tailrec

import com.google.protobuf.ByteString

import common.data.Data.{Record, KEY_SIZE, VALUE_SIZE, RECORD_SIZE}
import common.utils.SystemUtils

object FileManager {
  case class InputSubDir(val value: String)
  case class OutputSubDir(val value: String)

  val memSortDirName = "sorted"
  val fileMergeDirName = "merged"
  val labelingDirName = "labeled"
  val shuffleDirName = "shuffled"
  val finalDirName = "final"
  private val inputSubDirNames = Set(memSortDirName, fileMergeDirName, labelingDirName, shuffleDirName, finalDirName)
                                  .map(InputSubDir(_))
  private val outputSubDirNames = Set(memSortDirName, fileMergeDirName, labelingDirName, shuffleDirName, finalDirName)
                                  .map(OutputSubDir(_))

  private var inputDirs: Seq[String] = Seq.empty
  private var outputDir: Option[String] = None

  def setInputDirs(dirs: Seq[String]) = this.synchronized {
    inputDirs = dirs
  }

  def setOutputDir(dir: String) = this.synchronized {
    outputDir = Some(dir)
  }

  def getInputDirs = inputDirs

  def getOutputDir = outputDir

  def getInputFilePaths: Seq[String] = {
    if (inputDirs.isEmpty) throw new RuntimeException("Input directories are not set")
    else inputDirs.flatMap { dirPath =>
      Using(Files.list(Paths.get(dirPath))) { stream =>
        stream.iterator.asScala.map(_.toString).filter(path => Files.isRegularFile(Paths.get(path))).toSeq
      }.getOrElse(Seq.empty)
    }
  }

  def getFilePathFromInputDir(filename: String)(implicit inputSubDir: InputSubDir): String = {
    require { inputSubDirNames.contains(inputSubDir) }

    outputDir match {
      case Some(outputDirectory) => Paths.get(outputDirectory, inputSubDir.value, filename).toString
      case None => throw new RuntimeException("Output directory are not set")
    }
  }

  def getFilePathFromInputDirAll(filenames: Seq[String])(implicit inputSubDir: InputSubDir): List[String] = {
    require { inputSubDirNames.contains(inputSubDir) }

    filenames.map(getFilePathFromInputDir).toList
  }

  def getFilePathFromOutputDir(filename: String)(implicit outputSubDir: OutputSubDir): String = {
    require { outputSubDirNames.contains(outputSubDir) }

    outputDir match {
      case Some(outputDirectory) => Paths.get(outputDirectory, outputSubDir.value, filename).toString
      case None => throw new RuntimeException("Output directory are not set")
    }
  }

  def getFilePathFromOutputDirAll(filenames: Seq[String])(implicit outputSubDir: OutputSubDir): List[String] = {
    require { outputSubDirNames.contains(outputSubDir) }

    filenames.map(getFilePathFromOutputDir).toList
  }

  def getRandomFilename: String = UUID.randomUUID().toString
  
  def createDirectoryIfNotExists(dirPath: String): Unit = Files.createDirectories(Paths.get(dirPath))

  def getFilesize(filePath: String): Long = Files.size(Paths.get(filePath))

  /**
   * Read records from file starting at offset
   */
  def readRecords(filePath: String, offset: Long, count: Int): Array[Record] = {
    val file = Paths.get(filePath)

    Using(FileChannel.open(file, StandardOpenOption.READ)) { channel =>
      val records = Array.ofDim[Record](count)
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      
      var position = offset * RECORD_SIZE
      var i = 0
      while (i < count) {
        keyBuffer.clear()
        valueBuffer.clear()
        
        val keyBytesRead = channel.read(keyBuffer, position)
        val valueBytesRead = channel.read(valueBuffer, position + KEY_SIZE)
        
        if (keyBytesRead != KEY_SIZE || valueBytesRead != VALUE_SIZE) {
          throw new RuntimeException(s"Incomplete read at position $position in $filePath (key: $keyBytesRead/$KEY_SIZE, value: $valueBytesRead/$VALUE_SIZE)")
        }
        
        keyBuffer.flip()
        valueBuffer.flip()
        
        records(i) = (ByteString.copyFrom(keyBuffer), ByteString.copyFrom(valueBuffer))
        position += RECORD_SIZE
        i += 1
      }
      
      records
    }.get
  }

  /**
   * Write records to file
   */
  def writeRecords(filePath: String, records: Array[Record]): Unit = {
    Using(FileChannel.open(
      Paths.get(filePath),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )) { channel =>
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      
      var i = 0
      while (i < records.length) {
        val (key, value) = records(i)
        
        keyBuffer.clear()
        keyBuffer.put(key.toByteArray)
        keyBuffer.flip()
        while (keyBuffer.hasRemaining) {
          channel.write(keyBuffer)
        }
        
        valueBuffer.clear()
        valueBuffer.put(value.toByteArray)
        valueBuffer.flip()
        while (valueBuffer.hasRemaining) {
          channel.write(valueBuffer)
        }
        
        i += 1
      }
    }.get
  }

  def move(oldFilePath: String, newFilePath: String): Unit = Files.move(Paths.get(oldFilePath), Paths.get(newFilePath), StandardCopyOption.ATOMIC_MOVE)

  def delete(filePath: String): Unit = Files.deleteIfExists(Paths.get(filePath))

  def deleteAll(filePaths: Seq[String]): Unit = {
    filePaths.foreach { filePath =>
      delete(filePath)
    }
  }
}
