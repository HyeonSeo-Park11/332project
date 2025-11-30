package main

import java.nio.file.{Files, Paths, StandardOpenOption, StandardCopyOption}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import com.google.protobuf.ByteString
import common.utils.SystemUtils
import utils.RecordIOUtils
import scala.annotation.tailrec
import utils.PathUtils
import scala.concurrent.{ExecutionContext, Future}
import scala.async.Async.async
import common.data.Data.{Key, Record, getRecordOrdering, getKeyOrdering, RECORD_SIZE, KEY_SIZE, VALUE_SIZE}
import global.WorkerState

class LabelingManager(sortedFiles: List[String], assignedRange: Map[(String, Int), Record], outputDir: String)(implicit ec: ExecutionContext) {
  def start = {
    val labelingDir = Paths.get(outputDir, WorkerState.labelingDirName).toString
    PathUtils.createDirectoryIfNotExists(labelingDir)

    assignFilesToWorkers(labelingDir)
  }

  /**
   * Assign sorted files to workers based on assigned ranges
   * 
   * @param sortedFiles List of sorted file paths in order
   * @param assignedRange Map of (workerIp, workerPort) -> (startKey, endKey)
   * @param outputDir Directory to write assigned files
   * @return Map of (workerIp, workerPort) -> List[filePath]
   */
  private def assignFilesToWorkers(labelingDir: String): Future[Map[(String, Int), List[String]]] = async {
    println(s"[FileAssignment] Starting file assignment with ${sortedFiles.size} sorted files")
    
    // Sort workers by start key
    implicit val cp = getRecordOrdering
    val sortedWorkers = assignedRange.toList.sorted
    
    println(s"[FileAssignment] Worker ranges (sorted):")
    sortedWorkers.foreach { case ((ip, port), (start, end)) =>
      println(s"  $ip:$port -> [${new java.math.BigInteger(1, start.toByteArray()).toString(16)}, ${new java.math.BigInteger(1, end.toByteArray()).toString(16)})")
    }
    
    // Get file metadata (filename, startKey, endKey) in sorted order
    val fileMetadata = sortedFiles.map { filePath =>
      val (firstKey, lastKey) = getFirstAndLastKeyFromFile(filePath)
      (filePath, firstKey, lastKey)
    }
    
    val from = SystemUtils.getLocalIp.getOrElse(throw new IllegalStateException("Could not determine local IP address"))
    val comparator = getKeyOrdering
    
    @tailrec
    def processFiles(
      workerId: (String, Int),
      rangeStart: Key,
      rangeEnd: Key,
      files: List[(String, Key, Key)],
      assignments: Map[(String, Int), List[String]]
    ): (List[(String, Key, Key)], Map[(String, Int), List[String]]) = {
      files match {
        case Nil => (Nil, assignments)
        case (currentFile @ (filePath, fileStartKey, fileEndKey)) :: restFiles =>
          val (workerIp, _) = workerId
          println(s"[FileAssignment]   Checking file: $filePath")
          
          // Check if file's end key is within [rangeStart, rangeEnd)
          val fileEndInRange = comparator.compare(fileEndKey, rangeStart) >= 0 && 
                               comparator.compare(fileEndKey, rangeEnd) < 0
          
          if (fileEndInRange) {
            // Entire file belongs to this worker - just rename
            val fileNum = assignments.getOrElse(workerId, List.empty).size
            val newFileName = s"$from-$workerIp-$fileNum"
            val newFilePath = Paths.get(labelingDir, newFileName)
            
            Files.move(Paths.get(filePath), newFilePath, StandardCopyOption.REPLACE_EXISTING)
            println(s"[FileAssignment]   ✓ Renamed entire file to: $newFileName")
            
            val newAssignments = assignments.updated(workerId, newFileName :: assignments.getOrElse(workerId, List.empty))
            
            processFiles(workerId, rangeStart, rangeEnd, restFiles, newAssignments)
            
          } else {
            // File's end key is beyond this worker's range - need to split
            // Check if rangeEnd falls within this file
            val rangeEndInFile = comparator.compare(rangeEnd, fileStartKey) > 0 && 
                                 comparator.compare(rangeEnd, fileEndKey) <= 0
            
            if (rangeEndInFile) {
              // Split file at rangeEnd
              println(s"[FileAssignment]   Splitting file at rangeEnd...")
              
              // Load file into memory and find split point
              val records = RecordIOUtils.readAllRecords(filePath)
              val splitIndex = findSplitIndex(records, rangeEnd)
              
              // Split into two parts
              val part1Records = records.take(splitIndex)
              val part2Records = records.drop(splitIndex)
              
              // Part 1: belongs to current worker
              val fileNum = assignments.getOrElse(workerId, List.empty).size
              val part1FileName = s"$from-$workerIp-$fileNum"
              val part1FilePath = Paths.get(labelingDir, part1FileName)
              RecordIOUtils.writeRecords(part1FilePath.toString, part1Records)
              println(s"[FileAssignment]   ✓ Created part1: $part1FileName (${part1Records.length} records)")
              
              val newAssignments = assignments.updated(workerId, part1FileName :: assignments.getOrElse(workerId, List.empty))
              
              // Part 2: push back to front of deque for next worker
              val tempFileName = Files.createTempFile(Paths.get(labelingDir), "temp-", ".part").getFileName.toString
              val part2FilePath = Paths.get(labelingDir, tempFileName)
              RecordIOUtils.writeRecords(part2FilePath.toString, part2Records)
              
              val part2StartKey = part2Records.head._1
              val part2EndKey = part2Records.last._1
              println(s"[FileAssignment]   ✓ Created part2: $tempFileName (${part2Records.length} records) - pushed to front")
              
              // Delete original file
              Files.deleteIfExists(Paths.get(filePath))
              
              // Return remaining files with part2 prepended, and stop processing for this worker
              ((part2FilePath.toString, part2StartKey, part2EndKey) :: restFiles, newAssignments)
              
            } else {
              // File is completely beyond this worker's range
              println(s"[FileAssignment]   File is beyond worker's range - pushed back")
              (files, assignments)
            }
          }
      }
    }

    val finalAssignments = sortedWorkers.foldLeft((fileMetadata, Map.empty[(String, Int), List[String]])) {
      case ((files, assignments), (workerId, (rangeStart, rangeEnd))) =>
        val (remainingFiles, newAssignments) = processFiles(workerId, rangeStart, rangeEnd, files, assignments)
        println(s"[FileAssignment] Processing worker ${workerId._1}")
        (remainingFiles, newAssignments)
    }
    val result = finalAssignments._2.map { case (k, v) => k -> v.reverse }
    
    println(s"[FileAssignment] Assignment complete:")
    result.foreach { case ((ip, port), files) =>
      println(s"  $ip:$port -> ${files.size} files")
    }
    
    result
  }
  
  /**
   * Find the first index where key >= splitKey
   */
  private def findSplitIndex(
    records: Array[Record],
    splitKey: Key
  ): Int = {
    val comparator = getKeyOrdering
    var left = 0
    var right = records.length
    
    while (left < right) {
      val mid = left + (right - left) / 2
      if (comparator.compare(records(mid)._1, splitKey) < 0) {
        left = mid + 1
      } else {
        right = mid
      }
    }
    
    left
  }
  
  // Removed loadFileRecords and writeRecordsToFile as they are now in RecordIOUtils

  /**
   * Get first and last key from a file
   */
  private def getFirstAndLastKeyFromFile(filePath: String): Record = {
    val path = Paths.get(filePath)
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try {
      val buffer = ByteBuffer.allocate(KEY_SIZE)
      
      // Read first record
      channel.read(buffer)
      buffer.flip()
      val firstKeyBytes = new Array[Byte](KEY_SIZE)
      buffer.get(firstKeyBytes)
      val firstKey = ByteString.copyFrom(firstKeyBytes)
      
      // Read last record
      val fileSize = Files.size(path)
      channel.position(fileSize - RECORD_SIZE)
      buffer.clear()
      channel.read(buffer)
      buffer.flip()
      val lastKeyBytes = new Array[Byte](KEY_SIZE)
      buffer.get(lastKeyBytes)
      val lastKey = ByteString.copyFrom(lastKeyBytes)
      
      (firstKey, lastKey)
    } finally {
      channel.close()
    }
  }
}
