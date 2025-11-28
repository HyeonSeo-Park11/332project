package main

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import common.utils.SystemUtils
import utils.{PathUtils, RecordIOUtils}
import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import utils.PathUtils
import scala.async.Async.async
import common.data.Data.{Record, getRecordOrdering, RECORD_SIZE, KEY_SIZE, VALUE_SIZE}
import global.WorkerState

class MemorySortManager(inputDirs: Seq[String], outputDir: String) {
  val threadPool = Executors.newFixedThreadPool(RecordIOUtils.getThreadCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  def start = {
    val sortedDir = Paths.get(outputDir, WorkerState.memSortDirName).toString
    PathUtils.createDirectoryIfNotExists(sortedDir)
    
    inMemorySort(sortedDir, RecordIOUtils.getChunkSize)
  }
  /**
   * In-memory sort of all input files
   * Split files into chunks if they exceed chunk size
   */
  private def inMemorySort(sortedDir: String, chunkSize: Long): Future[List[String]] = {
    // Collect all input files
    val allFiles = inputDirs.flatMap { dirPath =>
      PathUtils.getFilesList(dirPath).filter { filePath =>
        Files.isRegularFile(Paths.get(filePath)) && Files.size(Paths.get(filePath)) >= RECORD_SIZE
      }
    }
    
    println(s"[MergeSort] Found ${allFiles.size} input files to sort")
    
    val threadCount = RecordIOUtils.getThreadCount
    println(s"[MergeSort] Using $threadCount threads for sorting (max concurrent files: ${RecordIOUtils.getMaxConcurrentFiles} by ${SystemUtils.getRamMb} MB RAM)")
    
    val sortedFiles = new ConcurrentLinkedQueue[String]()
    
    val fileId = new AtomicInteger(0)
    val futures = allFiles.map { filePath =>
      async {
        val threadId = Thread.currentThread().getName
        val file = Paths.get(filePath)
        val fileSize = Files.size(file)
        val numRecords = fileSize / RECORD_SIZE
        
        println(s"[MergeSort-InMemory][$threadId] Processing file: $filePath (${numRecords} records)")
        
        // Process chunks recursively
        @tailrec
        def processChunks(offset: Long, chunkCount: Int): Unit = {
          if (offset < numRecords) {
            val recordsToRead = Math.min(chunkSize, numRecords - offset).toInt
            val currentChunk = chunkCount + 1
            
            println(s"[MergeSort-InMemory][$threadId] Reading chunk $currentChunk: $recordsToRead records from offset $offset")
            val records = RecordIOUtils.readRecords(filePath, offset, recordsToRead)
            
            println(s"[MergeSort-InMemory][$threadId] Sorting chunk $currentChunk...")
            implicit val ordering = getRecordOrdering
            val sortedRecords = records.sorted
            
            val outputPath = s"$sortedDir/${fileId.incrementAndGet()}.bin"
            println(s"[MergeSort-InMemory][$threadId] Writing sorted chunk to: $outputPath")
            RecordIOUtils.writeRecords(outputPath, sortedRecords)
            sortedFiles.add(outputPath)
            
            processChunks(offset + recordsToRead, currentChunk)
          } else {
            println(s"[MergeSort-InMemory][$threadId] ✓ Completed file: $filePath ($chunkCount chunks)")
          }
        }
        
        processChunks(0L, 0)
      }
    }

    Future.sequence(futures)
    .map {
      case _ => 
        threadPool.shutdown()
        sortedFiles.asScala.toList.sortBy { path =>
          val name = Paths.get(path).getFileName.toString
          name.stripSuffix(".bin").toInt
        }
    }
  }
}
