package main

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.async.Async.{async, await}
import utils.{PathUtils, RecordIOUtils}
import scala.annotation.tailrec
import common.data.Data.{Record, getRecordOrdering, RECORD_SIZE}
import java.util.NoSuchElementException
import global.WorkerState

class FileMergeManager(files: List[String], outputDir: String) {
  files.foreach { file =>
    println(s"[FileMergeManager] Input file: $file")
  }
  
  val threadPool = Executors.newFixedThreadPool(RecordIOUtils.getThreadCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  def start(subDirName: String) = {
    val mergeDir = Paths.get(outputDir, subDirName).toString
    PathUtils.createDirectoryIfNotExists(mergeDir)

    twoWayMergeSort(mergeDir)
  }

  /**
   * 2-way merge sort
   * 
   * Merge pairs of file lists until all files are sorted in order
   * 
   * Example with 4 initial files:
   * Round 0: [[1],[2],[3],[4]]
   * Round 1: merge [1] and [3] -> [a,b], merge [2] and [4] -> [c,d]
   *          Result: [[a,b], [c,d]]
   * Round 2: merge [a,b] and [c,d] sequentially:
   *          - Start with files a and c
   *          - If a is exhausted, continue with b and c
   *          - If c is exhausted, continue with a (or b) and d
   *          - Continue until all files are merged
   *          Result: [[e,f,g,h]]
   * After this round, files in [e,f,g,h] are guaranteed to be sorted in order
   */
  private def twoWayMergeSort(mergeDir: String): Future[List[String]] = async {
    // Initialize: each file is its own list
    val fileLists: List[List[String]] = files.map(f => List(f))
    val fileId = new AtomicInteger(fileLists.size + 1)
    
    // Calculate number of merge rounds needed
    val totalRounds = Math.ceil(Math.log(fileLists.size) / Math.log(2)).toInt
    println(s"[MergeSort] Total merge rounds needed: $totalRounds")

    val finalListsFuture = (1 to totalRounds).foldLeft(Future.successful(fileLists)) {
      (prevFuture, round) => async {
        val lists = await { prevFuture }
        
        println(s"[MergeSort] Starting merge round $round/$totalRounds with ${lists.size} file lists")
        
        // Pair up file lists
        // [[1],[2],[3],[4]] -> [([1],[3]), ([2],[4])]
        val firstHalf = lists.take(lists.size / 2)
        val secondHalf = lists.drop(lists.size / 2)
        val listPairs = firstHalf.zip(secondHalf)
        
        // Handle remaining list if odd number
        val remainingList = if (lists.size % 2 == 1) {
          Some(lists.last)
        } else {
          None
        }
        
        // For each pair of lists, merge all files sequentially
        val nextFileLists = new ConcurrentLinkedQueue[List[String]]()
        
        val futures = listPairs.toList.map { case (list1, list2) =>
          async {
            val threadId = Thread.currentThread().getName
            println(s"[MergeSort-Round$round][$threadId] Merging list pair: ${list1.size} files + ${list2.size} files")
            val outputFiles = mergeFileLists(list1, list2, mergeDir, fileId, threadId, round)
            
            nextFileLists.add(outputFiles)
            println(s"[MergeSort-Round$round][$threadId] Completed merge: ${outputFiles.size} output files")
          }
        }

        await { Future.sequence(futures) }
        
        remainingList.foreach { list =>
          // Rename files in the remaining list
          val renamedFiles = list.zipWithIndex.map { case (file, idx) =>
            val outputPath = s"$mergeDir/${round}-rem-${fileId.getAndIncrement()}.bin"
            Files.move(Paths.get(file), Paths.get(outputPath), 
              java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            outputPath
          }
          nextFileLists.add(renamedFiles)
          println(s"[MergeSort] Renamed ${list.size} remaining files")
        }
        
        nextFileLists.asScala.toList
      }
    }

    val finalLists = await { finalListsFuture }
    threadPool.shutdown()

    finalLists.flatten
  }

  private class MultiFileIterator(files: List[String]) extends Iterator[Record] {
    private var remainingFiles = files
    private var currentIter: Iterator[Record] = Iterator.empty

    private def loadIterator(filePath: String): BufferedIterator[Record] = {
      val numRecords = (Files.size(Paths.get(filePath)) / RECORD_SIZE).toInt
      println(s"[MergeSort-Round] Loading file: $filePath ($numRecords records)")
      RecordIOUtils.readRecords(filePath, 0, numRecords).iterator.buffered
    }

    @tailrec
    final override def hasNext: Boolean = {
      if (currentIter.hasNext) true
      else if (remainingFiles.isEmpty) false
      else {
        val nextFile = remainingFiles.head
        remainingFiles = remainingFiles.tail
        currentIter = loadIterator(nextFile)
        hasNext
      }
    }

    override def next(): Record = {
      if (hasNext) currentIter.next()
      else throw new NoSuchElementException("next on empty iterator")
    }
  }

  // TODO: threadId/round are only for logging, later deletes them
  /**
   * Merge two lists of sorted files in memory
   * Files are loaded entirely into memory and merged
   */
  private def mergeFileLists(
    list1: List[String], 
    list2: List[String], 
    mergeDir: String, 
    fileId: AtomicInteger,
    threadId: String,
    round: Int
  ): List[String] = {
    val comparator = getRecordOrdering
    
    // 1. Calculate average file size (in records)
    val allFiles = list1 ++ list2
    val totalRecords = allFiles.map(f => Files.size(Paths.get(f)) / RECORD_SIZE).sum
    val avgRecordsPerFile = Math.ceil(totalRecords.toDouble / allFiles.size).toInt
    val targetChunkSize = Math.max(avgRecordsPerFile, 1)
    
    println(s"[MergeSort-Round$round][$threadId] Merging ${list1.size} + ${list2.size} files, avg chunk size: $targetChunkSize records")

    val iter1 = new MultiFileIterator(list1).buffered
    val iter2 = new MultiFileIterator(list2).buffered

    @tailrec
    def mergeLoop(
      buffer: ArrayBuffer[Record],
      outputFiles: List[String]
    ): List[String] = {
      assert { buffer.size <= targetChunkSize }
      
      if (buffer.size == targetChunkSize) {
        val currentFileId = s"$round-$threadId-${fileId.getAndIncrement()}"
        val outputPath = s"$mergeDir/$currentFileId.bin"
        println(s"[MergeSort-Round$round][$threadId] Writing output file: $outputPath (${buffer.size} records)")
        RecordIOUtils.writeRecords(outputPath, buffer.toArray)
        buffer.clear()
        mergeLoop(buffer, outputPath :: outputFiles)
      } else if (iter1.hasNext && iter2.hasNext) {
        if (comparator.compare(iter1.head, iter2.head) <= 0) buffer += iter1.next()
        else buffer += iter2.next()

        mergeLoop(buffer, outputFiles)
      } else if (iter1.hasNext) {
        while (iter1.hasNext && buffer.size < targetChunkSize) buffer += iter1.next()

        mergeLoop(buffer, outputFiles)
      } else if (iter2.hasNext) {
        while (iter2.hasNext && buffer.size < targetChunkSize) buffer += iter2.next()
        
        mergeLoop(buffer, outputFiles)
      } else {
        if (buffer.nonEmpty) {
          val currentFileId = s"$round-$threadId-${fileId.getAndIncrement()}"
          val outputPath = s"$mergeDir/$currentFileId.bin"
          println(s"[MergeSort-Round$round][$threadId] Writing output file: $outputPath (${buffer.size} records)")
          RecordIOUtils.writeRecords(outputPath, buffer.toArray)
          (outputPath :: outputFiles).reverse
        } else {
          outputFiles.reverse
        }
      }
    }
    
    val result = mergeLoop(ArrayBuffer.empty, Nil)
    
    // Delete input files that were merged
    allFiles.foreach { file =>
      try {
        Files.deleteIfExists(Paths.get(file))
        println(s"[MergeSort-Round$round][$threadId] Deleted intermediate file: $file")
      } catch {
        case e: Exception => println(s"[MergeSort-Round$round][$threadId] Warning: Failed to delete $file: ${e.getMessage}")
      }
    }
    
    println(s"[MergeSort-Round$round][$threadId] Merge complete: ${result.size} output files")
    result
  }
}
