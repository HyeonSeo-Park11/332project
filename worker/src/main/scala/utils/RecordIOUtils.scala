package utils

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import com.google.protobuf.ByteString
import common.utils.SystemUtils

object RecordIOUtils {
  val RECORD_SIZE = 100
  val KEY_SIZE = 10
  val VALUE_SIZE = 90
  val MAX_CHUNK_SIZE_MB = 32  // Upper bound: 32 MiB
  val MEMORY_OVERHEAD_FACTOR = 2 // (ByteString, ByteString) overhead, almost 2.2x memory usage, but 2x is enough since there are lots of optimizations

  /**
   * Calculate chunk size: at least 3 files should fit in RAM, with upper bound of 32MiB
   */
  def getChunkSize: Long = {
    val ramBytes = SystemUtils.getRamMb * 1024 * 1024
    val maxChunkBytes = MAX_CHUNK_SIZE_MB * 1024 * 1024
    // Ensure at least 3 chunks can fit in RAM (use 80% of RAM)
    val chunkBytesForRam = (ramBytes * 0.8 / (3 * MEMORY_OVERHEAD_FACTOR)).toLong
    val chunkBytes = Math.min(maxChunkBytes, chunkBytesForRam)
    chunkBytes / RECORD_SIZE // At least 1000 records
  }
  
  /**
   * Calculate how many files can be loaded in RAM simultaneously
   * Used to determine thread pool size
   */
  def getMaxConcurrentFiles: Int = {
    val ramBytes = SystemUtils.getRamMb * 1024 * 1024
    val chunkBytes = getChunkSize * RECORD_SIZE
    // we need 3 files per merge operation (2 input + 1 output)
    val maxFiles = (ramBytes * 0.8 / (chunkBytes * MEMORY_OVERHEAD_FACTOR)).toInt
    Math.max(3, maxFiles) // At least 3 files
  }
  
  /**
   * Calculate optimal thread count based on CPU cores and RAM constraints
   */
  def getThreadCount: Int = {
    val cpuThreads = SystemUtils.getProcessorNum * 2
    val maxConcurrentFiles = getMaxConcurrentFiles
    // Each merge task needs 3 files (2 input + 1 output)
    val ramBasedThreads = maxConcurrentFiles / 3
    Math.min(cpuThreads, ramBasedThreads)
  }

  /**
   * Read records from file starting at offset
   */
  def readRecords(filePath: String, offset: Long, count: Int): Array[(ByteString, ByteString)] = {
    val file = Paths.get(filePath)
    val channel = FileChannel.open(file, StandardOpenOption.READ)
    
    try {
      val records = Array.ofDim[(ByteString, ByteString)](count)
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
    } finally {
      channel.close()
    }
  }

  /**
   * Read all records from file
   */
  def readAllRecords(filePath: String): Array[(ByteString, ByteString)] = {
    val fileSize = Files.size(Paths.get(filePath))
    val numRecords = (fileSize / RECORD_SIZE).toInt
    readRecords(filePath, 0, numRecords)
  }

  /**
   * Write records to file
   */
  def writeRecords(filePath: String, records: Array[(ByteString, ByteString)]): Unit = {
    val channel = FileChannel.open(
      Paths.get(filePath),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    
    try {
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
    } finally {
      channel.close()
    }
  }
}
