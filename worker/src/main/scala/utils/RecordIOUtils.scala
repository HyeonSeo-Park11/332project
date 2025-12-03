package utils

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import com.google.protobuf.ByteString
import common.utils.SystemUtils
import common.data.Data.{Record, KEY_SIZE, VALUE_SIZE, RECORD_SIZE}

object RecordIOUtils {
  /**
   * Read records from file starting at offset
   */
  def readRecords(filePath: String, offset: Long, count: Int): Array[Record] = {
    val file = Paths.get(filePath)
    val channel = FileChannel.open(file, StandardOpenOption.READ)
    
    try {
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
    } finally {
      channel.close()
    }
  }

  /**
   * Read all records from file
   */
  def readAllRecords(filePath: String): Array[Record] = {
    val fileSize = Files.size(Paths.get(filePath))
    val numRecords = (fileSize / RECORD_SIZE).toInt
    readRecords(filePath, 0, numRecords)
  }

  /**
   * Write records to file
   */
  def writeRecords(filePath: String, records: Array[Record]): Unit = {
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
