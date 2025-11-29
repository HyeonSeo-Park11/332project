package common.data

import java.nio.ByteBuffer
import com.google.protobuf.ByteString
import scala.math.Ordering

object Data {
  type Key = ByteString
  type Value = ByteString
  type Record = (Key, Value)

  val RECORD_SIZE = 100
  val KEY_SIZE = 10
  val VALUE_SIZE = 90

  val getRecordOrdering: Ordering[Record] = {
    new Ordering[Record] {
      private val comparator = ByteString.unsignedLexicographicalComparator

      override def compare(x: Record, y: Record): Int = {
        if (x._1.size() > y._1.size()) 1
        else if (x._1.size() < y._1.size()) -1
        else comparator.compare(x._1, y._1)
      }
    }
  }

  val getKeyOrdering: Ordering[Key] = {
    new Ordering[Key] {
      private val comparator = ByteString.unsignedLexicographicalComparator

      override def compare(x: Key, y: Key): Int = {
        if (x.size() > y.size()) 1
        else if (x.size() < y.size()) -1
        else comparator.compare(x, y)
      }
    }
  }
}
