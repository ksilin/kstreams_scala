package com.example.time

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.api.{ Processor, ProcessorContext, Record }
import org.apache.kafka.streams.state.{ WindowStore, WindowStoreIterator }

import java.lang

object WindowStoreProcessors {

  case class SimpleWindowStoreProcessor(tableStoreName: String)
      extends Processor[String, MyRecord, String, MyRecord] {

    var store: WindowStore[String, MyRecord] = _

    override def init(context: ProcessorContext[String, MyRecord]): Unit =
      store = context.getStateStore(tableStoreName)

    override def process(record: Record[String, MyRecord]): Unit = {
      println(s"processing record: $record")

      val found: MyRecord = store.fetch(record.key(), record.timestamp())

      if (found != null) {
        println(s"! found existing records for key ${record.key()}: ${record.value()}, updating")
      } else {
        println(s"? no existing records for key ${record.key()} found: , storing ${record.value()}")
        store.put(record.key(), record.value(), record.timestamp())
      }
    }
  }

  case class SimpleLookbackRangeWindowStoreProcessor(tableStoreName: String, lookbackMs: Long)
      extends Processor[String, MyRecord, String, MyRecord] {

    var context: ProcessorContext[String, MyRecord] = _
    var store: WindowStore[String, MyRecord]        = _

    override def init(context: ProcessorContext[String, MyRecord]): Unit = {
      this.context = context
      store = context.getStateStore(tableStoreName)
    }

    override def process(record: Record[String, MyRecord]): Unit = {
      println(s"processing record: $record")

      val recordKey       = record.key()
      val recordTimestamp = record.timestamp()

      //val found: WindowStoreIterator[MyRecord] = store.fetch(record.key(), record.timestamp() - lookbackMs, record.timestamp())
      val found: WindowStoreIterator[MyRecord] =
        store.backwardFetch(recordKey, recordTimestamp - lookbackMs, recordTimestamp)

      var foundCount     = 0
      var last: MyRecord = null
      while (found.hasNext) {
        val f: KeyValue[lang.Long, MyRecord] = found.next()
        if (last == null) last = f.value
        foundCount = foundCount + 1
        println(
          s"found ${foundCount} record ${f} in range ${recordTimestamp - lookbackMs} - $recordTimestamp"
        )
      }

      val recordValue = record.value()
      if (foundCount > 0) {
        println(
          s"! found existing records for key $recordKey: $recordValue, updating with last: ${last}"
        )
        val updatedValue =
          recordValue.copy(description = last.description + "_" + recordValue.description)
        store.put(recordKey, updatedValue, recordTimestamp)
        context.forward(new Record(recordKey, updatedValue, recordTimestamp))
      } else {
        println(s"? no existing records for key $recordKey found: , storing $recordValue")
        store.put(recordKey, recordValue, recordTimestamp)
        context.forward(record)
      }
    }
  }

}
