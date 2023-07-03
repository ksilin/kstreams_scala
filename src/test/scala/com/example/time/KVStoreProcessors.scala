package com.example.time

import org.apache.kafka.streams.processor.api.{ Processor, ProcessorContext, Record }
import org.apache.kafka.streams.state.KeyValueStore

object KVStoreProcessors {

  case class TxAggregationProcessor(tableStoreName: String, lookbackMs: Long)
      extends Processor[String, Tx, String, TxAggregate] {

    var context: ProcessorContext[String, TxAggregate] = _
    var store: KeyValueStore[String, TxAggregate]      = _

    override def init(context: ProcessorContext[String, TxAggregate]): Unit = {
      this.context = context
      store = context.getStateStore(tableStoreName)
    }

    override def process(record: Record[String, Tx]): Unit = {
      val acctId = record.key()
      val tx     = record.value()
      println(s"processing tx ${tx.txId} for account ${acctId}")

      // here we are using the record TS for forwarding and purging
      val recordTimestamp = record.timestamp()

      val found: TxAggregate = store.get(acctId)

      if (found != null) {
        println(s"! found existing aggregation for account $acctId: $found, updating with : ${tx}")
        val updatedAgg = TxAggregate.update(found, tx, lookbackMs)
        store.put(acctId, updatedAgg)
        context.forward(new Record(acctId, updatedAgg, recordTimestamp))
      } else {
        println(s"? no existing aggregation for account $acctId found, storing $tx")
        val newAgg = TxAggregate(tx)
        store.put(acctId, newAgg)
        context.forward(new Record(acctId, newAgg, recordTimestamp))
      }

      // purge outdated aggregates
      store
        .all()
        .forEachRemaining(kv =>
          if (recordTimestamp - kv.value.updatedAt > lookbackMs) store.put(acctId, null)
        )
    }
  }

}
