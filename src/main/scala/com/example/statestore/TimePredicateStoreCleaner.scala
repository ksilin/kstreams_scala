package com.example.statestore

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.KeyValueStore

import java.time.Duration
import _root_.scala.jdk.CollectionConverters._

case class TimePredicateStoreCleaner[K, V](
                                              punctuateInterval: Duration,
                                              deleteIfTrue: (V, Long) => Boolean,
                                              storeName: String,
                                              punctuationType: PunctuationType = PunctuationType.WALL_CLOCK_TIME
) extends Transformer[K, V, KeyValue[K, V]]
    with StrictLogging {

  var ctx: ProcessorContext              = _
  var store: KeyValueStore[K, V]         = _
  var cancellablePunctuator: Cancellable = _

  override def init(context: ProcessorContext): Unit = {
    logger.debug(s"initializing $getClass with $context")
    ctx = context
    store = ctx.getStateStore(storeName)
    logger.debug(s"$getClass retrieved store $store")

    cancellablePunctuator = ctx.schedule(
      punctuateInterval,
      punctuationType,
      punctuator
    )
  }

  override def transform(key: K, value: V): KeyValue[K, V] =
    new KeyValue(key, value)

  override def close(): Unit = {}

  val punctuator: Punctuator = (now: Long) => {
    logger.debug(s"checking state store $storeName for records to remove at $now")
    store.all().asScala.foreach { kv: KeyValue[K, V] =>
      if (deleteIfTrue(kv.value, now)) {
        logger.debug(s"removing value for key ${kv.key} from store $storeName")
        store.delete(kv.key)
      }
    }
  }
}
