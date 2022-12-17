package com.example.statestore

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.{ Cancellable, PunctuationType, Punctuator }
import org.apache.kafka.streams.state.KeyValueStore

import java.lang
import java.time.Duration
import _root_.scala.jdk.CollectionConverters._

case class TTLExtractorStoreCleaner[K, V](
    punctuateInterval: Duration,
    ttl: lang.Long,
    timestampExtractor: V => lang.Long,
    storeName: String,
    punctuationType: PunctuationType = PunctuationType.WALL_CLOCK_TIME
) extends Transformer[K, V, KeyValue[K, V]]
    with StrictLogging {

  var ctx: ProcessorContext              = _
  var store: KeyValueStore[K, V]         = _
  var cancellablePunctuator: Cancellable = _

  override def init(context: ProcessorContext): Unit = {
    logger.debug(s"initializing transformer with $context")
    ctx = context
    store = ctx.getStateStore(storeName)
    logger.debug(s"transformer retrieved store $store")

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
    logger.debug(s"checking state store $storeName for expired values at $now")
    store.all().asScala.foreach { kv: KeyValue[K, V] =>
      val valueTs: Long = timestampExtractor(kv.value)
      val diff          = now - valueTs
      logger.trace(s"time diff for ${kv.key}: ${diff}")

      if (diff > ttl) {
        logger.debug(s"removing value for key ${kv.key} from store $storeName")
        store.delete(kv.key)
      }
    }
  }
}
