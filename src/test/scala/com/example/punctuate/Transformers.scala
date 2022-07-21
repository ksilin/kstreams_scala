package com.example.punctuate

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.KeyValueStore
import wvlet.log.{LocalLogSupport, LogSupport}

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime}

object Transformers extends LocalLogSupport {

  def identityTransformer[K, V](): Transformer[K, V, KeyValue[K, V]] =
    new Transformer[K, V, KeyValue[K, V]] {
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: K, value: V): KeyValue[K, V] = {
        new KeyValue(key, value)
      }
      override def close(): Unit = {}
    }

  def createTransformer[K, V](fun: (K, V) => KeyValue[K, V]): Transformer[K, V, KeyValue[K, V]] = {
    new Transformer[K, V, KeyValue[K, V]] {

      var ctx: ProcessorContext = _

      override def init(context: ProcessorContext): Unit = {
        ctx = context
      }

      override def transform(key: K, value: V): KeyValue[K, V] = fun(key, value)

      override def close(): Unit = {}
    }
  }

  def createCtxAwareTransformer[K, V](fun: (K, V, ProcessorContext) => KeyValue[K, V]): Transformer[K, V, KeyValue[K, V]] = {
    new Transformer[K, V, KeyValue[K, V]] {

      var ctx: ProcessorContext = _

      override def init(context: ProcessorContext): Unit = {
        ctx = context
      }

      override def transform(key: K, value: V): KeyValue[K, V] = fun(key, value, ctx)

      override def close(): Unit = {}
    }
  }

  def createCtxAwareValueTransformer[K, V, R](fun: (K, V, ProcessorContext) => R): ValueTransformerWithKey[K, V, R] = {
    new ValueTransformerWithKey[K, V, R] {

      var ctx: ProcessorContext = _

      override def init(context: ProcessorContext): Unit = {
        ctx = context
      }

      override def transform(key: K, value: V): R = fun(key, value, ctx)

      override def close(): Unit = {}
    }
  }

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  def timeLoggingTransformer[K, V](name: String): Transformer[K, V, KeyValue[K, V]] = createCtxAwareTransformer((k, v, ctx) => {
    info(s"$name: processing $k : $v")
    logCtx(ctx)
    KeyValue.pair(k, v)
  })

  def timeLoggingValueTransformer[K, V](name: String): ValueTransformerWithKey[K, V, V] = createCtxAwareValueTransformer((k, v, ctx) => {
    info(s"$name: processing $k : $v")
    logCtx(ctx)
    v
  })

  def logCtx(ctx: ProcessorContext) = {
    info(s"taskId: ${ctx.taskId()}")
    info(s"topic: ${ctx.topic()}")
    info(s"partition: ${ctx.partition()}")
    info(s"offset: ${ctx.offset()}")
    info(s"wallclock: ${df.format(ctx.currentSystemTimeMs())}")
    info(s"stream:    ${df.format(ctx.currentStreamTimeMs())}")
    info(s"record ts: ${df.format(ctx.timestamp())}")
  }


  def storeTransformer[K, V](storeName: String): Transformer[K, V, KeyValue[K, V]] = {
    new Transformer[K, V, KeyValue[K, V]] with StrictLogging {

      var store: KeyValueStore[K, V] = _

      override def init(context: ProcessorContext): Unit = {
        store = context.getStateStore(storeName)
      }

      override def transform(key: K, value: V): KeyValue[K, V] = {
        logger.info(s"storing ${key}:${value} in $storeName ")
        store.put(key, value)
        new KeyValue[K, V](key, value)
      }

      override def close(): Unit = {}
    }
  }

  def storeAndDeleteTransformer[K, V](storeName: String): Transformer[K, V, KeyValue[K, V]] = {
    new Transformer[K, V, KeyValue[K, V]] with StrictLogging {

      var store: KeyValueStore[K, V] = _

      override def init(context: ProcessorContext): Unit = {
        store = context.getStateStore(storeName)
      }

      override def transform(key: K, value: V): KeyValue[K, V] = {
        if(value == null) {
          logger.info(s"deleting entry for $key from store $storeName, received tombstone")
          store.delete(key)
        } else {
          logger.info(s"adding entry for $key to store $storeName: $value")
          store.put(key, value)
        }
        new KeyValue[K, V](key, value)
      }

      override def close(): Unit = {}
    }
  }


}
