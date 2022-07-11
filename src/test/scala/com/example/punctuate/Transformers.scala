package com.example.punctuate

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.KeyValueStore
import wvlet.log.{LocalLogSupport, LogSupport}

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
      override def init(context: ProcessorContext): Unit = {}

      override def transform(key: K, value: V): KeyValue[K, V] = fun(key, value)

      override def close(): Unit = {}
    }
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
