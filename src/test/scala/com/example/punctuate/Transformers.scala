package com.example.punctuate

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.KeyValueStore

object Transformers {

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


}
