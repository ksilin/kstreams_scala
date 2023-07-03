package com.example

import com.example.serde.{ GsonDeserializer, GsonSerializer }
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }

import java.{ lang, util }
import scala.jdk.CollectionConverters._
import java.util._
import scala.collection.mutable

package object time {

  case class MyRecord(name: String, description: String, createdAt: lang.Long, updatedAt: lang.Long)
  case class MyRecordJoined(maybeLeft: Option[MyRecord], maybeRight: Option[MyRecord])

  val recordSerializer: Serializer[MyRecord] = new GsonSerializer[MyRecord]
  val recordDeserializer: Deserializer[MyRecord] =
    new GsonDeserializer[MyRecord](classOf[MyRecord])
  val recordSerde: Serde[MyRecord] = Serdes.serdeFrom(recordSerializer, recordDeserializer)

  val recordJoinedSerializer: Serializer[MyRecordJoined] = new GsonSerializer[MyRecordJoined]
  val recordJoinedDeserializer: Deserializer[MyRecordJoined] =
    new GsonDeserializer[MyRecordJoined](classOf[MyRecordJoined])
  val recordJoinedSerde: Serde[MyRecordJoined] =
    Serdes.serdeFrom(recordJoinedSerializer, recordJoinedDeserializer)

  case class Tx(accountId: String, txId: String, status: String, amount: Int, createdAt: lang.Long)

  case class TxAggregate(
      accountId: String,
      txList: util.ArrayList[Tx],
      createdAt: lang.Long,
      updatedAt: lang.Long
  ) {
    val total: Int = txList.asScala.foldLeft(0)((acc: Int, t1: Tx) =>
      if (t1.status == "FAILED") acc else t1.amount + acc
    )
  }

  case object TxAggregate {
    def apply(tx: Tx): TxAggregate = TxAggregate(
      tx.accountId,
      new util.ArrayList[Tx](util.List.of(tx)),
      tx.createdAt,
      tx.createdAt
    )

    def update(agg: TxAggregate, tx: Tx, windowSize: Long): TxAggregate = {

      // tx to be added to list
      val txToAdd: Tx = agg.txList.asScala
        .find(_.txId == tx.txId)
        .fold {
          tx
        } { foundTx =>
          foundTx.copy(status = tx.status, createdAt = tx.createdAt)
        }

      // remove outdated tx and the one to be added
      val removedOldTx: mutable.Buffer[Tx] = agg.txList.asScala
        .filter(tx.createdAt - _.createdAt < windowSize)
        .filterNot(txToAdd.txId == _.txId)

      // create new aggregate with updated list and timestamp
      agg.copy(
        txList = new util.ArrayList(removedOldTx.append(txToAdd).asJava),
        updatedAt = tx.createdAt
      )
    }
  }

  val txSerializer: Serializer[Tx] = new GsonSerializer[Tx]
  val txDeserializer: Deserializer[Tx] =
    new GsonDeserializer[Tx](classOf[Tx])
  val txSerde: Serde[Tx] = Serdes.serdeFrom(txSerializer, txDeserializer)

  val txAggSerializer: Serializer[TxAggregate] = new GsonSerializer[TxAggregate]
  val txAggDeserializer: Deserializer[TxAggregate] =
    new GsonDeserializer[TxAggregate](classOf[TxAggregate])
  val txAggSerde: Serde[TxAggregate] = Serdes.serdeFrom(txAggSerializer, txAggDeserializer)

}
