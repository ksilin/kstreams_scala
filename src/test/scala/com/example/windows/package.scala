package com.example

import com.example.util.{ FutureConverter, KafkaSpecHelper }
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }
import io.circe.generic.auto._
import nequi.circe.kafka._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.streams.{ KafkaStreams, Topology }
import org.apache.kafka.streams.kstream.{ Named, SessionWindows, Suppressed, Windowed }
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{
  Consumed,
  Grouped,
  KGroupedStream,
  KStream,
  KTable,
  Materialized,
  Produced,
  SessionWindowedKStream
}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor1
import wvlet.log.LogSupport

import _root_.scala.jdk.CollectionConverters._
import java.time.Duration
import java.util.Properties
import scala.concurrent.{ Await, Future }
import scala.util.Random
import java.util.concurrent.{ Future => jFuture }
import _root_.scala.concurrent.duration._
import _root_.scala.concurrent.ExecutionContext.Implicits.global

package object windows extends LogSupport {

  case class Parcel(id: String, parts: List[String], createdAt: Long, updatedAt: Long)

  val parcelInputTopicName = "parcelInputTopic"
  val outputTopicName      = "outputTopic"

  private val storeName = "parcelTable"

  val parcelSerializer: Serializer[Parcel]     = implicitly
  val parcelDeserializer: Deserializer[Parcel] = implicitly
  val parcelSerde: Serde[Parcel]               = implicitly

  val parcelIds: List[String] = (1 to 10).toList map (_ => Random.alphanumeric.take(3).mkString)

  // different flavors of Suppressed
  val suppressedUntilWindowClosesUnbounded: Suppressed[Windowed[_]] =
    Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("untilCloses")

  // shorter, equal and longer than session
  val suppressedUntilTimeLimit1: Suppressed[Windowed[_]] =
    Suppressed
      .untilTimeLimit(
        Duration.ofSeconds(1),
        Suppressed.BufferConfig.maxRecords(1).emitEarlyWhenFull()
      )
      .withName("untilOneSec")
  val suppressedUntilTimeLimit3: Suppressed[Windowed[_]] =
    Suppressed
      .untilTimeLimit(
        Duration.ofSeconds(3),
        Suppressed.BufferConfig.maxRecords(1).emitEarlyWhenFull()
      )
      .withName("untilThreeSec")
  val suppressedUntilTimeLimit5: Suppressed[Windowed[_]] =
    Suppressed
      .untilTimeLimit(
        Duration.ofSeconds(5),
        Suppressed.BufferConfig.maxRecords(1).emitEarlyWhenFull()
      )
      .withName("untilFiveSec")

  val suppressTable: TableFor1[Suppressed[Windowed[_]]] = Table(
    "suppress",
    suppressedUntilWindowClosesUnbounded,
    suppressedUntilTimeLimit1,
    suppressedUntilTimeLimit3,
    suppressedUntilTimeLimit5
  )

  def streamsContext(
      adminClient: AdminClient,
      inputTopicName: String,
      outputTopicName: String,
      streamsConfig: Properties
  )(suppressed: Suppressed[Windowed[_]])(testCode: (KafkaStreams) => Any): Any = {
    info("creating topology with suppressed:")
    info(suppressed.toString) // no way to get the name of a suppressed instance
    KafkaSpecHelper.createOrTruncateTopic(adminClient, inputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)
    val topology = makeTopology(suppressed)
    //System.out.println(topology.describe())
    val streams = new KafkaStreams(topology, streamsConfig)
    streams.cleanUp()
    streams.start()

    try testCode(streams)
    finally {
      streams.close()
      streams.cleanUp()
    }
  }

  def makeTopology(
      suppressConfig: Suppressed[Windowed[_]],
      sessionWindowDuration: Duration = Duration.ofSeconds(3)
  ): Topology = {
    val sessionWindows: SessionWindows =
      SessionWindows.ofInactivityGapWithNoGrace(sessionWindowDuration)

    val builder: StreamsBuilder = new StreamsBuilder()

    val parcelStream: KStream[String, Parcel] = builder.stream(parcelInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, parcelSerde).withName("parcelInput")
    )

    // val t = parcelStream.transform(() => timeLoggingTransformer[String, Parcel]("parcelStream"))
    parcelStream.peek((k, v) => info(s"started processing for key $k : $v"))
    val groupedParcels: KGroupedStream[String, Parcel] = parcelStream.groupByKey(
      Grouped.`with`(Serdes.stringSerde, parcelSerde).withName("groupedParcels")
    )
    val windowed: SessionWindowedKStream[String, Parcel] = groupedParcels.windowedBy(sessionWindows)
    // TODO - use explicit store
    val reduced: KTable[Windowed[String], Parcel] = windowed.reduce { (aggregator, newParcel) =>
      info(s"reducing $newParcel with agg $aggregator")
      aggregator.copy(
        parts = aggregator.parts ++ newParcel.parts,
        updatedAt = System.currentTimeMillis()
      )
    }(Materialized.as(storeName)(Serdes.stringSerde, parcelSerde))

    // val t = reduced.transformValues(() => timeLoggingValueTransformer[Windowed[String], Parcel]("reducedParcels"))

    val suppressed: KTable[Windowed[String], Parcel] = reduced.suppress(
      suppressConfig.withName("suppressedParcels")
    ) //, Materialized.`with`(Serdes.stringSerde, parcelSerde))

    // suppressed.transformValues(() => timeLoggingValueTransformer[Windowed[String], Parcel]("suppressedParcels"))

    val completeParcels: KStream[Windowed[String], Parcel] =
      suppressed.toStream(Named.as("completeParcels"))
    // dead-end for logging only
    // val loggedCompleteParcels = completeParcels.transform(() => timeLoggingTransformer[Windowed[String], Parcel]("completeParcels"))
    val unwindowedParcels: KStream[String, Parcel] = completeParcels.map { (k, v) =>
      info(s"extracting key from windowed key ${k.key()}, ${k.window()} : $v")
      (k.key(), v)
    }
    unwindowedParcels.to(outputTopicName)(Produced.`with`(Serdes.stringSerde, parcelSerde))

    builder.build()
  }

  def expDelay(i: Int, j: Int): Long = i * 1001 + (Math.pow(2, j) * 1001).toLong

  def makeParcelConsumer(streamsConfig: Properties): KafkaConsumer[String, Parcel] =
    new KafkaConsumer[String, Parcel](
      streamsConfig,
      Serdes.stringSerde.deserializer(),
      parcelDeserializer
    )

  def getResultData(
      consumer: KafkaConsumer[String, Parcel],
      topic: String,
      pause: Int = 500,
      maxAttempts: Int = 10,
      abortOnFirstRecord: Boolean = false
  ): Iterable[ConsumerRecord[String, Parcel]] = {
    consumer.subscribe(List(topic).asJavaCollection)
    KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = pause,
      abortOnFirstRecord = abortOnFirstRecord,
      maxAttempts = maxAttempts
    )
  }

  def produceTestDataSync(
      producer: KafkaProducer[String, Parcel],
      topic: String,
      parcelIds: List[String],
      recordsPerId: Int = 1,
      initTime: Long = System.currentTimeMillis()
  ): Unit = {
    val recordsSent = produceTestData(producer, topic, parcelIds, recordsPerId, initTime)
    val recordsMeta = Await.result(recordsSent, 10.seconds)
    recordsMeta foreach { case (id, part, now, meta) =>
      info(s"record sent with id $id, part $part & ts $now : ${meta.offset()}")
    }
  }

  def produceTestData(
      producer: KafkaProducer[String, Parcel],
      topic: String,
      parcelIds: List[String],
      recordsPerId: Int = 1,
      initTime: Long = System.currentTimeMillis()
  ): Future[Seq[(String, String, Long, RecordMetadata)]] = {
    val recordsSent: Seq[(String, String, Long, jFuture[RecordMetadata])] =
      (1 to recordsPerId) flatMap { parcelPartIndex =>
        parcelIds.zipWithIndex map { case (id, parcelIndex) =>
          val now              = initTime + expDelay(parcelIndex, parcelPartIndex)
          val randomParcelPart = Random.alphanumeric.take(3).mkString
          val parcel           = Parcel(id, List(randomParcelPart), now, now)
          val record           = new ProducerRecord[String, Parcel](topic, null, now, id, parcel)
          (id, randomParcelPart, now, producer.send(record))
        }
      }
    val recordsSentScala = recordsSent.map {
      case (id, part, now, jFuture: jFuture[RecordMetadata]) =>
        FutureConverter.toScalaFuture(jFuture).map(recordMeta => (id, part, now, recordMeta))
    }
    Future.sequence(recordsSentScala)
  }

  private def produceSeqTestData(
      producer: KafkaProducer[String, Parcel],
      parcelIds: List[String],
      recordsPerId: Int = 1,
      initTime: Long = System.currentTimeMillis()
  ): Unit =
    parcelIds.zipWithIndex foreach { case (id, i) =>
      // make more to actually reduce
      (1 to recordsPerId) foreach { j =>
        val now              = initTime + expDelay(i, j)
        val randomParcelPart = Random.alphanumeric.take(3).mkString
        val parcel           = Parcel(id, List(randomParcelPart), now, now)
        val sent: RecordMetadata = producer
          .send(new ProducerRecord[String, Parcel](parcelInputTopicName, null, now, id, parcel))
          .get()
        info(s"record sent with id $id, part $randomParcelPart & ts $now : ${sent.offset()}")
      }
    }

}
