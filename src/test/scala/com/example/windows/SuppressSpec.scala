package com.example.windows

import com.example.punctuate.Transformers.{timeLoggingTransformer, timeLoggingValueTransformer}
import com.example.{KafkaSpecHelper, SpecBase}
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Named, SessionWindows, Suppressed, Windowed}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.BeforeAndAfterEach
import org.testcontainers.containers.Network

import java.time.Duration
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class SuppressSpec extends SpecBase with BeforeAndAfterEach {

  case class Parcel(id: String, parts: List[String], createdAt: Long, updatedAt: Long)

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  val bootstrap: String = broker.getBootstrapServers

  val parcelInputTopicName = "parcelInputTopic"
  val outputTopicName      = "outputTopic"

  val parcelSerializer: Serializer[Parcel]     = implicitly
  val parcelDeserializer: Deserializer[Parcel] = implicitly
  val parcelSerde: Serde[Parcel]               = implicitly

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  private val storeName = "parcelTable"

  val parcelIds: List[String] = (1 to 10).toList map (_ => Random.alphanumeric.take(3).mkString)

  // different flavors of Suppressed
  val suppressedUntilWindowClosesUnbounded: Suppressed[Windowed[_]] =
    Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())

  // shorter, equal and longer than session
  val suppressedUntilTimeLimit1: Suppressed[Windowed[_]] =
    Suppressed.untilTimeLimit(Duration.ofSeconds(1), Suppressed.BufferConfig.maxRecords(1))
  val suppressedUntilTimeLimit3: Suppressed[Windowed[_]] =
    Suppressed.untilTimeLimit(Duration.ofSeconds(3), Suppressed.BufferConfig.maxRecords(1))
  val suppressedUntilTimeLimit5: Suppressed[Windowed[_]] =
    Suppressed.untilTimeLimit(Duration.ofSeconds(5), Suppressed.BufferConfig.maxRecords(1))

  val parcelCreatedProducer = new KafkaProducer[String, Parcel](
    streamsConfiguration,
    Serdes.stringSerde.serializer,
    parcelSerializer
  )

  var streams: KafkaStreams = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)
    val topology = makeTopology(suppressedUntilWindowClosesUnbounded)
    streams = new KafkaStreams(topology, streamsConfiguration)
    streams.cleanUp()
    streams.start()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    streams.close()
  }

  "expect one final event per parcel after window closes - testcontainer, simple time progress, session window, unbounded suppression" - {

    "single parcel " - {

      val startTime = System.currentTimeMillis()

      "1 parcel id, 1 record - no output since stream time did not progress" in {

        produceTestData(parcelIds.take(1), 1, startTime)
        val records = getResultData(outputTopicName)
        records.isEmpty mustBe true
      }

      // TODO -
      "1 parcel id, 2 records within window - no output since stream time did not progress beyond session window limit expect single final record with aggregated data of both records" in {

        produceTestData(parcelIds.take(1), 2, startTime)
        val records = getResultData(outputTopicName)
        records foreach (r => info(r))
      }

      "1 parcel id, 3 records - 2 within window, 1 outside - expect single final record with aggregated data of both first records" in {

        produceTestData(parcelIds.take(1), 3, startTime)
        val records = getResultData(outputTopicName)
        records foreach (r => info(r))

      }

      "1 parcel id, 4 records - 2 within first window, next two within own window - expect two final record for first two windows" in {

        produceTestData(parcelIds.take(1), 4, startTime)
        val records = getResultData(outputTopicName)
        records foreach (r => info(r))
      }

      // TODO - unexpected results - no records produced
      "1 parcel id, 2 records, then once again with slightly later data with same id - expect more than one record" in {

        produceTestData(parcelIds.take(1), 2, startTime)
        info("producing late data")
        produceTestData(parcelIds.take(1), 2, startTime + 5000)

        val records = getResultData(outputTopicName)
        (records.size > 1) mustBe true
      }

      // TODO - unexpected one record for first window is produced
      "1 parcel id, 2 records, then significatly late data with same id - expect more than one record" in {

        produceTestData(parcelIds.take(1), 2, startTime)

        info("producing late data")
        produceTestData(parcelIds.take(1), 2, startTime + 100000)
        val records = getResultData(outputTopicName)
        (records.size > 1) mustBe true
      }
    }

    "two parcels - for mutual streamtime advance" - {

      val startTime = System.currentTimeMillis()

      "2 parcel ids, 1 record each - expect no final results" in {

        produceTestData(parcelIds.take(2), 1, startTime)

        val records = getResultData(outputTopicName)
        records.isEmpty mustBe true
      }

      // TODO - no results, expected single result as latest record is outside of window
      "2 parcel ids, 2 records each - expect one result - latest record is outside window " in {

        produceTestData(parcelIds.take(2), 2, startTime)

        val records = getResultData(outputTopicName)
        records foreach (r => info(r))
      }

      "2 parcel ids, 3 records each - expect two results since for both parcels latest records are outside window " in {

        produceTestData(parcelIds.take(2), 3, startTime)

        val records = getResultData(outputTopicName)
        records foreach (r => info(r))
      }

      "2 parcel ids, 4 records each - expect four results since for both parcels latest records are outside window " in {

        produceTestData(parcelIds.take(2), 4, startTime)
        val records = getResultData(outputTopicName)
        records foreach (r => info(r))
      }
    }

    "test expDelay" in {

      (1 to 3) foreach { i =>
        (1 to 5) foreach { j =>
          info(s"delay for $i, $j : ${expDelay(i, j)}")
        }
      }
    }
  }

  private def produceTestData(
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
        val sent: RecordMetadata = parcelCreatedProducer
          .send(new ProducerRecord[String, Parcel](parcelInputTopicName, null, now, id, parcel))
          .get()
        info(s"record sent with id $id, part $randomParcelPart & ts $now : ${sent.offset()}")
      }
    }

  def getResultData(topic: String): Iterable[ConsumerRecord[String, Parcel]] = {
    val consumer = new KafkaConsumer[String, Parcel](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      parcelDeserializer
    )
    consumer.subscribe(List(topic).asJavaCollection)
    KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = 500,
      abortOnFirstRecord = false,
      maxAttempts = 10
    )
  }

  def expDelay(i: Int, j: Int): Long = i * 1001 + (Math.pow(2, j) * 1001).toLong

  def makeTopology(
      suppressConfig: Suppressed[Windowed[_]],
      sessionWindowDuration: Duration = Duration.ofSeconds(3)
  ): Topology = {
    val sessionWindows: SessionWindows =
      SessionWindows.ofInactivityGapWithNoGrace(sessionWindowDuration)

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

}
