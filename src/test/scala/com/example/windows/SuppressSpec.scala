package com.example.windows

import com.example.punctuate.Transformers.{timeLoggingTransformer, timeLoggingValueTransformer}
import com.example.{KafkaSpecHelper, SpecBase}
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Named, SessionWindows, Suppressed, Windowed}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.testcontainers.containers.Network

import java.time.Duration
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class SuppressSpec extends SpecBase {

  case class Parcel(id: String, parts: List[String], createdAt: Long, updatedAt: Long)

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  val bootstrap: String = broker.getBootstrapServers

  val parcelInputTopicName = "parcelInputTopic"
  val outputTopicName            = "outputTopic"

  val parcelSerializer: Serializer[Parcel]     = implicitly
  val parcelDeserializer: Deserializer[Parcel] = implicitly
  val parcelSerde: Serde[Parcel]               = implicitly

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  private val storeName = "parcelTable"

  val parcelIds: List[String] = (1 to 10).toList map (_ => Random.alphanumeric.take(3).mkString)

  "must emit one final event per parcel after window closes - testcontainer" - {

    val topology = makeTopology()
    println(topology.describe())

    "1 parcel id, 1 record" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      produceTestData(parcelIds.take(1), 1)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 200, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }


    "1 parcel id, 2 records, then out-of-order data with same id" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      val time1 = System.currentTimeMillis()

      produceTestData(parcelIds.take(1), 2)

      info("producing out-of-order data")
      // late data
      produceTestData(parcelIds.take(1), 2, time1 - 100000)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 500, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

    "1 parcel id, 2 records, then late data with same id" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      val time1 = System.currentTimeMillis()

      produceTestData(parcelIds.take(1), 2)

      info("producing late data")
      // late data
      produceTestData(parcelIds.take(1), 2, time1 + 100000)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 500, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

    "1 parcel id, 3 records" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      produceTestData(parcelIds.take(1), 3)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 500, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

    "2 parcel ids, 1 record" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      produceTestData(parcelIds.take(2), 1)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 200, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

    "2 parcel ids, 2 records" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      produceTestData(parcelIds.take(2), 2)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 200, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

    "2 parcel id, 3 record" in {

      KafkaSpecHelper.createOrTruncateTopic(adminClient, parcelInputTopicName, 1, 1)
      KafkaSpecHelper.createOrTruncateTopic(adminClient, outputTopicName, 1, 1)

      val streams = new KafkaStreams(topology, streamsConfiguration)
      streams.cleanUp()
      streams.start()

      val time1 = System.currentTimeMillis()

      produceTestData(parcelIds.take(2), 3, time1)

      val consumer = new KafkaConsumer[String, Parcel](
        streamsConfiguration,
        Serdes.stringSerde.deserializer(),
        parcelDeserializer
      )
      consumer.subscribe(List(outputTopicName).asJavaCollection)
      KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 200, abortOnFirstRecord = false, maxAttempts = 10)

      streams.close()
    }

  }

  private def produceTestData(parcelIds: List[String], recordsPerId: Int = 1, initTime: Long = System.currentTimeMillis()): Unit = {

    val parcelCreatedProducer = new KafkaProducer[String, Parcel](
      streamsConfiguration,
      Serdes.stringSerde.serializer,
      parcelSerializer
    )

    parcelIds.zipWithIndex foreach { case (id, i) =>
      // make more to actually reduce
      (1 to recordsPerId) foreach { j =>
        val now = initTime + i * 1000 + (Math.pow(2, j)*1000).toLong
        val parcel = Parcel(id, List(Random.alphanumeric.take(3).mkString), now, now)
        val sent: RecordMetadata = parcelCreatedProducer.send(new ProducerRecord[String, Parcel](parcelInputTopicName, null, now, id, parcel))
          .get()
        info(s"record sent with id $id,  & ts $now : ${sent.offset()}")
      }
    }
  }


  def makeTopology(): Topology = {

    // different flavors of Suppressed yield same results
    val suppressedUntilWindowClosesUnbounded: Suppressed[Windowed[_]] = Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
    val suppressedUntilTimeLimit: Suppressed[Windowed[String]] = Suppressed.untilTimeLimit(Duration.ofSeconds(1), Suppressed.BufferConfig.maxRecords(1))

    val parcelStream: KStream[String, Parcel] = builder.stream(parcelInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, parcelSerde).withName("parcelInput")
    )

    // val t = parcelStream.transform(() => timeLoggingTransformer[String, Parcel]("parcelStream"))

    val groupedParcels: KGroupedStream[String, Parcel] = parcelStream.groupByKey(
      Grouped.`with`(Serdes.stringSerde, parcelSerde).withName("groupedParcels")
    )
    val windowed: SessionWindowedKStream[String, Parcel] = groupedParcels.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(3)))
    // TODO - use explicit store
    val reduced: KTable[Windowed[String], Parcel] = windowed.reduce((aggregator, newParcel) => {
      info(s"reducing $newParcel with agg $aggregator")
      aggregator.copy(
        parts = aggregator.parts ++ newParcel.parts,
        updatedAt = System.currentTimeMillis()
      )
    }
    )(Materialized.as(storeName)(Serdes.stringSerde, parcelSerde))

   // val t = reduced.transformValues(() => timeLoggingValueTransformer[Windowed[String], Parcel]("reducedParcels"))

    val suppressed: KTable[Windowed[String], Parcel] = reduced.suppress(suppressedUntilTimeLimit.withName("suppressedParcels"))//, Materialized.`with`(Serdes.stringSerde, parcelSerde))

    // suppressed.transformValues(() => timeLoggingValueTransformer[Windowed[String], Parcel]("suppressedParcels"))

    val completeParcels: KStream[Windowed[String], Parcel] = suppressed.toStream(Named.as("completeParcels"))
    // dead-end for logging only
    // val loggedCompleteParcels = completeParcels.transform(() => timeLoggingTransformer[Windowed[String], Parcel]("completeParcels"))
    val unwindowedParcels: KStream[String, Parcel] = completeParcels.map((k, v) => {
      info(s"extracting key from windowed key ${k.key()}, ${k.window()} : $v")
      (k.key(), v)
    }
    )
    unwindowedParcels.to(outputTopicName)(Produced.`with`(Serdes.stringSerde, parcelSerde))

    builder.build()
  }

}
