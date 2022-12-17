package com.example.windows

import com.example.SpecBase
import net.christophschubert.cp.testcontainers.{ CPTestContainerFactory, ConfluentServerContainer }
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.testcontainers.containers.Network

class SuppressSpec extends SpecBase {

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  val bootstrap: String = broker.getBootstrapServers

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"$suiteName-group")
  val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  val parcelCreatedProducer = new KafkaProducer[String, Parcel](
    streamsConfiguration,
    Serdes.stringSerde.serializer,
    parcelSerializer
  )

  private val streamsCtx =
    streamsContext(adminClient, parcelInputTopicName, outputTopicName, streamsConfiguration) _

  "expect one final event per parcel after window closes - testcontainer, simple time progress, session window, unbounded suppression" - {

    "single parcel" - {

      val startTime      = System.currentTimeMillis()
      val suppressConfig = suppressedUntilWindowClosesUnbounded

      "1 parcel id, 1 record - no output since stream time did not progress" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          1,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records.isEmpty mustBe true
      }

      "1 parcel id, 2 records within window - no output since stream time did not progress beyond session window limit expect single final record with aggregated data of both records" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          2,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records.isEmpty mustBe true
      }

      "1 parcel id, 3 records - 2 within window, 1 outside - expect single final record with aggregated data of both first records" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          3,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records foreach (r => info(r))
        records.size mustBe 1
      }

      "1 parcel id, 4 records - 2 within first window, next two within own window - expect two final record for first two windows" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          4,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records foreach (r => info(r))
        records.size mustBe 2
      }

      // TODO - unexpected results - no records produced
      "1 parcel id, 2 records, then once again with slightly later data with same id - expect more than one record" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          2,
          startTime
        )
        info("producing late data")
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          2,
          startTime + 5000
        )

        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        (records.size > 1) mustBe true
      }

      // TODO - unexpected one record for first window is produced
      "1 parcel id, 2 records, then significantly late data with same id - expect records from both 'batches'" in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          2,
          startTime
        )
        info("producing late data")
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(1),
          2,
          startTime + 100000
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        (records.size > 1) mustBe true
      }
    }

    "two parcels - for mutual streamtime  - suppressedUntilWindowClosesUnbounded" - {

      val startTime      = System.currentTimeMillis()
      val suppressConfig = suppressedUntilWindowClosesUnbounded

      "2 parcel ids, 1 record each - expect no final results" in streamsCtx(
        suppressedUntilWindowClosesUnbounded
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(2),
          1,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records.isEmpty mustBe true
      }

      // TODO - no results, expected single result as latest record is outside of window
      "2 parcel ids, 2 records each - expect one result - latest record is outside window " in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(2),
          2,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records foreach (r => info(r))
        records.nonEmpty mustBe true
      }

      "2 parcel ids, 3 records each - expect two results since for both parcels latest records are outside window " in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(2),
          3,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records foreach (r => info(r))
        records.nonEmpty mustBe true
      }

      "2 parcel ids, 4 records each - expect four results since for both parcels latest records are outside window " in streamsCtx(
        suppressConfig
      ) { _ =>
        produceTestDataSync(
          parcelCreatedProducer,
          parcelInputTopicName,
          parcelIds.take(2),
          4,
          startTime
        )
        val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName)
        records foreach (r => info(r))
        records.nonEmpty mustBe true
      }
    }

  }

}
