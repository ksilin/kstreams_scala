package com.example.windows

import com.example.SpecBase
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.ParallelTestExecution
import org.testcontainers.containers.Network
import org.scalatest.prop.TableDrivenPropertyChecks._

class SuppressPropSpec extends SpecBase with ParallelTestExecution {

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  val bootstrap: String = broker.getBootstrapServers

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  val parcelCreatedProducer = new KafkaProducer[String, Parcel](
    streamsConfiguration,
    Serdes.stringSerde.serializer,
    parcelSerializer
  )

  val streamsCtx = streamsContext(adminClient, parcelInputTopicName, outputTopicName, streamsConfiguration) _

  "expect one final event per parcel after window closes - testcontainer, simple time progress, session window, unbounded suppression" - {

    "single parcel " - {

      val startTime = System.currentTimeMillis()

      "1 parcel id, 1 record - no output since stream time did not progress" in {

        forAll(suppressTable) { suppress =>
          // whenever(suppress.)
          streamsCtx(suppress) { s =>
            produceTestDataSync(parcelCreatedProducer, parcelInputTopicName, parcelIds.take(1), 1, startTime)
            val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName, 200, 3, true)
            records.isEmpty mustBe true
          }
        }
      }

      "1 parcel id, 3 records - must output single result record" in {

        forEvery(suppressTable) { suppress =>
          streamsCtx(suppress) { s =>
            produceTestDataSync(parcelCreatedProducer, parcelInputTopicName, parcelIds.take(1), 3, startTime)
            val records = getResultData(makeParcelConsumer(streamsConfiguration), outputTopicName, 200, 3, true)
            records foreach (r => info(r))
            records.size mustBe 1
          }
        }
      }
    }
  }
}
