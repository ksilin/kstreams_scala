package com.example.testcontainers

import com.example.json.TestData._
import com.example.{KafkaSpecHelper, SpecBase, Translation}
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.testcontainers.containers.Network
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes
import io.circe.generic.auto._
import nequi.circe.kafka._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.testcontainers.containers.output.OutputFrame

import java.util.Properties
import scala.jdk.CollectionConverters._

class TestContainersInitSpec extends SpecBase {

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer = containerFactory.createConfluentServer().withLogConsumer((frame: OutputFrame) => println(frame.getUtf8String.take(0))).asInstanceOf[ConfluentServerContainer]
  val schemaRegistry: SchemaRegistryContainer = containerFactory.createSchemaRegistry(broker)//.withLogLevel("INFO")
  schemaRegistry.start() //will implicitly start kafka container

  val bootstrap: String = broker.getBootstrapServers
  info(s"bootstrap: $bootstrap")

  val textLineInputTopicName = "inputTopic"
  val translationInputTopicName  = "translationsInputTopic"
  val translationOutputTopicName = "translationOutputTopic"

  val translationSerializer: Serializer[Translation] = implicitly
  val translationDeserializer: Deserializer[Translation] = implicitly
  val translationSerde: Serde[Translation] = implicitly

  val config = new Properties()
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName)
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  config.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val adminClient: AdminClient = AdminClient.create(config)

  "must run simple topology on testcontainer server" in {

    KafkaSpecHelper.createTopic(adminClient, textLineInputTopicName, 1, 1)
    KafkaSpecHelper.createTopic(adminClient, translationInputTopicName, 1, 1)

    val textLinesProducer = new KafkaProducer[Int, String](config, Serdes.intSerde.serializer(),Serdes.stringSerde.serializer )
    val translationsProducer = new KafkaProducer[String, String](config, Serdes.stringSerde.serializer(),Serdes.stringSerde.serializer )

    val topology = makeTopology()
    val streams = new KafkaStreams(topology, config)
    streams.start()

    textLines.zipWithIndex foreach { case (t, i) =>
      textLinesProducer.send(new ProducerRecord[Int, String](textLineInputTopicName, i, t)).get()
    }
    translationsEn foreach { case (w, tr) =>
      translationsProducer.send(new ProducerRecord[String, String](translationInputTopicName, w, tr))
    }

    // TODO: check state and metrics here:

    val consumer = new KafkaConsumer[String, Translation](config, Serdes.stringSerde.deserializer(), translationDeserializer)
    consumer.subscribe(List(translationOutputTopicName).asJavaCollection)
    KafkaSpecHelper.fetchAndProcessRecords(consumer)

    streams.close()
  }

  def makeTopology(): Topology = {

    val textLineStream: KStream[Int, String]       = builder.stream(textLineInputTopicName)(Consumed.`with`(Serdes.intSerde, Serdes.stringSerde))
    val translations: KStream[String, String] = builder.stream(translationInputTopicName)(Consumed.`with`(Serdes.stringSerde, Serdes.stringSerde))

    val wordSplit: KStream[String, String] = textLineStream
      .flatMap { (_, v: String) =>
        val words = wordPattern.split(v.toLowerCase)
        List.from(words).map(w => (w, w))
      }

    val trTable: KTable[String, Translation] =
      translations.groupByKey(Grouped.`with`(Serdes.stringSerde, Serdes.stringSerde))
        .aggregate(Translation("", Set.empty[String]))(
        (aggKey, newTranslation: String, aggregated: Translation) =>
          Translation(aggKey, aggregated.translations + newTranslation)
      ) (Materialized.`with`(Serdes.stringSerde, translationSerde))

    // default join windows?
    val joined: KStream[String, Translation] =
      wordSplit.leftJoin(trTable)((_, translation) => translation)// TODO - whats the implicit here?
    //(StreamJoined.`with`(Serdes.stringSerde, Serdes.stringSerde, translationSerde))

    joined.to(translationOutputTopicName)(Produced.`with`(Serdes.stringSerde, translationSerde))

    builder.build()
  }


}
