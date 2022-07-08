package com.example.json

import com.example.{ SpecBase, Translation }
import com.goyeau.kafka.streams.circe.CirceSerdes
import com.goyeau.kafka.streams.circe.CirceSerdes._
import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import TestData._

class KafkaStreamsCirceGenericSerdeSpec extends SpecBase {

  val wordInputTopicName = "inputTopic"

  val translationInputTopicName  = "translationsInputTopic"
  val translationOutputTopicName = "translationOutputTopic"

  "must translate from english & russian to german" in {

    val topology = makeTopology()
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val (inputTopic, translationInputTopicEn, translationOutputTopic) =
      createTestTopics(topologyTestDriver)

    translationsEn.foreach { case (k: String, v: String) =>
      translationInputTopicEn.pipeInput(k, v)
    }
    inputTopic.pipeValueList(textLines.asJava)

    val outputRecords: mutable.Map[String, Translation] =
      translationOutputTopic.readKeyValuesToMap().asScala

    outputRecords must contain theSameElementsAs expectedTranslations
  }

  def makeTopology(): Topology = {

    val textLines: KStream[Int, String]       = builder.stream(wordInputTopicName)
    val translations: KStream[String, String] = builder.stream(translationInputTopicName)

    val wordSplit: KStream[String, String] = textLines
      .flatMap { (_, v: String) =>
        val words = wordPattern.split(v.toLowerCase)
        List.from(words).map(w => (w, w))
      }

    val trTable: KTable[String, Translation] =
      translations.groupByKey.aggregate(Translation("", Set.empty[String]))(
        (aggKey, newTranslation: String, aggregated: Translation) =>
          Translation(aggKey, aggregated.translations + newTranslation)
      ) //(Materialized.`with`(Serdes.String(), translationSerde)) // <- using implicit Serdes

    // default join windows?
    val joined: KStream[String, Translation] =
      wordSplit.leftJoin(trTable)((_, translation) => translation)

    joined.to(translationOutputTopicName)

    builder.build()
  }

  def createTestTopics(topologyTestDriver: TopologyTestDriver): (
      TestInputTopic[Integer, String],
      TestInputTopic[String, String],
      TestOutputTopic[String, Translation]
  ) = {
    val inputTopic: TestInputTopic[Integer, String] = topologyTestDriver.createInputTopic(
      wordInputTopicName,
      Serdes.Integer().serializer(),
      Serdes.String().serializer()
    )
    val translationInputTopicEn: TestInputTopic[String, String] =
      topologyTestDriver.createInputTopic(
        translationInputTopicName,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )

    val translationOutputTopic: TestOutputTopic[String, Translation] = {
      topologyTestDriver.createOutputTopic(
        translationOutputTopicName,
        Serdes.String().deserializer(),
        // Needs an Decoder, can be provided by importing io.circe.generic.auto._
        CirceSerdes.deserializer[Translation]
      )
    }
    (inputTopic, translationInputTopicEn, translationOutputTopic)
  }

}
