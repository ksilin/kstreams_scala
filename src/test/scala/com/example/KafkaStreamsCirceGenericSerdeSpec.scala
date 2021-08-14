package com.example

import org.apache.kafka.common.serialization.{ Deserializer, Serdes }
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }

import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.goyeau.kafka.streams.circe.CirceSerdes
import io.circe.generic.auto._
import com.goyeau.kafka.streams.circe.CirceSerdes._

class KafkaStreamsCirceGenericSerdeSpec extends SpecBase {

  val wordInputTopicName = "inputTopic"

  val translationInputTopicName  = "translationsInputTopic"
  val translationOutputTopicName = "translationOutputTopic"

  val inputValues = List(
    "this is the end",
  )

  val wordPattern: Pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val translationsEn: List[(String, String)] = List(
    ("this", "dies"),
    ("this", "es"),
    ("the", "der"),
    ("the", "die"),
    ("the", "das"),
    ("is", "ist"),
    ("end", "ende"),
    ("end", "schluss"),
  )

  val expectedTranslations = Map(
    "the"  -> Translation("the", Set("der", "die", "das")),
    "this" -> Translation("this", Set("dies", "es")),
    "is"   -> Translation("is", Set("ist")),
    "end"  -> Translation("end", Set("ende", "schluss"))
  )

  "must translate from english & russian to german" in {

    // --- topo

    val textLines: KStream[Int, String] = builder.stream(wordInputTopicName)

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

    // --- topo

    val topology: Topology = builder.build()
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

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

    val translationOutputTopic: TestOutputTopic[String, Translation] =
      topologyTestDriver.createOutputTopic(
        translationOutputTopicName,
        Serdes.String().deserializer(),
        // Needs an Decoder, can be provided by importing io.circe.generic.auto._
        CirceSerdes.deserializer[Translation]
      )

    translationsEn.foreach { case (k: String, v: String) =>
      translationInputTopicEn.pipeInput(k, v)
    }
    inputTopic.pipeValueList(inputValues.asJava)

    val outputRecords: mutable.Map[String, Translation] =
      translationOutputTopic.readKeyValuesToMap().asScala

    outputRecords must contain theSameElementsAs expectedTranslations
  }

}
