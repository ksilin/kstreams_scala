package com.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }

import java.{ lang, util }
import scala.jdk.CollectionConverters._

class WordCountSimpleSpec extends SpecBase {

  val wordInputTopicName  = "inputTopic"
  val wordOutputTopicName = "outputTopic"

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit",
    "И теперь пошли русские слова"
  )

  val expectedWordCounts: Map[String, Long] = Map(
    "hello"   -> 1L,
    "all"     -> 1L,
    "streams" -> 2L,
    "lead"    -> 1L,
    "to"      -> 1L,
    "join"    -> 1L,
    "kafka"   -> 3L,
    "summit"  -> 1L,
    "и"       -> 1L,
    "теперь"  -> 1L,
    "пошли"   -> 1L,
    "русские" -> 1L,
    "слова"   -> 1L
  )

  "must count words across messages in topic" in {

    val topology: Topology =
      WordCount.createTopology(builder, wordInputTopicName, wordOutputTopicName)
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTopic: TestInputTopic[Integer, String] = topologyTestDriver.createInputTopic(
      wordInputTopicName,
      Serdes.Integer().serializer(),
      Serdes.String().serializer()
    )
    val outputTopic: TestOutputTopic[String, lang.Long] = topologyTestDriver.createOutputTopic(
      wordOutputTopicName,
      Serdes.String().deserializer(),
      Serdes.Long().deserializer()
    )

    inputTopic.pipeValueList(inputValues.asJava)

    val outputRecords: util.Map[String, lang.Long] = outputTopic.readKeyValuesToMap()
    outputRecords.asScala must contain theSameElementsAs expectedWordCounts
  }

}
