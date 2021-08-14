package com.example

import org.apache.kafka.common.serialization.{
  IntegerDeserializer,
  IntegerSerializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.streams.{
  KeyValue,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.util
import scala.jdk.CollectionConverters._

class SimplestPipeSpec extends SpecBase {

  val INPUT_TOPIC  = "input"
  val OUTPUT_TOPIC = "output"

  "must pipe values from input to output topic without modification" in {

    // new KeyValue[Integer, String] not needed
    val inputValues: List[(Integer, String)] = List((1, "v1"), (1, "v1"), (1, "v2"))

    val inputStream: KStream[Int, String] = builder.stream(INPUT_TOPIC) //(Consumed.`with`)

    inputStream
      .peek((k, v) => info(s"peeking into input stream: ${k} : ${v}"))
      .mapValues { v =>
        v
      }
      .to(OUTPUT_TOPIC) //, Produced.`with`(Serdes.Integer(), Serdes.String))

    val topology: Topology = builder.build()
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTopic: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC, new IntegerSerializer(), new StringSerializer())

    val outputTopic: TestOutputTopic[Integer, String] = topologyTestDriver
      .createOutputTopic(OUTPUT_TOPIC, new IntegerDeserializer(), new StringDeserializer())

    val testRecords = inputValues.map { case (k, v) => new KeyValue(k, v) }.asJava
    inputTopic.pipeKeyValueList(testRecords)

    val outputRecords: util.List[KeyValue[Integer, String]] = outputTopic.readKeyValuesToList()

    outputRecords mustBe testRecords
  }

}
