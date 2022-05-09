package com.example

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.util.regex.Pattern

object TotalCount {

  def createTopology(builder: StreamsBuilder, inputTopic: String, outputTopic: String): Topology = {

    val textLines: KStream[Int, Long] =
      builder.stream(inputTopic) //(Consumed.`with`(Serdes.Integer(), Serdes.String()))

    val wordCounts: KTable[Long, Long] = textLines
      .peek((k, v) => println(s"processing $k : $v"))
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((_: Int, i: Long) => i) //(Grouped.`with`(Serdes.Integer(), Serdes.String()))
      .count()                         //(Materialized.`with`(Serdes.Integer(), Serdes.String()))

    wordCounts.toStream.to(outputTopic)
    builder.build()
  }

  case class Config(
      inputTopic: String = "FALLBACK_INPUT_TOPIC",
      outputTopic: String = "FALLBACK_OUTPUT_TOPIC",
      bootstrapServer: String = "FALLBACK_BOOTSTRAP_SERVER"
  )
}
