package com.example

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder

import java.util.regex.Pattern
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable, Materialized }
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._

object WordCount {

  val wordPattern: Pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  def createTopology(builder: StreamsBuilder, inputTopic: String, outputTopic: String): Topology = {

    val textLines: KStream[Int, String] =
      builder.stream(inputTopic) //(Consumed.`with`(Serdes.Integer(), Serdes.String()))

    val wordCounts: KTable[String, Long] = textLines
      .peek((k, v) => println(s"processing $k : $v"))
      .flatMapValues((v: String) => List.from(wordPattern.split(v.toLowerCase)))
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((_: Int, word: String) => word) //(Grouped.`with`(Serdes.Integer(), Serdes.String()))
      .count()                                 //(Materialized.`with`(Serdes.Integer(), Serdes.String()))

    wordCounts.toStream.to(outputTopic)
    builder.build()
  }

  case class WordCountConfig(
      inputTopic: String = "FALLBACK_INPUT_TOPIC",
      outputTopic: String = "FALLBACK_OUTPUT_TOPIC",
      bootstrapServer: String = "FALLBACK_BOOTSTRAP_SERVER"
  )
}
