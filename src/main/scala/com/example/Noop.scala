package com.example

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes._

object Noop {

  def createTopology(builder: StreamsBuilder, inputTopic: String, outputTopic: String): Topology = {

    val input: KStream[Int, Long] =
      builder.stream(inputTopic) //(Consumed.`with`(Serdes.Integer(), Serdes.Long()))
    input.to(outputTopic)
    builder.build()
  }

  case class Config(
      inputTopic: String = "FALLBACK_INPUT_TOPIC",
      outputTopic: String = "FALLBACK_OUTPUT_TOPIC",
      bootstrapServer: String = "FALLBACK_BOOTSTRAP_SERVER"
  )
}
