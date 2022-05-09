package com.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.{
  KeyValue,
  StreamsConfig,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}

import java.{ lang, util }
import scala.jdk.CollectionConverters._

class PoisonPillSpec extends SpecBase {

  // https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/

  val inputTopicName  = "inputTopic"
  val outputTopicName = "outputTopic"

  val dataBeforePoisonPill: Seq[KeyValue[Integer, lang.Long]] =
    (1 to 3) map (i => new KeyValue(i, i.longValue))
  val poisonPills: Seq[KeyValue[Integer, String]] = (4 to 6) map (i => new KeyValue(i, i.toString))
  val dataAfterPoisonPill: Seq[KeyValue[Integer, lang.Long]] =
    (7 to 9) map (i => new KeyValue(i, i.longValue))

  "must process messages before and after the poison pills" in {

    val topology: Topology =
      Noop.createTopology(builder, inputTopicName, outputTopicName)
    info(topology.describe())

    streamsConfiguration.put(
      StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      classOf[LogAndContinueExceptionHandler]
    )
    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTopicLong: TestInputTopic[Integer, lang.Long] = topologyTestDriver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      Serdes.Long().serializer()
    )

    val inputTopicPoison: TestInputTopic[Integer, String] = topologyTestDriver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      Serdes.String().serializer()
    )

    val outputTopic: TestOutputTopic[Integer, lang.Long] = topologyTestDriver.createOutputTopic(
      outputTopicName,
      Serdes.Integer().deserializer(),
      Serdes.Long().deserializer()
    )

    inputTopicLong.pipeKeyValueList(dataBeforePoisonPill.toList.asJava)
    inputTopicPoison.pipeKeyValueList(poisonPills.toList.asJava)
    inputTopicLong.pipeKeyValueList(dataAfterPoisonPill.toList.asJava)

    val outputRecords: util.Map[Integer, lang.Long] = outputTopic.readKeyValuesToMap()
    outputRecords.asScala.values must contain theSameElementsAs (dataBeforePoisonPill.map(
      _.value
    ) ++ dataAfterPoisonPill.map(_.value))

    // if you look in the logs, you will see how the errors of the poison pills are logged:
    // [2022-03-20T16:01:21,743Z] [WARN ] [RecordDeserializer] - stream-thread [ScalaTest-run-running-PoisonPillSpec] task [0_0] Skipping record due to deserialization error. topic=[inputTopic] partition=[0] offset=[5]
    //org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8
    //	at org.apache.kafka.common.serialization.LongDeserializer.deserialize(LongDeserializer.java:26)
  }

  "custom DeserializationExceptionHandler - poison pill records mus land in the DLQ" in {
    val topology: Topology =
      TotalCount.createTopology(builder, inputTopicName, outputTopicName)
    info(topology.describe())

    streamsConfiguration.put(
      StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      classOf[SendToDLQDeserializationExceptionHandler]
    )
    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTopicLong: TestInputTopic[Integer, lang.Long] = topologyTestDriver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      Serdes.Long().serializer()
    )

    val inputTopicPoison: TestInputTopic[Integer, String] = topologyTestDriver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      Serdes.String().serializer()
    )

    val dlqTopic: TestOutputTopic[lang.Integer, String] = topologyTestDriver.createOutputTopic(
      SendToDLQDeserializationExceptionHandler.DLQ_TOPIC_DEFAULT_NAME,
      Serdes.Integer().deserializer(),
      Serdes.String().deserializer()
    )

    val outputTopic: TestOutputTopic[lang.Long, lang.Long] = topologyTestDriver.createOutputTopic(
      outputTopicName,
      Serdes.Long().deserializer(),
      Serdes.Long().deserializer()
    )

    inputTopicLong.pipeKeyValueList(dataBeforePoisonPill.toList.asJava)
    inputTopicPoison.pipeKeyValueList(poisonPills.toList.asJava)
    inputTopicLong.pipeKeyValueList(dataAfterPoisonPill.toList.asJava)

    val dlqRecords: util.Map[lang.Long, lang.Long] = outputTopic.readKeyValuesToMap()
    dlqRecords.asScala foreach println

    val outputRecords: util.Map[lang.Long, lang.Long] = outputTopic.readKeyValuesToMap()
    outputRecords.asScala.values must contain theSameElementsAs (dataBeforePoisonPill.map(
      _.value
    ) ++ dataAfterPoisonPill.map(_.value))
  }

}
