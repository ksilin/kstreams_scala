package com.example.join

import com.example.SpecBase
import com.example.serde.{ GsonDeserializer, GsonSerializer }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.{ ByteArrayKeyValueStore, StreamsBuilder }
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }

import java.time.Duration
import java.{ lang, util }
import scala.jdk.CollectionConverters._

class JoinLookbackSpec extends SpecBase {

  case class MyRecord(name: String, description: String, ts: lang.Long)

  val storeName       = s"${suiteName}_store"
  val leftTopicName   = s"${suiteName}_leftTopic"
  val rightTopicName  = s"${suiteName}_rightTopic"
  val resultTopicName = s"${suiteName}_resultTopic"

  val now: Long = System.currentTimeMillis()

  val jsonSerializer                        = new GsonSerializer[MyRecord]
  val jsonDeserializer                      = new GsonDeserializer[MyRecord](classOf[MyRecord])
  val myRecordSerde: WrapperSerde[MyRecord] = new WrapperSerde(jsonSerializer, jsonDeserializer)

  // https://kafka.apache.org/32/documentation/streams/developer-guide/dsl-api.html#kstream-kstream-join

  "stream-stream inner" in {

    val topology: Topology =
      createStreamStreamInnerJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)
    info(topology.describe())

    runTest(topology)
  }

  "stream-stream left" in {

    val topology: Topology =
      createStreamStreamLeftJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)
    info(topology.describe())

    runTest(topology)
  }

  "stream-stream outer" in {

    val topology: Topology =
      createStreamStreamOuterJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)
    info(topology.describe())

    runTest(topology)
  }


  "stream-table inner" in {

    val topology: Topology =
      createStreamTableInnerJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)
    info(topology.describe())

    runTest(topology)
  }

  // almost exactly like a S-T join
  "stream-globalKTable inner" in {

    val topology: Topology =
      createStreamTableInnerJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)
    info(topology.describe())

    runTest(topology)
  }

  "table-table inner" in {

    val topology: Topology =
      createTableTableInnerJoinTopology(builder, leftTopicName, rightTopicName, resultTopicName)

    runTest(topology)
  }


  private def runTest(topology: Topology): Unit = {
    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val (leftTopic, rightTopic, resultTopic) = createTopics(topologyTestDriver)

    produceRecordsFetchJoinResults(leftTopic, rightTopic, resultTopic)
  }

  private def produceRecordsFetchJoinResults(
      leftTopic: TestInputTopic[Integer, MyRecord],
      rightTopic: TestInputTopic[Integer, MyRecord],
      resultTopic: TestOutputTopic[String, MyRecord]
  ): Unit = {

    val now = System.currentTimeMillis()

    val left1 = MyRecord(name = "l1", description = "A", ts = now - Duration.ofMinutes(10).toMillis)
    val left2 = MyRecord(name = "l2", description = "B", ts = now - Duration.ofMinutes(8).toMillis)
    val left4 = MyRecord(name = "l3", description = "C", ts = now - Duration.ofMinutes(6).toMillis)
    val left6 = MyRecord(name = "l4", description = "D", ts = now - Duration.ofMinutes(2).toMillis)

    val right1 = MyRecord(name = "r1", description = "a", ts = now - Duration.ofMinutes(9).toMillis)
    val right2 = MyRecord(name = "r2", description = "b", ts = now - Duration.ofMinutes(7).toMillis)
    val right4 = MyRecord(name = "r3", description = "c", ts = now - Duration.ofMinutes(5).toMillis)
    val right6 = MyRecord(name = "r4", description = "d", ts = now - Duration.ofMinutes(3).toMillis)

    //info("produce first record left")
    leftTopic.pipeInput(1, left1)
    fetchAndPrintResults(resultTopic, "after first record left")

    //info("produce first record right")
    rightTopic.pipeInput(1, right1)
    fetchAndPrintResults(resultTopic, "after first record right")

    //info("produce second record left")
    leftTopic.pipeInput(1, left2)
    fetchAndPrintResults(resultTopic, "after second record left")

    //info("produce second record right")
    rightTopic.pipeInput(1, right2)
    fetchAndPrintResults(resultTopic, "after second record right")

    //info("produce third record left - tombstone")
    leftTopic.pipeInput(1, null)
    fetchAndPrintResults(resultTopic, "after third record left - tombstone")

    //info("produce third record right - tombstone")
    rightTopic.pipeInput(1, null)
    fetchAndPrintResults(resultTopic, "after third record right - tombstone")

    //info("produce fourth record left")
    leftTopic.pipeInput(1, left4)
    fetchAndPrintResults(resultTopic, "after fourth record left")

    //info("produce fourth record right")
    rightTopic.pipeInput(1, right4)
    fetchAndPrintResults(resultTopic, "after fourth record right")

    //

    // info("produce fifth record right - tombstone")
    rightTopic.pipeInput(1, null)
    fetchAndPrintResults(resultTopic, "after fifth record right - tombstone")

    // info("produce fifth record left - tombstone")
    leftTopic.pipeInput(1, null)
    fetchAndPrintResults(resultTopic, "after fifth record left - tombstone")

    // info("produce sixth record right")
    rightTopic.pipeInput(1, right6)
    fetchAndPrintResults(resultTopic, "after sixth record right")

    // info("produce sixth record left")
    leftTopic.pipeInput(1, left6)
    fetchAndPrintResults(resultTopic, "after sixth record left")
  }

  private def fetchAndPrintResults(
      resultTopic: TestOutputTopic[String, MyRecord],
      tag: String = ""
  ): Unit = {
    val values = resultTopic.readValuesToList().asScala
    info(s"fetched ${values.size} values $tag:")
    values foreach { r =>
      if (null == r) warn("tombstone join result")
      else info(r)
    }
  }

  def createTopics(topologyTestDriver: TopologyTestDriver): (
      TestInputTopic[Integer, MyRecord],
      TestInputTopic[Integer, MyRecord],
      TestOutputTopic[String, MyRecord]
  ) = {
    val leftTopic: TestInputTopic[Integer, MyRecord] = topologyTestDriver.createInputTopic(
      leftTopicName,
      Serdes.Integer().serializer(),
      jsonSerializer
    )
    val rightTopic: TestInputTopic[Integer, MyRecord] = topologyTestDriver.createInputTopic(
      rightTopicName,
      Serdes.Integer().serializer(),
      jsonSerializer
    )

    val resultTopic: TestOutputTopic[String, MyRecord] = topologyTestDriver.createOutputTopic(
      resultTopicName,
      Serdes.String().deserializer(),
      jsonDeserializer
    )
    (leftTopic, rightTopic, resultTopic)
  }

  val tsExtractor = new TimestampExtractor {
    override def extract(record: ConsumerRecord[AnyRef, AnyRef], partitionTime: Long): Long =
      if (null == record.value()) partitionTime
      else record.value().asInstanceOf[MyRecord].ts
  }

  val consumed =
    Consumed.`with`(Serdes.Integer(), myRecordSerde).withTimestampExtractor(tsExtractor)
  val joiner = { (vl: MyRecord, vr: MyRecord) =>
    if (null == vl) {
      warn(s"null from left, right value: $vr")
      null
    } else if (null == vr) {
      warn(s"null from right, left value: $vl")
      null
    } else {
      //info(s"joining values:")
      //info(s"left: $vl")
      //info(s"right: $vr")
      MyRecord(
        name = s"${vl.name} + ${vr.name}",
        description = s"${vl.description} + ${vr.description}",
        ts = Math.max(vl.ts, vr.ts)
      )
    }
  }
  val joinWindows: JoinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofDays(100))
  val streamJoined: StreamJoined[Integer, MyRecord, MyRecord] =
    StreamJoined.`with`(Serdes.Integer(), myRecordSerde, myRecordSerde)
  val joined: Joined[Integer, MyRecord, MyRecord] =
    Joined.`with`(Serdes.Integer(), myRecordSerde, myRecordSerde)
  val materilaized: Materialized[Integer, MyRecord, ByteArrayKeyValueStore] =
    Materialized.`with`(Serdes.Integer(), myRecordSerde)

  def createStreamStreamInnerJoinTopology(
      builder: StreamsBuilder,
      leftTopicName: String,
      rightTopicName: String,
      outputTopicName: String
  ): Topology = {

    val leftStream: KStream[Integer, MyRecord]  = builder.stream(leftTopicName)(consumed)
    val rightStream: KStream[Integer, MyRecord] = builder.stream(rightTopicName)(consumed)

    val joinedStream: KStream[Integer, MyRecord] =
      leftStream.join(rightStream)(joiner, joinWindows)(streamJoined)

    joinedStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }

  def createStreamStreamLeftJoinTopology(
      builder: StreamsBuilder,
      leftTopicName: String,
      rightTopicName: String,
      outputTopicName: String
  ): Topology = {

    val leftStream: KStream[Integer, MyRecord] = builder.stream(leftTopicName)(consumed)
    val rightStream: KStream[Integer, MyRecord] = builder.stream(rightTopicName)(consumed)

    val joinedStream: KStream[Integer, MyRecord] =
      leftStream.leftJoin(rightStream)(joiner, joinWindows)(streamJoined)

    joinedStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }

  def createStreamStreamOuterJoinTopology(
                                          builder: StreamsBuilder,
                                          leftTopicName: String,
                                          rightTopicName: String,
                                          outputTopicName: String
                                        ): Topology = {

    val leftStream: KStream[Integer, MyRecord] = builder.stream(leftTopicName)(consumed)
    val rightStream: KStream[Integer, MyRecord] = builder.stream(rightTopicName)(consumed)

    val joinedStream: KStream[Integer, MyRecord] =
      leftStream.outerJoin(rightStream)(joiner, joinWindows)(streamJoined)

    joinedStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }


  def createStreamTableInnerJoinTopology(
      builder: StreamsBuilder,
      leftTopicName: String,
      rightTopicName: String,
      outputTopicName: String
  ): Topology = {

    val leftStream: KStream[Integer, MyRecord] = builder.stream(leftTopicName)(consumed)
    val rightTable: KTable[Integer, MyRecord]  = builder.table(rightTopicName)(consumed)

    val joinedStream: KStream[Integer, MyRecord] = leftStream.join(rightTable)(joiner)(joined)

    joinedStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }

  def createStreamGlobalKTableInnerJoinTopology(
                                          builder: StreamsBuilder,
                                          leftTopicName: String,
                                          rightTopicName: String,
                                          outputTopicName: String
                                        ): Topology = {

    val leftStream: KStream[Integer, MyRecord] = builder.stream(leftTopicName)(consumed)
    val rightTable: GlobalKTable[Integer, MyRecord]  = builder.globalTable(rightTopicName)(consumed)

    // KV-mapper required
    // bc NO copartitioning
    val mapper = { (k: Integer, v: MyRecord) => k}

    val joinedStream: KStream[Integer, MyRecord] = leftStream.join(rightTable)(mapper, joiner)

    joinedStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }


  def createTableTableInnerJoinTopology(
      builder: StreamsBuilder,
      leftTopicName: String,
      rightTopicName: String,
      outputTopicName: String
  ): Topology = {

    val leftTable: KTable[Integer, MyRecord]  = builder.table(leftTopicName)(consumed)
    val rightTable: KTable[Integer, MyRecord] = builder.table(rightTopicName)(consumed)

    val joinedTable: KTable[Integer, MyRecord] = leftTable.join(rightTable, materilaized)(joiner)

    joinedTable.toStream.to(outputTopicName)(Produced.`with`(Serdes.Integer(), myRecordSerde))

    builder.build()
  }

  //def createCountingStateDedupTopology(builder: StreamsBuilder, leftTopicName: String, outputTopicName: String): Unit

}
