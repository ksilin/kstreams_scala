package com.example.join

import com.example.SpecBase
import com.example.util.KafkaSpecHelper
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.kstream.{JoinWindows, Suppressed, TimeWindows, Windowed, WindowedSerdes}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.testcontainers.containers.Network
import org.testcontainers.shaded.org.apache.commons.io.FileUtils

import java.io.File
import java.time.{Duration, Instant}
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class JoinSuppressKeyAndDelaySpec extends SpecBase {

  case class Parcel(id: String, parts: List[String], createdAt: Long, updatedAt: Long)

  val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  val bootstrap: String = broker.getBootstrapServers

  val parcelSerializer: Serializer[Parcel]     = implicitly
  val parcelDeserializer: Deserializer[Parcel] = implicitly
  val parcelSerde: Serde[Parcel]               = implicitly

  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName)
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  val fetchMaxAttempts = 50

  // StreamA: |-[A,A]---------[B,B]----------[A,A]------|
  // StreamB: |--------[A,a]---------[C,c]--------[D,d]-|

  // -> [A,(A,a)],[B,(B,null)]

  // StreamA: |-[A,A]---------[B,B]----------[C,C]------|
  // StreamB: |--------[A,a]---------[C,c]--------[D,d]-|

  private val leader_flow = "leader_flow"
  private val follower_flow = "follower_flow"
  private val leader_follower_join_flow = "leader_follower_join_flow"

  def getTopology(joinWindowDuration: Duration, reduceWindowDuration: Duration): Topology = {
    val builder = new StreamsBuilder()

    val consumed = Consumed.`with`[String, String](Serdes.stringSerde, Serdes.stringSerde)
    val windowedSerde = new WindowedSerdes.TimeWindowedSerde(Serdes.stringSerde, reduceWindowDuration.toMillis)

    val produced = Produced.`with`[Windowed[String], String](windowedSerde, Serdes.stringSerde)
    val leftStream = builder.stream[String, String](leader_flow)(consumed)
    val rightStream = builder.stream[String, String](follower_flow)(consumed)

    val joinedStream: KStream[String, String] = leftStream.leftJoin(rightStream)(
      {(leftV, rightV) =>
         println(s"joining $leftV with $rightV")
        "[" + leftV + "," + rightV + "]"
      }, JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration)
    )(StreamJoined.`with`[String, String, String](Serdes.stringSerde, Serdes.stringSerde, Serdes.stringSerde))

    val materialized: Materialized[Windowed[String], String, ByteArrayKeyValueStore] = Materialized.`with`(windowedSerde, Serdes.stringSerde).withCachingDisabled()

    joinedStream
      .peek((key, value) => println(s"-------\n\nBEFORE - key=$key, value=$value"))
      .groupByKey
      .windowedBy(TimeWindows.ofSizeWithNoGrace(reduceWindowDuration))
      .reduce(_ + _)
      .toStream
      .peek((key, value) => println(s"-------\n\nREDUCED - key=$key, value=$value"))
      .toTable(materialized)
      .suppress(Suppressed.untilWindowCloses(unbounded()).withName("suppression"))
      .toStream
      .peek((key, value) => println(s"AFTER - key=$key, value=$value"))
      .to(leader_follower_join_flow)(produced)

    builder.build()
  }

  val now: Long = System.currentTimeMillis()

  val leaderRecord1 = new ProducerRecord[String, String]( leader_flow, 0, now, "A", "LA")
  val leaderRecord2 = new ProducerRecord[String, String]( leader_flow, 0, now + 2000, "B", "LB")
  val leaderRecord3a = new ProducerRecord[String, String]( leader_flow, 0, now + 20000, "A", "LA")
  val leaderRecord3c = new ProducerRecord[String, String]( leader_flow, 0, now + 20000, "C", "LC")

  val followerRecord1 = new ProducerRecord[String, String]( follower_flow, 0, now + 1000, "A", "FA")
  val followerRecord2 = new ProducerRecord[String, String]( follower_flow, 0 , now + 8000,  "C", "FC")
  val followerRecord3 = new ProducerRecord[String, String]( follower_flow, 0 , now + 20000,  "D", "FD")

  "test suppress topo - no delay, closing with A" in {

    createOrTruncateTestTopics(adminClient)

    val joinWindowDuration = Duration.ofSeconds(2)
    val reduceWindowDuration = Duration.ofSeconds(2)

    val suffix = "nodelayA"
    val streams: KafkaStreams = startTestTopology(joinWindowDuration, reduceWindowDuration, suffix)
    val consumer = createAndSubscribeConsumer(suffix)

    produceRecords(delay = 0, finalLeaderRecord = leaderRecord3a)

    KafkaSpecHelper.fetchAndProcessRecords(consumer, maxAttempts = fetchMaxAttempts)
    deleteStateStoreDir
    streams.close()
  }

  "test suppress topo - delay, closing with A" in {

    createOrTruncateTestTopics(adminClient)

    val joinWindowDuration = Duration.ofSeconds(2)
    val reduceWindowDuration = Duration.ofSeconds(2)

    val suffix = "delayA"
    val streams: KafkaStreams = startTestTopology(joinWindowDuration, reduceWindowDuration, suffix)

    val consumer = createAndSubscribeConsumer("suffix")

    produceRecords(delay = 1000, finalLeaderRecord = leaderRecord3a)

    KafkaSpecHelper.fetchAndProcessRecords(consumer, maxAttempts = fetchMaxAttempts)
    deleteStateStoreDir
    streams.close()
  }

  private def deleteStateStoreDir() = {
    try {
      val str = streamsConfiguration.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
      FileUtils.deleteDirectory(new File("/tmp/kafka-streams/" + str))
      FileUtils.forceMkdir(new File("/tmp/kafka-streams/" + str))
    } catch {
      case e: Exception => logger.error(e.toString)
    }
  }

  "test suppress topo - no delay, closing with C" in {

    createOrTruncateTestTopics(adminClient)

    val joinWindowDuration = Duration.ofSeconds(2)
    val reduceWindowDuration = Duration.ofSeconds(2)
    val suffix = "nodelayC"
    val streams: KafkaStreams = startTestTopology(joinWindowDuration, reduceWindowDuration, suffix)
    val consumer = createAndSubscribeConsumer(suffix)

    produceRecords(delay = 0, finalLeaderRecord = leaderRecord3c)

    KafkaSpecHelper.fetchAndProcessRecords(consumer, maxAttempts = fetchMaxAttempts)
    deleteStateStoreDir
    streams.close()
  }

  "test suppress topo - delay, closing with C" in {

    createOrTruncateTestTopics(adminClient)

    val joinWindowDuration = Duration.ofSeconds(2)
    val reduceWindowDuration = Duration.ofSeconds(2)

    val suffix = "delayC"
    val streams: KafkaStreams = startTestTopology(joinWindowDuration, reduceWindowDuration, suffix)
    val consumer = createAndSubscribeConsumer(suffix)

    produceRecords(delay = 1000, finalLeaderRecord = leaderRecord3c)

    KafkaSpecHelper.fetchAndProcessRecords(consumer, maxAttempts = fetchMaxAttempts)
    deleteStateStoreDir
    streams.close()
  }


  private def createAndSubscribeConsumer(suffix: String) = {
    streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}_${suffix}_${Random.alphanumeric.take(10).mkString}")
    val consumer = new KafkaConsumer[String, String](
      streamsConfiguration,
      Serdes.stringSerde.deserializer,
      Serdes.stringSerde.deserializer
    )
    consumer.subscribe(List(leader_follower_join_flow).asJavaCollection)
    consumer
  }

  private def startTestTopology(joinWindowDuration: Duration, reduceWindowDuration: Duration, appIdSuffix: String) = {
    val topology = getTopology(joinWindowDuration, reduceWindowDuration)
    println(topology.describe())

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, s"$suiteName-$appIdSuffix")
    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.cleanUp()
    streams.start()
    streams
  }

  private def createOrTruncateTestTopics(adminClient: AdminClient): Unit = {
    val leaderTopicCreated = KafkaSpecHelper.createOrTruncateTopic(adminClient, leader_flow, 1, 1)
    println(s"leaderTopicCreated: $leaderTopicCreated")
    val followerTopicCreated = KafkaSpecHelper.createOrTruncateTopic(adminClient, follower_flow, 1, 1)
    println(s"followerTopicCreated: $followerTopicCreated")
    val outTopicCreated = KafkaSpecHelper.createOrTruncateTopic(adminClient, leader_follower_join_flow, 1, 1)
    println(s"outTopicCreated: $outTopicCreated")
  }

  def produceRecords(delay: Int, finalLeaderRecord: ProducerRecord[String, String]): Unit = {

    // produce data
    val producer = new KafkaProducer[String, String](
      streamsConfiguration,
      Serdes.stringSerde.serializer,
      Serdes.stringSerde.serializer
    )

    produceRecord(producer, leaderRecord1, delay)
    //produceRecord(producer, leaderRecord1, delay)
    produceRecord(producer, followerRecord1, delay)

    produceRecord(producer, leaderRecord2, delay)
    produceRecord(producer, followerRecord2, delay)

    produceRecord(producer, finalLeaderRecord, delay)
    produceRecord(producer, followerRecord3, delay)
  }

  def produceRecord(producer: KafkaProducer[String, String], record: ProducerRecord[String, String], delay: Int = 0): Unit ={
    val rm = producer.send(record).get()
    println(Instant.ofEpochMilli(rm.timestamp()))
    Thread.sleep(delay)
  }
}
