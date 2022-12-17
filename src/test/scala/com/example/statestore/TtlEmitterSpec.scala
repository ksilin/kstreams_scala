package com.example.statestore

import com.example.SpecBase
import com.example.punctuate.Transformers
import com.example.serde.{ GsonDeserializer, GsonSerializer }
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{ Consumed, KStream, Produced }
import org.apache.kafka.streams.scala.serialization.Serdes.{ intSerde, longSerde }
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }

import java.time.{ Duration, Instant }
import java.{ lang, util }
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class TtlEmitterSpec extends SpecBase {

  case class MyRecord(name: String, description: String, timestamp: lang.Long)

  val storeName       = s"${suiteName}_store"
  val purgeStoreName  = s"${storeName}_purge"
  val inputTopicName  = s"${suiteName}_inputTopic"
  val outputTopicName = s"${suiteName}_outputTopic"

  val now: Long                     = System.currentTimeMillis()
  val ttl                           = 3000
  val punctuationInterval: Duration = Duration.ofSeconds(1)

  val tsExtractor: MyRecord => lang.Long = r => r.timestamp

  val jsonSerializer                        = new GsonSerializer[MyRecord]
  val jsonDeserializer                      = new GsonDeserializer[MyRecord](classOf[MyRecord])
  val myRecordSerde: WrapperSerde[MyRecord] = new WrapperSerde(jsonSerializer, jsonDeserializer)

  private val ids: List[Int] = (1 to 3).toList
  val data: Seq[KeyValue[Integer, MyRecord]] =
    ids map (i =>
      new KeyValue(
        i,
        MyRecord(
          Random.alphanumeric.take(10).mkString,
          Random.alphanumeric.take(30).mkString,
          (now - i * 1000L)
        )
      )
    )

  "must remove oldest record after TTL expires - tested with topology" in {

    val topology: Topology = createTopology(builder, inputTopicName, outputTopicName, storeName)
    // info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)
    val outputTopic        = prepTestData(topologyTestDriver, inputTopicName, outputTopicName, data.toList)

    (1 to 3).foreach { _ =>
      info("advancing time by 1 sec")
      topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1))
    }

    val outputRecords: util.Map[Integer, MyRecord] = outputTopic.readKeyValuesToMap()
    info("output records: ")
    outputRecords.asScala foreach (r => info(r))
    info("data value: ")
    data foreach (r => info(r.value))

    val store: KeyValueStore[Integer, MyRecord] =
      topologyTestDriver.getKeyValueStore[Integer, MyRecord](storeName)
    val storeContents: List[KeyValue[Integer, MyRecord]] = store.all().asScala.toList
    storeContents.size mustBe 1
  }

  def createTopology(
      builder: StreamsBuilder,
      inputTopic: String,
      outputTopic: String,
      storeName: String
  ): Topology = {

    val keyValueStoreBuilder: StoreBuilder[KeyValueStore[Int, MyRecord]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        intSerde,
        myRecordSerde
      )
    builder.addStateStore(keyValueStoreBuilder)

    val purgeStoreBuilder: StoreBuilder[KeyValueStore[Int, Long]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(purgeStoreName),
        intSerde,
        longSerde
      )
    builder.addStateStore(purgeStoreBuilder)

    val input: KStream[Int, MyRecord] =
      builder.stream(inputTopic)(
        Consumed.`with`(intSerde, myRecordSerde)
      ) //(Consumed.`with`(Serdes.Integer(), Serdes.Long()))

    val ttlEmitterTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] =
      () =>
        new TTLEmitter(
          Duration.ofMillis(ttl),
          punctuationInterval,
          purgeStoreName,
          PunctuationType.WALL_CLOCK_TIME
        )
    val storeTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] = () =>
      Transformers.storeAndDeleteTransformer(
        storeName
      ) //.(punctuationInterval, ttl, tsExtractor, storeName)

    // writes tombstones back to input topic
    val ttlEmitter: Unit = input
      .transform(ttlEmitterTransformSupplier, purgeStoreName)
      .to(inputTopicName)(Produced.`with`(intSerde, myRecordSerde))

    val stored = input.transform(storeTransformSupplier, storeName)

    stored.to(outputTopic)(Produced.`with`(intSerde, myRecordSerde))
    builder.build()
  }

  def prepTestData(
      driver: TopologyTestDriver,
      inputTopicName: String,
      outputTopicName: String,
      data: List[KeyValue[Integer, MyRecord]]
  ): TestOutputTopic[Integer, MyRecord] = {
    val testInputTopic: TestInputTopic[Integer, MyRecord] = driver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      myRecordSerde.serializer()
    )

    val outputTopic: TestOutputTopic[Integer, MyRecord] = driver.createOutputTopic(
      outputTopicName,
      Serdes.Integer().deserializer(),
      myRecordSerde.deserializer()
    )

    testInputTopic.pipeKeyValueList(
      data.asJava,
      Instant.ofEpochMilli(data.head.value.timestamp),
      Duration.ofMillis(1000)
    )
    outputTopic
  }

}
