package com.example.statestore

import com.example.SpecBase
import com.example.punctuate.Transformers
import com.example.serde.{GsonDeserializer, GsonSerializer}
import com.google.gson.annotations.JsonAdapter
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.MockProcessorContext
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.intSerde
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

import java.time.{Duration, Instant}
import java.lang
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class TimePredicateStoreCleanerSpec extends SpecBase {

  case class MyRecord(name: String, description: String, from: lang.Long, to: lang.Long)

  val storeName       = s"${suiteName}_store"
  val inputTopicName  = s"${suiteName}_inputTopic"
  val outputTopicName = s"${suiteName}_outputTopic"

  val now: Long                     = System.currentTimeMillis()
  val punctuationInterval: Duration = Duration.ofSeconds(1)

  val jsonSerializer                        = new GsonSerializer[MyRecord]
  val jsonDeserializer                      = new GsonDeserializer[MyRecord](classOf[MyRecord])
  val myRecordSerde: WrapperSerde[MyRecord] = new WrapperSerde(jsonSerializer, jsonDeserializer)

  val deleteIfTrue: (MyRecord, Long) => Boolean = (r, currentTimestamp) => {
    val now  = Instant.ofEpochMilli(currentTimestamp)
    val from = Instant.ofEpochMilli(r.from)
    val to   = Instant.ofEpochMilli(r.to)
    info(s"comparing range ${from} - ${to} to $now")
    now.isAfter(to) || now.isBefore(from)
  }

  val notYet = new KeyValue(
    1,
    MyRecord(
      Random.alphanumeric.take(5).mkString,
      Random.alphanumeric.take(10).mkString,
      from = (now + 10000L),
      to = (now + 100000L)
    )
  )
  val rightNow = new KeyValue(
    2,
    MyRecord(
      Random.alphanumeric.take(5).mkString,
      Random.alphanumeric.take(10).mkString,
      from = (now - 10000L),
      to = (now + 10000L)
    )
  )
  val notAnymore = new KeyValue(
    3,
    MyRecord(
      Random.alphanumeric.take(5).mkString,
      Random.alphanumeric.take(10).mkString,
      from = (now - 20000L),
      to = (now - 10000L)
    )
  )
  val data: Seq[KeyValue[Int, MyRecord]] = List(notYet, rightNow, notAnymore)

  "must remove not yet valid and no longer valid record - tested with topology" in {

    val topology: Topology = createTopology(builder, inputTopicName, outputTopicName, storeName)
    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)
    val outputTopic        = prepTestData(topologyTestDriver, inputTopicName, outputTopicName, data.toList)

      info("advancing time by 2 sec")
      topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(2))

    val store: KeyValueStore[Integer, MyRecord] =
      topologyTestDriver.getKeyValueStore[Integer, MyRecord](storeName)
    val storeContents: List[KeyValue[Integer, MyRecord]] = store.all().asScala.toList
    storeContents.size mustBe 1
  }

  "must remove not yet valid and no longer valid record - tested with MockProcessorContext" in {

    val context: MockProcessorContext =
      new MockProcessorContext //[Int, MyRecord] = new MockProcessorContext()
    val store: KeyValueStore[Int, MyRecord] =
      Stores
        .keyValueStoreBuilder(
          Stores.inMemoryKeyValueStore(storeName),
          Serdes.intSerde,
          myRecordSerde
        )
        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
        .build()
    // deprecated - use StateStoreContext after 3.3 is out
    store.init(context, store)
    context.register(store, null)
    //context.addStateStore(store)

    val transformer: TimePredicateStoreCleaner[Int, MyRecord] =
      new TimePredicateStoreCleaner[Int, MyRecord](punctuationInterval, deleteIfTrue, storeName)
    transformer.init(context)

    store.putAll(data.toList.asJava)

    store.all().asScala.size mustBe data.size

    // punctuate manually as MockCtx does not do that
    val punctuators = context.scheduledPunctuators().asScala
    punctuators foreach { p =>
      p.getPunctuator.punctuate(now + 2500L)
    }
    // expect single record (with key 2) to be deleted
    store.all().asScala.size mustBe (1)
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

    val input: KStream[Int, MyRecord] =
      builder.stream(inputTopic)(
        Consumed.`with`(intSerde, myRecordSerde)
      )

    val storeTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] = () =>
      Transformers.storeTransformer(storeName)
    val ttlTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] = () =>
      TimePredicateStoreCleaner(punctuationInterval, deleteIfTrue, storeName)
    val stored                              = input.transform(storeTransformSupplier, storeName)
    val transformed: KStream[Int, MyRecord] = stored.transform(ttlTransformSupplier, storeName)
    transformed.to(outputTopic)(Produced.`with`(intSerde, myRecordSerde))
    builder.build()
  }

  def prepTestData(
      driver: TopologyTestDriver,
      inputTopicName: String,
      outputTopicName: String,
      data: List[KeyValue[Int, MyRecord]]
  ): TestOutputTopic[Int, MyRecord] = {
    val testInputTopic: TestInputTopic[Int, MyRecord] = driver.createInputTopic(
      inputTopicName,
      Serdes.intSerde.serializer(),
      myRecordSerde.serializer()
    )

    val outputTopic: TestOutputTopic[Int, MyRecord] = driver.createOutputTopic(
      outputTopicName,
      Serdes.intSerde.deserializer(),
      myRecordSerde.deserializer()
    )

    testInputTopic.pipeKeyValueList(data.asJava)
    outputTopic
  }

}
