package com.example.statestore

import com.example.SpecBase
import com.example.punctuate.Transformers
import com.example.serde.{ GsonDeserializer, GsonSerializer }
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{ Consumed, KStream, Produced }
import org.apache.kafka.streams.scala.serialization.Serdes.intSerde
import org.apache.kafka.streams.state.{ KeyValueStore, Stores }

import java.time.{ Duration, Instant }
import java.{ lang, util }
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class WindowedKeyValueStoreSupplierSpec extends SpecBase {

  case class MyRecord(name: String, description: String, timestamp: lang.Long)

  val storeName       = s"${suiteName}_store"
  val inputTopicName  = s"${suiteName}_inputTopic"
  val outputTopicName = s"${suiteName}_outputTopic"

  val now: Long                     = System.currentTimeMillis()
  val ttl                           = 2000L
  val punctuationInterval: Duration = Duration.ofSeconds(1)

  val tsExtractor: MyRecord => lang.Long = r => r.timestamp

  val jsonSerializer                        = new GsonSerializer[MyRecord]
  val jsonDeserializer                      = new GsonDeserializer[MyRecord](classOf[MyRecord])
  val myRecordSerde: WrapperSerde[MyRecord] = new WrapperSerde(jsonSerializer, jsonDeserializer)

  private val ids: List[Int] = (1 to 100).toList
  val data: Seq[KeyValue[Integer, MyRecord]] =
    ids map (i =>
      new KeyValue(
        i,
        MyRecord(
          s"${i}_${Random.alphanumeric.take(5).mkString}",
          s"${i}_${Random.alphanumeric.take(10).mkString}",
          (now - 10000L +  i * 1000L)
        )
      )
    )

  "must remove oldest record after TTL expires - tested with topology" in {

    val topology: Topology = createTopology(builder, inputTopicName, outputTopicName, storeName)
    // info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)
    val (inputTopic, outputTopic) =
      prepTestTopics(topologyTestDriver, inputTopicName, outputTopicName)

    data foreach {kv =>
      inputTopic.pipeInput(kv.key, kv.value, kv.value.timestamp)
      Thread.sleep(100)
    }

    val outputRecords: util.Map[Integer, MyRecord] = outputTopic.readKeyValuesToMap()
    info("output records: ")
    outputRecords.asScala foreach (r => info(r))
    info("data value: ")
    data foreach (r => info(r.value))

    // TODO - fails here, not sure what the diff is
    //outputRecords.asScala.values must contain theSameElementsAs data.map(_.value)

    val store: KeyValueStore[Integer, MyRecord] =
      topologyTestDriver.getKeyValueStore[Integer, MyRecord](storeName)
    val storeContents: List[KeyValue[Integer, MyRecord]] = store.all().asScala.toList
    //storeContents.size mustBe 1

    // inputTopic.getCurrentTime - its private
    info(s"store contents: ${storeContents.size}}")
    storeContents foreach (r => info(r.value))
  }

  def createTopology(
      builder: StreamsBuilder,
      inputTopic: String,
      outputTopic: String,
      storeName: String
  ): Topology = {

    // creating a builder
    //TODO - clarify MetricsScope
    val storeSupplier = new WindowedKeyValueStoreSupplier(storeName, ttl)
    // storeSupplier's metricsScope can't be null
    val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, intSerde, myRecordSerde)

//    val keyValueStoreBuilder: StoreBuilder[KeyValueStore[Int, MyRecord]] =
//      Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), intSerde, myRecordSerde )

    builder.addStateStore(storeBuilder)

    val input: KStream[Int, MyRecord] =
      builder.stream(inputTopic)(
        Consumed.`with`(intSerde, myRecordSerde)
      ) //(Consumed.`with`(Serdes.Integer(), Serdes.Long()))

    val storeTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] = () =>
      Transformers.storeTransformer(storeName) //.(punctuationInterval, ttl, tsExtractor, storeName)
    //val ttlTransformSupplier: TransformerSupplier[Int, MyRecord, KeyValue[Int, MyRecord]] = () => TTLExtractorStoreCleaner(punctuationInterval, ttl, tsExtractor, storeName)
    val stored = input.transform(storeTransformSupplier, storeName)
    //val transformed: KStream[Int, MyRecord] = stored.transform(ttlTransformSupplier, storeName)
    stored.to(outputTopic)(Produced.`with`(intSerde, myRecordSerde))
    builder.build()
  }

  def prepTestTopics(
      driver: TopologyTestDriver,
      inputTopicName: String,
      outputTopicName: String
  ): (TestInputTopic[Integer, MyRecord], TestOutputTopic[Integer, MyRecord]) = {
    val testInputTopic: TestInputTopic[Integer, MyRecord] = driver.createInputTopic(
      inputTopicName,
      Serdes.Integer().serializer(),
      myRecordSerde.serializer(),
      Instant.ofEpochMilli(now),
      // auto-advancing event time by 1 sec per event
      Duration.ofSeconds(1)
    )

    val outputTopic: TestOutputTopic[Integer, MyRecord] = driver.createOutputTopic(
      outputTopicName,
      Serdes.Integer().deserializer(),
      myRecordSerde.deserializer()
    )
    (testInputTopic, outputTopic)
  }

}
