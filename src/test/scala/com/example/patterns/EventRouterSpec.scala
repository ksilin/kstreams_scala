package com.example.patterns

import org.apache.kafka.streams.{KeyValue, StreamsConfig, TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}

import scala.jdk.CollectionConverters._
import com.example.SpecBase
import com.example.patterns.EventRouterTopology.{ChClustMvke, RouteMappings, ProductFilter}
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore

import java.time.{Duration, Instant}
import scala.util.Random

class EventRouterSpec extends SpecBase {

  // if we want to by dynamic about the topics, use builder.stream[K, V](topicPattern: Pattern)
  // to work with multiple topics dynamically
  // BUT - the final mappign will need to be static regardless

  private val nowInstant: Instant = Instant.now()
  private val now: Long           = nowInstant.toEpochMilli

  val filterATopicName  = "filterAInputTopic"
  val filterBTopicName  = "filterBInputTopic"
  val productTopicName  = "productInputTopic"
  val targetATopicName  = "targetAResultTopic"
  val targetBTopicName  = "targetBResultTopic"
  val fallbackTopicName = "failbackResultTopic"
  val storeName         = "productMappingStore"

  val gson: Gson = new Gson()

  // TODO - variant without explicit default topic name - default map value:
  // map.default("X")
  val routingMap =Map(
    "A" -> targetATopicName,
    "B" -> targetBTopicName,
  )

  val productIdA = "productA"
  val productIdB = "productB"
  val productIdC = "productC"
  val productIdD = "productD"

  val productToAFilter: ProductFilter = ProductFilter(DOCNUM = Random.alphanumeric.take(3).mkString,
    UpdateTimeStamp = Random.alphanumeric.take(3).mkString,
    UpdatedBy = Random.alphanumeric.take(3).mkString,
    ParentTransactionID = Random.alphanumeric.take(3).mkString,
    TransactionID = Random.alphanumeric.take(3).mkString,
    CH_CLUST_MVKE = Array(ChClustMvke(MATNR = Random.alphanumeric.take(3).mkString, "A"))
  )

  val productToBFilter: ProductFilter = ProductFilter(DOCNUM = Random.alphanumeric.take(3).mkString,
    UpdateTimeStamp = Random.alphanumeric.take(3).mkString,
    UpdatedBy = Random.alphanumeric.take(3).mkString,
    ParentTransactionID = Random.alphanumeric.take(3).mkString,
    TransactionID = Random.alphanumeric.take(3).mkString,
    CH_CLUST_MVKE = Array(ChClustMvke(MATNR = Random.alphanumeric.take(3).mkString, "B"))
  )

  val filterKVStringsA: List[KeyValue[String, String]] = List(
    new KeyValue(productIdA, gson.toJson(productToAFilter)),
    new KeyValue(productIdC, gson.toJson(productToAFilter)),
  )

  val filterKVStringsB: List[KeyValue[String, String]] = List(
    new KeyValue(productIdB, gson.toJson(productToBFilter)),
    new KeyValue(productIdC, gson.toJson(productToBFilter))
  )


  "must route products" in {

    val topo = EventRouterTopology.createTopology(
      builder,
      productTopicName,
      filterATopicName,
      filterBTopicName,
      fallbackTopicName,
      storeName,
      routingMap
    )

    info(topo.describe())

    streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
    streamsConfiguration.put(StreamsConfig.POLL_MS_CONFIG, 100)

    val driver = new TopologyTestDriver(topo, streamsConfiguration)

    val stringSerde = Serdes.stringSerde
    val stringSerializer = stringSerde.serializer()
    val stringDeserializer = stringSerde.deserializer()

    val productTopic: TestInputTopic[String, String] = driver.createInputTopic(
      productTopicName,
      stringSerializer,
      stringSerializer
    )
    val filterATopic: TestInputTopic[String, String] = driver.createInputTopic(
      filterATopicName,
      stringSerializer,
      stringSerializer
    )
    val filterBTopic: TestInputTopic[String, String] = driver.createInputTopic(
      filterBTopicName,
      stringSerializer,
      stringSerializer
    )

    val targetATopic: TestOutputTopic[String, String] = driver.createOutputTopic(
      targetATopicName,
      stringDeserializer,
      stringDeserializer
    )
    val targetBTopic: TestOutputTopic[String, String] = driver.createOutputTopic(
      targetBTopicName,
      stringDeserializer,
      stringDeserializer
    )
    val failbackTopic: TestOutputTopic[String, String] = driver.createOutputTopic(
      fallbackTopicName,
      stringDeserializer,
      stringDeserializer
    )

    filterATopic.pipeKeyValueList(filterKVStringsA.asJava, nowInstant, Duration.ofMillis(100))
    filterBTopic.pipeKeyValueList(filterKVStringsB.asJava, nowInstant, Duration.ofMillis(100))

    // one product for A
    // one product for B
    // one product for both A and B
    // one product for none
    val productKVs = List(
      new KeyValue(productIdA, productIdA),
      new KeyValue(productIdB, productIdB),
      new KeyValue(productIdC, productIdC),
      new KeyValue(productIdD, productIdD)
    )

    productTopic.pipeKeyValueList(productKVs.asJava)

    info("result in A:")
    val targetARecords = targetATopic.readKeyValuesToMap().asScala
    targetARecords foreach { case (k, v) =>
      info(s"$k, $v")
    }
    targetARecords.keys must contain allElementsOf List(productIdA, productIdC)

    info("result in B:")
    val targetBRecords = targetBTopic.readKeyValuesToMap().asScala
    targetBRecords foreach { case (k, v) =>
      info(s"$k, $v")
    }
    targetBRecords.keys must contain allElementsOf List(productIdB, productIdC)

    info("result in fallback:")
    val fallbackRecords = failbackTopic.readKeyValuesToMap().asScala
    fallbackRecords foreach { case (k, v) =>
      info(s"$k, $v")
    }
    fallbackRecords.keys must contain allElementsOf List(productIdD)

    val stateStore: KeyValueStore[String, RouteMappings] = driver.getKeyValueStore[String, RouteMappings](storeName)
    info("store contents at test end (should contain complete routing info):")
    val storeContents: List[KeyValue[String, RouteMappings]] = stateStore.all().asScala.toList
    storeContents foreach (r => info(r))

  }

}
