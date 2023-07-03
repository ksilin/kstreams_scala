package com.example.time

import com.example.SpecBase
import com.example.time.WindowStoreProcessors._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.{ Consumed, KStream, Produced, Windowed }
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{
  KeyValueIterator,
  StoreBuilder,
  Stores,
  WindowBytesStoreSupplier,
  WindowStore
}
import org.apache.kafka.streams.{
  StreamsBuilder,
  StreamsConfig,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}

import scala.jdk.CollectionConverters._
import java.time.{ Duration, Instant }

class WindowStoreSpec extends SpecBase {

  val inputTopicName  = "inputTopic"
  val outputTopicName = "outputTopic"
  val storeName       = "windowStore"

  private val nowInstant: Instant = Instant.now()
  private val now: Long           = nowInstant.toEpochMilli

  streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.POLL_MS_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0)

  def createTopology(
      processorSupplier: ProcessorSupplier[String, MyRecord, String, MyRecord]
  ): Topology = {

    val consumed: Consumed[String, MyRecord] = Consumed.`with`(Serdes.stringSerde, recordSerde)
    val produced: Produced[String, MyRecord] = Produced.`with`(Serdes.stringSerde, recordSerde)

    val windowStoreSize      = Duration.ofSeconds(3)
    val windowStoreRetention = Duration.ofSeconds(3)
    // TODO - how to define a grace period
    val store: WindowBytesStoreSupplier =
      Stores.persistentWindowStore(storeName, windowStoreRetention, windowStoreSize, false)
    val storeBuilder: StoreBuilder[WindowStore[String, MyRecord]] =
      Stores.windowStoreBuilder(store, Serdes.stringSerde, recordSerde)

    val builder = new StreamsBuilder()
    builder.addStateStore(storeBuilder)

    val inputStream: KStream[String, MyRecord] =
      builder.stream(inputTopicName, consumed.withName("inputStream"))

    val processedStream: KStream[String, MyRecord] =
      inputStream.process(processorSupplier, storeName)

    processedStream.to(outputTopicName, produced.withName("outputStream"))

    builder.build()
  }

  "must aggregate with lookback" in {
    val processorSupplier: ProcessorSupplier[String, MyRecord, String, MyRecord] =
      () => SimpleLookbackRangeWindowStoreProcessor(storeName, 5000)
    val topo: Topology = createTopology(processorSupplier)
    println(topo.describe())
    val testTopologyDriver = new TopologyTestDriver(topo, streamsConfiguration, nowInstant)

    val inputTopic: TestInputTopic[String, MyRecord] = testTopologyDriver.createInputTopic(
      inputTopicName,
      Serdes.stringSerde.serializer(),
      recordSerde.serializer(),
      nowInstant,
      Duration.ofMillis(1000)
    )
    val outputTopic: TestOutputTopic[String, MyRecord] = testTopologyDriver.createOutputTopic(
      outputTopicName,
      Serdes.stringSerde.deserializer(),
      recordSerde.deserializer()
    )

    // evicted
    inputTopic.pipeInput("1", MyRecord(name = "1_1", description = "A", now, now)) // -9000
    inputTopic.pipeInput(
      "2",
      MyRecord(name = "2_1", description = "B", now + 100, now + 100)
    ) // -8000
    inputTopic.pipeInput(
      "1",
      MyRecord(name = "1_2", description = "C", now + 200, now + 200)
    ) // -7000

    // in store
    inputTopic.pipeInput(
      "2",
      MyRecord(name = "2_2", description = "D", now + 300, now + 300)
    ) // -6000
    inputTopic.pipeInput(
      "3",
      MyRecord(name = "3_1", description = "E", now + 400, now + 400)
    ) // -5000

    inputTopic.pipeInput(
      "3",
      MyRecord(name = "3_2", description = "F", now + 500, now + 500)
    ) // -4000
    inputTopic.pipeInput(
      "1",
      MyRecord(name = "1_3", description = "G", now + 600, now + 600)
    ) // -3000
    inputTopic.pipeInput(
      "2",
      MyRecord(name = "2_3", description = "H", now + 700, now + 700)
    ) // -2000
    inputTopic.pipeInput(
      "1",
      MyRecord(name = "1_4", description = "I", now + 600, now + 600)
    )                                                                                          // -1000
    inputTopic.pipeInput("2", MyRecord(name = "2_4", description = "J", now + 700, now + 700)) // 0

    println("--- output topic")
    outputTopic.readKeyValuesToList().asScala foreach { rkv =>
      println(rkv)
    }
    println("---")

    // TODO - no real way to get stream time from the testTopologyDriver
    val store: WindowStore[String, MyRecord] = testTopologyDriver.getWindowStore(storeName)
    val backwardAll: KeyValueIterator[Windowed[String], MyRecord] =
      store.backwardFetchAll(now, now + 100000)
    println("--- store contents backwards: ")
    backwardAll.forEachRemaining(kv => println(kv))
    println("---")

  }

}
