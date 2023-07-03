package com.example.time

import com.example.SpecBase
import com.example.time.KVStoreProcessors._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{
  KeyValue,
  StreamsBuilder,
  StreamsConfig,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}
import org.apache.kafka.streams.kstream.{ Consumed, KStream, Produced }
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state._

import scala.jdk.CollectionConverters._

import java.time.format.DateTimeFormatter
import java.time.{ Duration, Instant, ZoneId, ZonedDateTime }

class SlidingWindowWithKVStoreSpec extends SpecBase {

  val inputTopicName       = "inputTopic"
  val outputTopicName      = "outputTopic"
  val aggregationStoreName = "aggregationStore"

  private val nowInstant: Instant = Instant.now()
  private val now: Long           = nowInstant.toEpochMilli

  streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.POLL_MS_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 1)
  streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0)

  def createTopology(
      processorSupplier: ProcessorSupplier[String, Tx, String, TxAggregate]
  ): Topology = {

    val consumed: Consumed[String, Tx]          = Consumed.`with`(Serdes.String(), txSerde)
    val produced: Produced[String, TxAggregate] = Produced.`with`(Serdes.String(), txAggSerde)

    val store: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(aggregationStoreName)
    val storeBuilder: StoreBuilder[KeyValueStore[String, TxAggregate]] =
      Stores.keyValueStoreBuilder(store, Serdes.String(), txAggSerde)

    val builder = new StreamsBuilder()
    builder.addStateStore(storeBuilder)

    val inputStream: KStream[String, Tx] =
      builder.stream(inputTopicName, consumed.withName("inputStream"))

    val processedStream: KStream[String, TxAggregate] =
      inputStream.process(processorSupplier, aggregationStoreName)

    processedStream.to(outputTopicName, produced.withName("outputStream"))

    builder.build()
  }

  "must aggregate state with lookback and purge" in {
    val processorSupplier: ProcessorSupplier[String, Tx, String, TxAggregate] =
      () => TxAggregationProcessor(aggregationStoreName, 3000)
    val topo: Topology = createTopology(processorSupplier)
    println(topo.describe())
    val testTopologyDriver = new TopologyTestDriver(topo, streamsConfiguration, nowInstant)

    val inputTopic: TestInputTopic[String, Tx] = testTopologyDriver.createInputTopic(
      inputTopicName,
      Serdes.String().serializer(),
      txSerde.serializer()
    )
    val outputTopic: TestOutputTopic[String, TxAggregate] = testTopologyDriver.createOutputTopic(
      outputTopicName,
      Serdes.String().deserializer(),
      txAggSerde.deserializer()
    )

    val acc1    = "acc1"
    val tx1_1_1 = Tx(acc1, "tx1", "PENDING", 3, now)
    val tx1_1_2 = Tx(acc1, "tx1", "COMPLETE", 3, now + 1000)
    val tx1_2_1 = Tx(acc1, "tx2", "PENDING", 5, now + 2000)
    val tx1_2_2 = Tx(acc1, "tx2", "FAILED", 5, now + 3000)

    val acc2    = "acc2"
    val tx2_1_1 = Tx(acc2, "tx1", "PENDING", 7, now + 4000)
    val tx2_1_2 = Tx(acc2, "tx1", "COMPLETE", 7, now + 5000)
    val tx2_2_1 = Tx(acc2, "tx2", "PENDING", 11, now + 6000)
    val tx2_2_2 = Tx(acc2, "tx2", "FAILED", 11, now + 7000)

    inputTopic.pipeInput(acc1, tx1_1_1)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc1, tx1_1_2)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc1, tx1_2_1)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc1, tx1_2_2)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc2, tx2_1_1)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc2, tx2_1_2)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    inputTopic.pipeInput(acc2, tx2_2_1)
    inputTopic.advanceTime(Duration.ofMillis(1000))

    println("--- output topic: ")
    outputTopic.readKeyValuesToList().asScala foreach { rkv =>
      println(rkv)
    }
    println("---")

    val store: KeyValueStore[String, TxAggregate] =
      testTopologyDriver.getKeyValueStore(aggregationStoreName)
    println("--- final store state: ")
    store.all().forEachRemaining { kv: KeyValue[String, TxAggregate] =>
      val zdt =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(kv.value.updatedAt), ZoneId.systemDefault())
      println(
        s"${kv.key}: total ${kv.value.total} @ ${zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}"
      )

      kv.value.txList.asScala foreach { tx =>
        val zdt =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(tx.createdAt), ZoneId.systemDefault())
        println(s"${tx.txId}: ${tx.status} @ ${zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")
      }
    }
    println("---")
  }

}
