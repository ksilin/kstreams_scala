package com.example.statestore

import com.example.SpecBase
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.apache.kafka.streams.state.KeyValueStore

import java.time.{ Duration, Instant }
import java.{ lang, util }
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class EventDrivenAggregationSpec extends SpecBase {

  val inputTopicName        = "inputTopic"
  val triggerInputTopicName = "triggerTopic"
  val outputTopicName       = "outputTopic"
  val storeName             = s"${suiteName}_store"

  private val sensorIds: List[String]  = (1 to 10).map(_.toString).toList
  private val machineIds: List[String] = ('A' to 'B').map(_.toString).toList

  private val nowInstant: Instant = Instant.now()
  val now: Long = nowInstant.toEpochMilli
  val later = now + 10000

  val timestamps = now.to(later, 1000L).toList

  val sensorData: List[KeyValue[String, SensorData]] =
    sensorIds map { i: String =>
      val v = Random.nextInt(10)
      new KeyValue(i, SensorData(i, v, now + v))
    }

  val machineData: List[KeyValue[String, MachineData]] = machineIds flatMap { machineId =>
    timestamps map { newTs =>
      val machineIdData =
        MachineData(machineId, machineId + "_customer", sensorData.map(_.value).asJava, newTs)
      new KeyValue(machineId, machineIdData)
    }
  }

  val triggerDataStart: List[KeyValue[String, MachineTrigger]] = machineIds map { machineId =>
    val trigger = MachineTrigger(machineId, machineId + "_before_1", machineId + "_after_1", now)
    new KeyValue(machineId, trigger)
  }

  val triggerDataEnd: List[KeyValue[String, MachineTrigger]] = machineIds map { machineId =>
    val trigger = MachineTrigger(machineId, machineId + "_after_1", machineId + "_after_2", later)
    new KeyValue(machineId, trigger)
  }

  "must aggregate and react to triggers" in {

    val topology: Topology =
      //EventDrivenAggregationTopo.createTopologyPAPI(inputTopicName, triggerInputTopicName, outputTopicName, storeName, "1")
      EventDrivenAggregationTopo.createTopologyDSL(
        builder,
        inputTopicName,
        triggerInputTopicName,
        outputTopicName,
        storeName,
        "1",
        1000L
      )

    info(topology.describe())

    streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
    streamsConfiguration.put(StreamsConfig.POLL_MS_CONFIG, 100)

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration, nowInstant)

    val inputTopicMachineData: TestInputTopic[String, MachineData] =
      topologyTestDriver.createInputTopic(
        inputTopicName,
        Serdes.String().serializer(),
        EventDrivenAggregationTopo.machineDataSerializer
      )

    val triggerTopic: TestInputTopic[String, MachineTrigger] = topologyTestDriver.createInputTopic(
      triggerInputTopicName,
      Serdes.String().serializer(),
      EventDrivenAggregationTopo.triggerSerializer
    )

    val outputTopic: TestOutputTopic[String, lang.String] = topologyTestDriver.createOutputTopic(
      outputTopicName,
      Serdes.String().deserializer(),
      Serdes.String().deserializer()
    )
    // first batch of data to test for dropping the records if no aggregation available
    inputTopicMachineData.pipeKeyValueList(machineData.take(2).asJava)
    // trigger creating first aggregation
    triggerTopic.pipeKeyValueList(triggerDataStart.asJava)
    inputTopicMachineData.pipeKeyValueList(machineData.take(5).asJava)
    // TODO - change data processing config here
    inputTopicMachineData.pipeKeyValueList(machineData.drop(5).asJava)
    // trigger finishing first aggregation
    triggerTopic.pipeKeyValueList(triggerDataEnd.asJava)

    topologyTestDriver.advanceWallClockTime(Duration.ofMillis(later - now))
    (1 to 5).foreach { _ =>
      Thread.sleep(100)
      // trigger idle aggregation
      topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1))
    }

    logger.info("resulting records:")
    val outputRecords: util.Map[String, lang.String] = outputTopic.readKeyValuesToMap()
    outputRecords.asScala.values foreach (r => logger.info(r))

    // explicit store init
    val store: KeyValueStore[String, lang.String] =
      topologyTestDriver.getKeyValueStore[String, lang.String](storeName)
    logger.info("store contents at test end (should contain aggregations with sentinel afters):")
    val storeContents: List[KeyValue[String, lang.String]] = store.all().asScala.toList
    storeContents foreach (r => logger.info(r))

    topologyTestDriver.metrics().asScala.foreach { case (k, v) =>
      logger.info(s"metric ${k.name()}: ${v.metricValue()}")
    }
  }

}
