package com.example.statestore

import com.example.SpecBase
import com.example.statestore.EventDrivenAggregationTopo.{
  aggregationDeserializer,
  idleAggregationWarningDeserializer
}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.apache.kafka.streams.state.KeyValueStore

import java.time.{ Duration, Instant }
import java.util
import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

class EventDrivenAggregationSpec extends SpecBase {

  val machineDataTopicName       = "machineDataInputTopic"
  val triggerInputTopicName      = "triggerInputTopic"
  val aggregationResultTopicName = "aggregationResultTopic"
  val idleWarningTopicName       = "idleWarningTopic"
  val storeName                  = "aggregationResultStore"

  private val sensorIds: List[String]  = (1 to 10).map(_.toString).toList
  private val machineIds: List[String] = ('A' to 'B').map(_.toString).toList

  private val nowInstant: Instant = Instant.now()
  private val now: Long           = nowInstant.toEpochMilli
  private val later: Long         = now + 10000

  private val timestamps: List[Long] = now.to(later, 1000L).toList

  private val sensorData: List[KeyValue[String, SensorData]] =
    sensorIds map { i: String =>
      val v = Random.nextInt(10)
      new KeyValue(i, SensorData(i, v, now + v))
    }

  private val machineData: List[KeyValue[String, MachineData]] = machineIds flatMap { machineId =>
    timestamps map { newTs =>
      val machineIdData =
        MachineData(machineId, machineId + "_customer", sensorData.map(_.value).asJava, newTs)
      new KeyValue(machineId, machineIdData)
    }
  }

  private val triggerDataStart: List[KeyValue[String, MachineTrigger]] = machineIds map {
    machineId =>
      val trigger = MachineTrigger(machineId, machineId + "_before_1", machineId + "_after_1", now)
      new KeyValue(machineId, trigger)
  }

  private val triggerDataEnd: List[KeyValue[String, MachineTrigger]] = machineIds map { machineId =>
    val trigger = MachineTrigger(machineId, machineId + "_after_1", machineId + "_after_2", later)
    new KeyValue(machineId, trigger)
  }

  "must aggregate and react to triggers" in {

    val topology: Topology =
      EventDrivenAggregationTopo.createTopologyPAPI(
        //EventDrivenAggregationTopo.createTopologyDSL(
        //  builder,
        machineDataTopicName,
        triggerInputTopicName,
        aggregationResultTopicName,
        idleWarningTopicName,
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
        machineDataTopicName,
        Serdes.String().serializer(),
        EventDrivenAggregationTopo.machineDataSerializer
      )

    val inputTopicTrigger: TestInputTopic[String, MachineTrigger] =
      topologyTestDriver.createInputTopic(
        triggerInputTopicName,
        Serdes.String().serializer(),
        EventDrivenAggregationTopo.triggerSerializer
      )

    val aggregationResultOutputTopic: TestOutputTopic[String, SensorDataAggregation] =
      topologyTestDriver.createOutputTopic(
        aggregationResultTopicName,
        Serdes.String().deserializer(),
        aggregationDeserializer
      )

    val idleWarningOutputTopic: TestOutputTopic[String, IdleAggregationWarning] =
      topologyTestDriver.createOutputTopic(
        idleWarningTopicName,
        Serdes.String().deserializer(),
        idleAggregationWarningDeserializer
      )

    // first batch of data to test for dropping the records if no aggregation available
    inputTopicMachineData.pipeKeyValueList(machineData.take(2).asJava)
    // trigger creating first aggregation
    inputTopicTrigger.pipeKeyValueList(triggerDataStart.asJava)
    inputTopicMachineData.pipeKeyValueList(machineData.take(5).asJava)
    // TODO - change data processing config here
    inputTopicMachineData.pipeKeyValueList(machineData.drop(5).asJava)
    // trigger finishing first aggregation
    inputTopicTrigger.pipeKeyValueList(triggerDataEnd.asJava)

    topologyTestDriver.advanceWallClockTime(Duration.ofMillis(later - now))
    (1 to 5).foreach { _ =>
      Thread.sleep(100)
      // trigger idle aggregation
      topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1))
    }

    info("resulting records:")
    val outputRecords: util.Map[String, SensorDataAggregation] =
      aggregationResultOutputTopic.readKeyValuesToMap()
    outputRecords.asScala.values foreach (r => info(r))

    info("warning records:")
    val warningRecords: util.Map[String, IdleAggregationWarning] =
      idleWarningOutputTopic.readKeyValuesToMap()
    warningRecords.asScala.values foreach (r => info(r))

    // explicit store init
    val store: KeyValueStore[String, SensorDataAggregation] =
      topologyTestDriver.getKeyValueStore[String, SensorDataAggregation](storeName)
    info("store contents at test end (should contain aggregations with sentinel afters):")
    val storeContents: List[KeyValue[String, SensorDataAggregation]] = store.all().asScala.toList
    storeContents foreach (r => info(r))

    topologyTestDriver.metrics().asScala.foreach { case (k, v) =>
      info(s"metric ${k.name()}: ${v.metricValue()}")
    }
  }

}
