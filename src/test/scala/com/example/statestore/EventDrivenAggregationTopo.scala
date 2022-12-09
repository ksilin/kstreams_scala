package com.example.statestore

import com.example.serde.{ GsonDeserializer, GsonSerializer }
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }
import org.apache.kafka.streams.processor.api.{ Processor, ProcessorContext, Record }
import org.apache.kafka.streams.processor.{ Cancellable, PunctuationType, Punctuator }
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{ Consumed, KStream, Produced }
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import scala.jdk.CollectionConverters._

case class SensorData(name: String, value: Int, ts: Long)
case class MachineData(
    name: String,
    customer: String,
    sensorData: java.util.List[SensorData],
    ts: Long
)
case class MachineTrigger(name: String, before: String, after: String, ts: Long)

case class SensorDataAggregation(
    name: String,
    triggerStart: MachineTrigger,
    triggerStop: MachineTrigger,
    sensorSum: Int,
    ts: Long
)
case class AggregationConfig(factorOldValue: Double = 1.0, factorNewValue: Double = 1.0)
case class TriggerPunctuatorConfig(inactivityCheckPeriod: Long, warnTimeoutMs: Long, warnOnInactivity: Boolean = true)

object EventDrivenAggregationTopo extends StrictLogging {

  val machineDataSerializer: Serializer[MachineData] = new GsonSerializer[MachineData]
  val machineDataDeserializer: Deserializer[MachineData] =
    new GsonDeserializer[MachineData](classOf[MachineData])
  val machineDataSerde: Serde[MachineData] =
    Serdes.serdeFrom(machineDataSerializer, machineDataDeserializer)

  val triggerSerializer: Serializer[MachineTrigger] = new GsonSerializer[MachineTrigger]
  val triggerDeserializer: Deserializer[MachineTrigger] =
    new GsonDeserializer[MachineTrigger](classOf[MachineTrigger])
  val triggerSerde: Serde[MachineTrigger] = Serdes.serdeFrom(triggerSerializer, triggerDeserializer)

  val aggregationSerializer: Serializer[SensorDataAggregation] =
    new GsonSerializer[SensorDataAggregation]
  val aggregationDeserializer: Deserializer[SensorDataAggregation] =
    new GsonDeserializer[SensorDataAggregation](classOf[SensorDataAggregation])
  val aggregationSerde: Serde[SensorDataAggregation] =
    Serdes.serdeFrom(aggregationSerializer, aggregationDeserializer)

  // cannot serialize Option with Gson -> replacing by sentinel value
  val dummyTrigger: MachineTrigger = MachineTrigger("PLACEHOLDER", "PLACEHOLDER", "PLACEHOLDER", 0L)

  def createTopologyPAPI(
      inputTopic: String,
      triggerTopic: String,
      outputTopic: String,
      storeName: String,
      sensorName: String,
      inactivityCheckPeriod: Long
  ): Topology = {

    val aggregationStoreBuilder: StoreBuilder[KeyValueStore[String, SensorDataAggregation]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        stringSerde,
        aggregationSerde
      )

    val dataInputSourceName    = "dataInput"
    val triggerInputSourceName = "triggerInput"
    val dataProcessorName      = "dataProcessor"
    val triggerProcessorName   = "triggerProcessor"

    val topo = new Topology()
    topo
      .addSource(
        dataInputSourceName,
        stringSerde.deserializer(),
        machineDataDeserializer,
        inputTopic
      )
      .addSource(
        triggerInputSourceName,
        stringSerde.deserializer(),
        triggerDeserializer,
        triggerTopic
      )
      .addProcessor(
        dataProcessorName,
        () => makeMachineDataProcessor(storeName, sensorName),
        dataInputSourceName
      )
      .addProcessor(
        triggerProcessorName,
        () => makeTriggerProcessor(storeName, inactivityCheckPeriod),
        triggerInputSourceName
      )
      .addStateStore(aggregationStoreBuilder, dataProcessorName, triggerProcessorName)
      .addSink(
        outputTopic,
        outputTopic,
        stringSerde.serializer(),
        aggregationSerializer,
        triggerProcessorName
      )

    topo
  }

  def createTopologyDSL(
      builder: StreamsBuilder,
      inputTopic: String,
      triggerTopic: String,
      outputTopic: String,
      storeName: String,
      sensorName: String,
      inactivityCheckPeriod: Long
  ): Topology = {

    val aggregationStoreBuilder: StoreBuilder[KeyValueStore[String, SensorDataAggregation]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        stringSerde,
        aggregationSerde
      )
    builder.addStateStore(aggregationStoreBuilder)

    val dataStream: KStream[String, MachineData] =
      builder.stream(inputTopic)(Consumed.`with`(stringSerde, machineDataSerde))
    val triggerStream: KStream[String, MachineTrigger] =
      builder.stream(triggerTopic)(Consumed.`with`(stringSerde, triggerSerde))

    dataStream.process[String, SensorDataAggregation](
      () => makeMachineDataProcessor(storeName, sensorName),
      storeName
    )
    val aggregationStream =
      triggerStream.process[String, SensorDataAggregation](
        () => makeTriggerProcessor(storeName, inactivityCheckPeriod),
        storeName
      )

    aggregationStream.to(outputTopic)(Produced.`with`(stringSerde, aggregationSerde))

    builder.build()
  }

  def makeMachineDataProcessor(
      storeName: String,
      sensorName: String,
  ): Processor[String, MachineData, String, SensorDataAggregation] =
    new Processor[String, MachineData, String, SensorDataAggregation] {

      var ctx: ProcessorContext[String, SensorDataAggregation] = _
      var store: KeyValueStore[String, SensorDataAggregation]  = _

      override def init(context: ProcessorContext[String, SensorDataAggregation]): Unit = {
        logger.info("initializing machine data processor - aggregator")
        ctx = context
        store = ctx.getStateStore(storeName)
      }

      override def process(record: Record[String, MachineData]): Unit = {
        val data      = record.value()
        val machineId = record.key()
        logger.info(s"processing data: $machineId $data")
        Option(store.get(machineId)) map (c => updateAggregation(c, data)) getOrElse logger.warn(
          s"no current aggregation found for $machineId and sensor $sensorName at ${record
            .timestamp()}. Ignoring event."
        )
      }

      // TODO - add generate warning event to warning topic on aggregation passing threshold

      def updateAggregation(currentAgg: SensorDataAggregation, newData: MachineData): Unit = {

        val maybeSensorValue: Option[Int] =
          newData.sensorData.asScala.find(sensor => sensorName == sensor.name).map(_.value)

        // TODO - fetch configurable multiplication factor from config state store here

        maybeSensorValue map { value: Int =>
          val newAgg = currentAgg.copy(sensorSum = currentAgg.sensorSum + value, ts = newData.ts)

          logger.info(s"updating $value to current: ${currentAgg.sensorSum}")
          logger.info(s"current aggregation for ${currentAgg.name}: $newAgg")
          store.put(currentAgg.name, newAgg)
        } getOrElse logger.warn(
          s"no sensor value ford for sensor $sensorName in ${newData.name} with timestamp ${newData.ts}. Ignoring event"
        )
      }
    }

  def makeTriggerProcessor(
      storeName: String,
      inactivityCheckPeriod: Long
  ): Processor[String, MachineTrigger, String, SensorDataAggregation] =
    new Processor[String, MachineTrigger, String, SensorDataAggregation] {

      var ctx: ProcessorContext[String, SensorDataAggregation] = _
      var store: KeyValueStore[String, SensorDataAggregation]  = _
      var cancellablePunctuator: Cancellable                   = _

      // TODO - replace with value from config state store
      var punctuatorConfig = TriggerPunctuatorConfig(1000, 3000)

      override def init(context: ProcessorContext[String, SensorDataAggregation]): Unit = {
        logger.info("initializing change trigger processor")
        ctx = context
        store = ctx.getStateStore(storeName)

        // TODO - make punctuator configurable through an additional config topic:
        // configure warn timeout or disable warn timeout
        cancellablePunctuator = ctx.schedule(
          Duration.ofMillis(inactivityCheckPeriod),
          PunctuationType.WALL_CLOCK_TIME,
          forwardIfNoTriggerForTooLongPunctuator
        )
      }

      override def process(record: Record[String, MachineTrigger]): Unit = {
        val trigger   = record.value()
        val machineId = record.key()

        logger.info(s"change event for ${trigger.name} detected: ")
        logger.info(s"processing change from ${trigger.before} to ${trigger.after} at ${trigger.ts} ")

        val currentValue: Option[SensorDataAggregation] = Option(store.get(machineId))

        currentValue foreach { currentAgg =>
          val updatedAgg = currentAgg.copy(triggerStop = trigger)
          val forwardedRecord: Record[String, SensorDataAggregation] =
            new Record(machineId, updatedAgg, trigger.ts)
          logger.info(s"forwarding aggregation $updatedAgg")
          ctx.forward(forwardedRecord)
        }
        initStoreFor(machineId, trigger)
      }

      def initStoreFor(machineId: String, trigger: MachineTrigger): Unit = {
        val newAggregation = SensorDataAggregation(machineId, trigger, dummyTrigger, 0, trigger.ts)
        logger.info(s"initializing state store value for $machineId with $newAggregation")
        store.put(machineId, newAggregation)
      }

      val forwardIfNoTriggerForTooLongPunctuator: Punctuator = (ts: Long) => {
        if(punctuatorConfig.warnOnInactivity) {
          val warnForTheseInactiveAggregations = store.all.asScala.map{kv =>
            (ts - kv.value.ts, kv)
          }.filter( _._1 > punctuatorConfig.warnTimeoutMs).toList
          logger.info(s"forwardIfNoTriggerForTooLongPunctuator at $ts found ${warnForTheseInactiveAggregations.size} idle aggregations to warn on")
          warnForTheseInactiveAggregations foreach { case (idleTs, agg) =>
            logger.warn(s"${agg.value.name} inactive for $idleTs ms - dispatching inactivity warning")
            // TODO - forward to warn topic
            // optionally, delete from store
          }
        }
      }
    }

}
