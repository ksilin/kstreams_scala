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
      sensorName: String
  ): Topology = {

    val keyValueStoreBuilder: StoreBuilder[KeyValueStore[String, String]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        stringSerde,
        stringSerde
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
        () => dataProcessor(storeName, sensorName),
        dataInputSourceName
      )
      .addProcessor(
        triggerProcessorName,
        () => triggerProcessor(storeName),
        triggerInputSourceName
      )
      .addStateStore(keyValueStoreBuilder, dataProcessorName, triggerProcessorName)
      .addSink(
        outputTopic,
        outputTopic,
        stringSerde.serializer(),
        stringSerde.serializer(),
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
      sensorName: String
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
      () => dataProcessor(storeName, sensorName),
      storeName
    )
    val aggregationStream =
      triggerStream.process[String, SensorDataAggregation](
        () => triggerProcessor(storeName),
        storeName
      )

    aggregationStream.to(outputTopic)(Produced.`with`(stringSerde, aggregationSerde))

    builder.build()
  }

  def dataProcessor(
      storeName: String,
      sensorName: String
  ): Processor[String, MachineData, String, SensorDataAggregation] =
    new Processor[String, MachineData, String, SensorDataAggregation] {

      var ctx: ProcessorContext[String, SensorDataAggregation] = _
      var store: KeyValueStore[String, SensorDataAggregation]  = _
      var cancellablePunctuator: Cancellable                   = _

      override def init(context: ProcessorContext[String, SensorDataAggregation]): Unit = {
        logger.info("initializing machine data processor - aggregator")
        ctx = context
        store = ctx.getStateStore(storeName)

        cancellablePunctuator = ctx.schedule(
          Duration.ofMillis(1000),
          PunctuationType.WALL_CLOCK_TIME,
          deleteIfPeriodTooLongPunctuator
        )
      }

      override def process(record: Record[String, MachineData]): Unit = {
        val data      = record.value()
        val machineId = record.key()
        logger.info(s"processing data: $machineId $data")
        val maybeSensorValue: Option[Int] =
          data.sensorData.asScala.find(sensor => sensorName == sensor.name).map(_.value)
        maybeSensorValue map { value: Int =>
          Option(store.get(machineId)) map (c => updateAggregation(c, value)) getOrElse logger.warn(
            s"no current aggregation found for $machineId and sensor $sensorName at ${record
              .timestamp()}. Ignoring event. Current value $value"
          )
        } getOrElse logger.warn(
          s"no sensor value ford for sensor $sensorName in $machineId at ${record.timestamp()}. Ignoring event"
        )
      }

      val deleteIfPeriodTooLongPunctuator: Punctuator = (ts: Long) => {
        logger.info(s"data processor punctuator at $ts")
      }

      def updateAggregation(currentAgg: SensorDataAggregation, value: Int): Unit = {
        val newAgg = currentAgg.copy(sensorSum = currentAgg.sensorSum + value)
        logger.info(s"adding $value to current: ${currentAgg.sensorSum}")
        logger.info(s"current aggregation for ${currentAgg.name}: $newAgg")
        store.put(currentAgg.name, newAgg)
      }
    }

  def triggerProcessor(
      storeName: String
  ): Processor[String, MachineTrigger, String, SensorDataAggregation] =
    new Processor[String, MachineTrigger, String, SensorDataAggregation] {

      var ctx: ProcessorContext[String, SensorDataAggregation] = _
      var store: KeyValueStore[String, SensorDataAggregation]  = _
      var cancellablePunctuator: Cancellable                   = _

      override def init(context: ProcessorContext[String, SensorDataAggregation]): Unit = {
        logger.info("initializing change trigger processor")
        ctx = context
        store = ctx.getStateStore(storeName)

        cancellablePunctuator = ctx.schedule(
          Duration.ofMillis(1000),
          PunctuationType.WALL_CLOCK_TIME,
          deleteIfPeriodTooLongPunctuator
        )
      }

      override def process(record: Record[String, MachineTrigger]): Unit = {
        val trigger   = record.value()
        val machineId = record.key()

        logger.info(s"change for ${trigger.name} detected: ")
        logger.info(s"from ${trigger.before} to ${trigger.after} detected at ${trigger.ts} ")

        val currentValue: Option[SensorDataAggregation] = Option(store.get(machineId))

        currentValue foreach { currentAgg =>
          logger.info(s"change triggers output forwarding: $currentValue")
          val forwardedRecord: Record[String, SensorDataAggregation] =
            new Record(machineId, currentAgg, trigger.ts)
          logger.info(s"forwarding aggregation $currentAgg")
          ctx.forward(forwardedRecord)
        }
        initStoreFor(machineId, trigger)
      }

      def initStoreFor(machineId: String, trigger: MachineTrigger): Unit = {
        val newAggregation = SensorDataAggregation(machineId, trigger, dummyTrigger, 0, trigger.ts)
        logger.info(s"(re)initializing state store value for $machineId with $newAggregation")
        store.put(machineId, newAggregation)
      }

      val deleteIfPeriodTooLongPunctuator: Punctuator = (ts: Long) => {
        logger.info(s"change trigger processor punctuator at $ts")
      }
    }

}
