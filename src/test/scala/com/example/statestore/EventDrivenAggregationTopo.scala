package com.example.statestore

import com.example.serde.{GsonDeserializer, GsonSerializer}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType, Punctuator}
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.Topology

import java.time.Duration
import scala.jdk.CollectionConverters._

case class SensorData(name: String, value: Int, ts: Long)
case class MachineData(name: String, customer: String, sensorData: java.util.List[SensorData], ts: Long)
case class MachineTrigger(name: String, before: String, after: String, ts: Long)

case class SensorDataAggregation(name: String, triggerStart: MachineTrigger, triggerStop: MachineTrigger, sensorSum: Int, ts: Long)

object EventDrivenAggregationTopo extends StrictLogging {

  val machineDataSerializer: Serializer[MachineData] = new GsonSerializer[MachineData]
  val machineDataDeserializer: Deserializer[MachineData] = new GsonDeserializer[MachineData](classOf[MachineData])
  val machineDataSerde: Serde[MachineData] = Serdes.serdeFrom(machineDataSerializer, machineDataDeserializer)

  val triggerSerializer: Serializer[MachineTrigger] = new GsonSerializer[MachineTrigger]
  val triggerDeserializer: Deserializer[MachineTrigger] = new GsonDeserializer[MachineTrigger](classOf[MachineTrigger])
  val triggerSerde: Serde[MachineTrigger] = Serdes.serdeFrom(triggerSerializer, triggerDeserializer)

  val aggregationSerializer: Serializer[SensorDataAggregation] = new GsonSerializer[SensorDataAggregation]
  val aggregationDeserializer: Deserializer[SensorDataAggregation] = new GsonDeserializer[SensorDataAggregation](classOf[SensorDataAggregation])
  val aggregationSerde: Serde[SensorDataAggregation] = Serdes.serdeFrom(aggregationSerializer, aggregationDeserializer)

  def createTopology(
      inputTopic: String,
      triggerTopic: String,
      outputTopic: String,
      storeName: String
  ): Topology = {

    val keyValueStoreBuilder: StoreBuilder[KeyValueStore[String, String]] =
      Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), stringSerde, stringSerde)

    val dataInputSourceName    = "dataInput"
    val triggerInputSourceName = "triggerInput"
    val processorName          = "myProcessor"

    val topo = new Topology()
    topo
      .addSource(dataInputSourceName, stringSerde.deserializer(), machineDataDeserializer, inputTopic)
      .addSource(
        triggerInputSourceName,
        stringSerde.deserializer(),
        triggerDeserializer,
        triggerTopic
      )
      .addProcessor(
    processorName,
    () => processor(storeName, "8"),
    dataInputSourceName,
    triggerInputSourceName
      )
      .addStateStore(keyValueStoreBuilder, processorName)
      .addSink(
        outputTopic,
        outputTopic,
        stringSerde.serializer(),
        stringSerde.serializer(),
        processorName
      )

    topo
  }

  def processor(storeName: String, sensorName: String): Processor[String, String, String, String] =
    new Processor[String, String, String, String] {

      var ctx: ProcessorContext[String, String]   = _
      var store: KeyValueStore[String, String]    = _
      var cancellablePunctuator: Cancellable = _

      override def init(context: ProcessorContext[String, String]): Unit = {
        logger.info("initializing processor")
        ctx = context
        store = ctx.getStateStore(storeName)

        cancellablePunctuator = ctx.schedule(
          Duration.ofMillis(1000),
          PunctuationType.WALL_CLOCK_TIME,
          deleteIfPeriodTooLongPunctuator
        )
      }

      override def process(record: Record[String, String]): Unit = {
        // TODO - putIfAbsent with conditional may be more practical here
        logger.info(s"processing ${record.key()} : ${record.value()}")

        logger.info(s"class: ${record.value().getClass} : ${MachineTrigger.getClass}")

        if(record.value().getClass.toString.contains("MachineTrigger")){// == MachineTrigger.getClass){
          val trigger = record.value().asInstanceOf[MachineTrigger]
          logger.info(s"change for ${trigger.name} detected: ")
          logger.info(s"from ${trigger.before} to ${trigger.after} detected at ${trigger.ts} ")

          val currentValue: String = Option(store.get(record.key())).getOrElse("")
          if(currentValue.nonEmpty){
            logger.info(s"change triggers output forwarding: ${currentValue}")
            val forwardedRecord: Record[String, String] = new Record(record.key(), currentValue, trigger.ts)
            ctx.forward(forwardedRecord)
          } else {
            logger.info(s"initializing state store value for ${record.key()} with 0")
            store.put(record.key(), "0")
          }
        } else {
          val data = record.value().asInstanceOf[MachineData]
          logger.info(s"found data: ${data}")
          val maybeSensorValue = data.sensorData.asScala.find(sensor => sensorName == sensor.name).map(_.value)
          maybeSensorValue foreach { value =>
            val currentValue: String = Option(store.get(record.key())).getOrElse("")
            if (currentValue.nonEmpty) {
              logger.info(s"adding ${value} to current: ${currentValue}")
              store.put(record.key(), currentValue + value.toString)
            } else {
              logger.info(s"initializing state store value for ${record.key()} with ${value}")
              store.put(record.key(), value.toString)
            }
          }
        }
      }

      override def close(): Unit = {}

      val deleteIfPeriodTooLongPunctuator: Punctuator = (ts: Long) => {
        logger.info(s"checking state store for string length > 5 at $ts")
        //store.all().asScala.filter(kv => kv.value.length > 5).foreach { largeKv =>
          //logger.info(s"setting to empty string and forwarding $largeKv")
          //store.put(largeKv.key, "")
          //val newRecord =
           // new Record[String, String](largeKv.key, largeKv.value, ctx.currentSystemTimeMs())
          //ctx.forward(newRecord)
        //}
      }
    }
}
