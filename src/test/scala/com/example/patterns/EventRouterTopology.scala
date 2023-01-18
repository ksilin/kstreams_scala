package com.example.patterns

import com.example.serde.{GsonDeserializer, GsonSerializer}
import com.google.gson.Gson
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.{StoreBuilder, Stores}
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.common.serialization.{Serdes => JSerdes}
import org.apache.kafka.streams.kstream.{Produced => ProducedJ}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
import wvlet.log.LogSupport

case object EventRouterTopology extends LogSupport {

  case class FilterMapping(productId: String, targetId: String)

  case class RouteMappings(productId: String, targetIds: Array[String])

  case class ProductWithRoute(productData: String, route: String)

  case class ChClustMvke(
      MATNR: String,
      VKORG: String
  )

  case class ProductFilter(
      CH_CLUST_MVKE: Array[ChClustMvke],
      DOCNUM: String,
      UpdateTimeStamp: String,
      UpdatedBy: String,
      ParentTransactionID: String,
      TransactionID: String
  )

  val productFilterSerializer: Serializer[ProductFilter] = new GsonSerializer[ProductFilter]
  val productFilterDeserializer: Deserializer[ProductFilter] =
    new GsonDeserializer[ProductFilter](classOf[ProductFilter])
  val productFilterSerde: Serde[ProductFilter] =
    JSerdes.serdeFrom(productFilterSerializer, productFilterDeserializer)

  val filterMappingsSerializer: Serializer[RouteMappings] = new GsonSerializer[RouteMappings]
  val filterMappingsDeserializer: Deserializer[RouteMappings] =
    new GsonDeserializer[RouteMappings](classOf[RouteMappings])
  val filterMappingsSerde: Serde[RouteMappings] =
    JSerdes.serdeFrom(filterMappingsSerializer, filterMappingsDeserializer)

  val productWithRouteSerializer: Serializer[ProductWithRoute] =
    new GsonSerializer[ProductWithRoute]
  val productWithRouteDeserializer: Deserializer[ProductWithRoute] =
    new GsonDeserializer[ProductWithRoute](classOf[ProductWithRoute])
  val productWithRouteSerde: Serde[ProductWithRoute] =
    JSerdes.serdeFrom(productWithRouteSerializer, productWithRouteDeserializer)

  val gson: Gson = new Gson()

  def createTopology(
      builder: StreamsBuilder,
      productInputTopic: String,
      filterInputTopicA: String,
      filterInputTopicB: String,
      fallbackTopicName: String,
      storeName: String,
      routingMap: Map[String, String]
  ): Topology = {

    val productInput: KStream[String, String] = builder.stream(productInputTopic)

    val aggregationStoreBuilder: StoreBuilder[KeyValueStore[String, RouteMappings]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        stringSerde,
        filterMappingsSerde
      )
    builder.addStateStore(aggregationStoreBuilder)

    val filterInputA: KStream[String, String] = builder.stream(filterInputTopicA)
    val filterInputB: KStream[String, String] = builder.stream(filterInputTopicB)

    val routingSinkProcessorName = "routingSinkProcessor"

    val aggregatedStreamA: KStream[String, RouteMappings] =
      filterInputA.process[String, RouteMappings](
        () => makeFilterDataProcessor(storeName, "A"),
        Named.as("filterA"),
        storeName
      )
    val aggregatedStreamB: KStream[String, RouteMappings] =
      filterInputB.process[String, RouteMappings](
        () => makeFilterDataProcessor(storeName, "B"),
        Named.as("filterB"),
        storeName
      )

    val mappedProductStream: KStream[String, ProductWithRoute] =
      productInput.process[String, ProductWithRoute](
        () => makeProductDataProcessor(storeName, routingSinkProcessorName),
        Named.as("productProcessor"),
        storeName
      )

    val topicNameExtractor: TopicNameExtractor[String, ProductWithRoute] =
      (_: String, v: ProductWithRoute, _: RecordContext) => {
        routingMap.getOrElse(v.route, fallbackTopicName)
      }

    mappedProductStream.to(topicNameExtractor)(
      ProducedJ
        .as(routingSinkProcessorName)
        .withValueSerde(productWithRouteSerde)
        .withKeySerde(stringSerde)
    )
    builder.build()
  }

  def makeFilterDataProcessor(
      aggregationStoreName: String,
      targetName: String,
  ): Processor[String, String, String, RouteMappings] =
    new Processor[String, String, String, RouteMappings] {

      var ctx: ProcessorContext[String, RouteMappings]                   = _
      var productRouteMappingStore: KeyValueStore[String, RouteMappings] = _

      override def init(context: ProcessorContext[String, RouteMappings]): Unit = {
        logger.info(s"initializing product filter processor for target $targetName")
        ctx = context
        productRouteMappingStore = ctx.getStateStore(aggregationStoreName)
      }

      override def process(record: Record[String, String]): Unit = {
        logger.info(s"processing data filter record: $record for target $targetName")
        val rawData                      = record.value()
        val productFilter: ProductFilter = gson.fromJson(rawData, classOf[ProductFilter])

        val productId = record.key()
        logger.info(s"processing data: $productId $productFilter")

        val mappings = productRouteMappingStore.get(productId)
        logger.info(s"current mappings: $mappings")
        val currentProductRouteMapping = Option(mappings).getOrElse {
          logger.warn(
            s"no current product route mapping found for $productId and target $targetName at ${record
              .timestamp()}. Creating new product route mapping."
          )
          RouteMappings(productId, Array.empty[String])
        }
        logger.info(s"currentProductRouteMapping: $currentProductRouteMapping")
        updateMapping(currentProductRouteMapping, productFilter.CH_CLUST_MVKE.head.VKORG)
      }

      def updateMapping(currentMapping: RouteMappings, newData: String): Unit = {
        val updatedTargetIds = (currentMapping.targetIds.toSet + newData).toArray
        logger.info(s"updating ${currentMapping.targetIds} to current: ${updatedTargetIds}")
        val newMapping = currentMapping.copy(targetIds = updatedTargetIds)
        productRouteMappingStore.put(currentMapping.productId, newMapping)
      }
    }

  def makeProductDataProcessor(
      productRouteMappingStoreName: String,
      sinkProcessor: String
  ): Processor[String, String, String, ProductWithRoute] =
    new Processor[String, String, String, ProductWithRoute] {

      var ctx: ProcessorContext[String, ProductWithRoute]         = _
      var routeMappingStore: KeyValueStore[String, RouteMappings] = _
      val fallbackMapping                                         = "NO MAPPING FOUND"

      override def init(context: ProcessorContext[String, ProductWithRoute]): Unit = {
        logger.info("initializing product data processor")
        ctx = context
        routeMappingStore = ctx.getStateStore(productRouteMappingStoreName)
      }

      override def process(record: Record[String, String]): Unit = {
        val data: String      = record.value()
        val productId: String = record.key()
        logger.info(s"processing data: $productId $data")

        Option(routeMappingStore.get(productId)) map (c =>
          c.targetIds foreach { target =>
            val outRecord = ProductWithRoute(data, target)

            val r: Record[String, ProductWithRoute] =
              new Record[String, ProductWithRoute](productId, outRecord, record.timestamp())
            ctx.forward(r, sinkProcessor)
          }
        ) getOrElse {
          logger.warn(
            s"no current routing information found for $productId at ${record
              .timestamp()}. forwarding to fallback."
          )
          val outRecord = ProductWithRoute(data, fallbackMapping)
          val r: Record[String, ProductWithRoute] =
            new Record[String, ProductWithRoute](productId, outRecord, record.timestamp())
          ctx.forward(r, sinkProcessor)
        }
      }
    }

}
