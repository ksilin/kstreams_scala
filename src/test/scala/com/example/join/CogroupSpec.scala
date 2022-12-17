package com.example.join

import com.example.SpecBase
import com.example.util.KafkaSpecHelper
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{ CPTestContainerFactory, ConfluentServerContainer }
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.{ OutputFrame, WaitingConsumer }

import java.util.concurrent.TimeUnit
import _root_.scala.util.Random
import _root_.scala.jdk.CollectionConverters._

class CogroupSpec extends SpecBase {

  // a customer is represented by a name and three collections of line items from different streams
  // Cart, Wishlist & Purchases

  // without cogroup, we would pre-aggregate the line items and then outer join one after the other

  // with cogroup, we can aggregate directly in the target type
  // and there is just one state store

  case class LineItem(id: String, name: String)
  case class Customer(
      name: String,
      cart: List[LineItem],
      wishlist: List[LineItem],
      purchases: List[LineItem]
  )

  val logConsumer: WaitingConsumer = new WaitingConsumer()

  private val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  private val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  broker.followOutput(logConsumer, OutputFrame.OutputType.STDOUT)

  logConsumer.waitUntil(
    { frame: OutputFrame =>
      frame.getUtf8String.contains("started")
    },
    60,
    TimeUnit.SECONDS
  )

  private val bootstrap: String = broker.getBootstrapServers

  private val purchaseInputTopicName = "purchaseTopic"
  private val wishlistInputTopicName = "wishlistInputTopic"
  private val cartInputTopicName     = "cartInputTopic"
  private val topics                 = List(purchaseInputTopicName, wishlistInputTopicName, cartInputTopicName)

  private val customerOutputTopicName = "customerOutputTopic"

  private val itemSerializer: Serializer[LineItem] = implicitly
  private val itemSerde: Serde[LineItem]           = implicitly

  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName)
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"$suiteName-group")
  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  private val customerIds: List[String] = List("Shaun Shopper", "Berta Buyer", "Peter Purchaser")

  private val lineItems: Seq[LineItem] = (1 to 5) map { _ =>
    LineItem(Random.alphanumeric.take(3).mkString, Random.alphanumeric.take(7).mkString)
  }

  val records: Seq[ProducerRecord[String, LineItem]] = lineItems map { item =>
    val topic    = Random.shuffle(topics).head
    val customer = Random.shuffle(customerIds).head
    new ProducerRecord[String, LineItem](topic, customer, item)
  }

  "must cogroup items" in {

    KafkaSpecHelper.createOrTruncateTopic(adminClient, purchaseInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, wishlistInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, cartInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, customerOutputTopicName, 1, 1)

    val consumer = new KafkaConsumer[String, Customer](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      implicitly[Deserializer[Customer]]
    )
    consumer.subscribe(List(customerOutputTopicName).asJavaCollection)
    produceTestData(records)

    val topology = makeCogroupTopology()
    println(topology.describe())
    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.start()

    KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = 500,
      maxAttempts = 10,
      abortOnFirstRecord = false
    )

    streams.close()
  }

  "must join items" in {

    KafkaSpecHelper.createOrTruncateTopic(adminClient, purchaseInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, wishlistInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, cartInputTopicName, 1, 1)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, customerOutputTopicName, 1, 1)

    streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"$suiteName-group2")
    val consumer = new KafkaConsumer[String, Customer](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      implicitly[Deserializer[Customer]]
    )
    consumer.subscribe(List(customerOutputTopicName).asJavaCollection)

    produceTestData(records)

    val topology = makeNoCogroupTopology()
    println(topology.describe())
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, s"${suiteName}-2")
    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.start()

    KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = 500,
      maxAttempts = 10,
      abortOnFirstRecord = false
    )

    streams.close()
  }

  private def produceTestData(records: Seq[ProducerRecord[String, LineItem]]): Unit = {
    val lineItemProducer = new KafkaProducer[String, LineItem](
      streamsConfiguration,
      Serdes.stringSerde.serializer(),
      itemSerializer
    )
    records foreach { r =>
      info(s"producing record for customer ${r.key()}: ${r.value()}")
      lineItemProducer.send(r).get()
    }
  }

  def makeCogroupTopology(): Topology = {

    val purchaseStream: KStream[String, LineItem] = builder.stream(purchaseInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("purchaseInput")
    )
    val wishlistStream: KStream[String, LineItem] = builder.stream(wishlistInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("wishlistInput")
    )
    val cartStream: KStream[String, LineItem] = builder.stream(cartInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("cartInput")
    )

    val groupedPurchases = purchaseStream.groupByKey(Grouped.`with`("groupedPurchase"))
    val groupedWishlist  = wishlistStream.groupByKey(Grouped.`with`("groupedWishlist"))
    val groupedCart      = cartStream.groupByKey(Grouped.`with`("groupedCart"))

    val cogrouped: CogroupedKStream[String, Customer] = groupedPurchases.cogroup[Customer]({
      case (customerName: String, item: LineItem, customer: Customer) =>
        info(s"started cogrouping: $customerName, $item, $customer")
        customer.copy(name = customerName, purchases = item :: customer.purchases)
    })

    val customerTable: KTable[String, Customer] = cogrouped
      .cogroup[LineItem](
        groupedWishlist,
        { case (customerName, item, customer) =>
          info(s"cogrouping wishlist: $customerName, $item, $customer")
          customer.copy(name = customerName, wishlist = item :: customer.wishlist)
        }
      )
      .cogroup[LineItem](
        groupedCart,
        { case (customerName, item, customer) =>
          info(s"cogrouping cart: $customerName, $item, $customer")
          customer.copy(name = customerName, cart = item :: customer.cart)
        }
      )
      .aggregate(Customer("CUSTOMER_NAME_PLACEHOLDER", Nil, Nil, Nil)) // Materialized implicit

    customerTable
      .toStream(Named.as("outStream"))
      .peek { case (k, v) => info(s"writing $k : $v to $customerOutputTopicName") }
      .to(customerOutputTopicName)

    builder.build()
  }

  def makeNoCogroupTopology(): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder()

    val purchaseStream: KStream[String, LineItem] = builder.stream(purchaseInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("purchaseInput2")
    )
    val wishlistStream: KStream[String, LineItem] = builder.stream(wishlistInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("wishlistInput2")
    )
    val cartStream: KStream[String, LineItem] = builder.stream(cartInputTopicName)(
      Consumed.`with`(Serdes.stringSerde, itemSerde).withName("cartInput2")
    )

    val groupedPurchases: KGroupedStream[String, LineItem] =
      purchaseStream.groupByKey(Grouped.`with`("groupedPurchase2"))
    val groupedWishlist = wishlistStream.groupByKey(Grouped.`with`("groupedWishlist2"))
    val groupedCart     = cartStream.groupByKey(Grouped.`with`("groupedCart2"))

    val aggregatedPurchases: KTable[String, Customer] =
      groupedPurchases.aggregate(Customer("CUSTOMER_NAME_PLACEHOLDER", Nil, Nil, Nil)) {
        (name, item, customer) =>
          info(s"aggregating purchases: $name, $item, $customer")
          customer.copy(name = name, purchases = item :: customer.purchases)
      }
    val aggregatedWishlist: KTable[String, Customer] =
      groupedWishlist.aggregate(Customer("CUSTOMER_NAME_PLACEHOLDER", Nil, Nil, Nil)) {
        (name, item, customer) =>
          info(s"aggregating wishlist: $name, $item, $customer")
          customer.copy(name = name, wishlist = item :: customer.wishlist)
      }
    val aggregatedCart: KTable[String, Customer] =
      groupedCart.aggregate(Customer("CUSTOMER_NAME_PLACEHOLDER", Nil, Nil, Nil)) {
        (name, item, customer) =>
          info(s"aggregating cart: $name, $item, $customer")
          customer.copy(name = name, cart = item :: customer.cart)
      }

    val fullCustomer: KTable[String, Customer] = aggregatedPurchases
      .outerJoin(aggregatedWishlist) { (customerPurchase, customerWishList) =>
        info(s"joining: customerPurchase: $customerPurchase, customerWishList: $customerWishList")
        val name      = if (null == customerPurchase) customerWishList.name else customerPurchase.name
        val purchases = if (null == customerPurchase) Nil else customerPurchase.purchases
        val wishList  = if (null == customerWishList) Nil else customerWishList.wishlist
        Customer(name, Nil, wishList, purchases)
      }
      .outerJoin(aggregatedCart) { (customerPurshaseWishlist, customerCart) =>
        info(
          s"joining: customerPurshaseWishlist: $customerPurshaseWishlist, customerCart: $customerCart"
        )
        val name = if (null == customerCart) customerPurshaseWishlist.name else customerCart.name
        val purchases =
          if (null == customerPurshaseWishlist) Nil else customerPurshaseWishlist.purchases
        val wishList =
          if (null == customerPurshaseWishlist) Nil else customerPurshaseWishlist.wishlist
        val cart = if (null == customerCart) Nil else customerCart.cart
        Customer(name, cart, wishList, purchases)
      }

    fullCustomer
      .toStream(Named.as("outStream2"))
      .peek { case (k, v) => info(s"writing $k : $v to $customerOutputTopicName") }
      .to(customerOutputTopicName)

    builder.build()
  }

  // cogroup topo
  // 1 aggregate state store
  /*
  Topologies:
   Sub-topology: 0
    Source: cartInput (topics: [cartInputTopic])
      --> COGROUPKSTREAM-AGGREGATE-0000000006
    Source: purchaseInput (topics: [purchaseTopic])
      --> COGROUPKSTREAM-AGGREGATE-0000000004
    Source: wishlistInput (topics: [wishlistInputTopic])
      --> COGROUPKSTREAM-AGGREGATE-0000000005
    Processor: COGROUPKSTREAM-AGGREGATE-0000000004 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> COGROUPKSTREAM-MERGE-0000000007
      <-- purchaseInput
    Processor: COGROUPKSTREAM-AGGREGATE-0000000005 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> COGROUPKSTREAM-MERGE-0000000007
      <-- wishlistInput
    Processor: COGROUPKSTREAM-AGGREGATE-0000000006 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> COGROUPKSTREAM-MERGE-0000000007
      <-- cartInput
    Processor: COGROUPKSTREAM-MERGE-0000000007 (stores: [])
      --> outStream
      <-- COGROUPKSTREAM-AGGREGATE-0000000004, COGROUPKSTREAM-AGGREGATE-0000000005, COGROUPKSTREAM-AGGREGATE-0000000006
    Processor: outStream (stores: [])
      --> KSTREAM-PEEK-0000000009
      <-- COGROUPKSTREAM-MERGE-0000000007
    Processor: KSTREAM-PEEK-0000000009 (stores: [])
      --> KSTREAM-SINK-0000000010
      <-- outStream
    Sink: KSTREAM-SINK-0000000010 (topic: customerOutputTopic)
      <-- KSTREAM-PEEK-0000000009
   */

  // join topo
  // three state stores
  /*
  Topologies:
   Sub-topology: 0
    Source: purchaseInput2 (topics: [purchaseTopic])
      --> KSTREAM-AGGREGATE-0000000004
    Source: wishlistInput2 (topics: [wishlistInputTopic])
      --> KSTREAM-AGGREGATE-0000000006
    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> KTABLE-JOINTHIS-0000000010
      <-- purchaseInput2
    Processor: KSTREAM-AGGREGATE-0000000006 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000005])
      --> KTABLE-JOINOTHER-0000000011
      <-- wishlistInput2
    Processor: KTABLE-JOINOTHER-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> KTABLE-MERGE-0000000009
      <-- KSTREAM-AGGREGATE-0000000006
    Processor: KTABLE-JOINTHIS-0000000010 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000005])
      --> KTABLE-MERGE-0000000009
      <-- KSTREAM-AGGREGATE-0000000004
    Source: cartInput2 (topics: [cartInputTopic])
      --> KSTREAM-AGGREGATE-0000000008
    Processor: KSTREAM-AGGREGATE-0000000008 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000007])
      --> KTABLE-JOINOTHER-0000000014
      <-- cartInput2
    Processor: KTABLE-MERGE-0000000009 (stores: [])
      --> KTABLE-JOINTHIS-0000000013
      <-- KTABLE-JOINTHIS-0000000010, KTABLE-JOINOTHER-0000000011
    Processor: KTABLE-JOINOTHER-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003, KSTREAM-AGGREGATE-STATE-STORE-0000000005])
      --> KTABLE-MERGE-0000000012
      <-- KSTREAM-AGGREGATE-0000000008
    Processor: KTABLE-JOINTHIS-0000000013 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000007])
      --> KTABLE-MERGE-0000000012
      <-- KTABLE-MERGE-0000000009
    Processor: KTABLE-MERGE-0000000012 (stores: [])
      --> outStream2
      <-- KTABLE-JOINTHIS-0000000013, KTABLE-JOINOTHER-0000000014
    Processor: outStream2 (stores: [])
      --> KSTREAM-PEEK-0000000016
      <-- KTABLE-MERGE-0000000012
    Processor: KSTREAM-PEEK-0000000016 (stores: [])
      --> KSTREAM-SINK-0000000017
      <-- outStream2
    Sink: KSTREAM-SINK-0000000017 (topic: customerOutputTopic)
      <-- KSTREAM-PEEK-0000000016
   */

}
