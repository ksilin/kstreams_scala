package com.example.join

import com.example.{KafkaSpecHelper, SpecBase}
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.{OutputFrame, WaitingConsumer}

import java.util.concurrent.TimeUnit
import _root_.scala.util.Random
import _root_.scala.jdk.CollectionConverters._

class CogroupSpec extends SpecBase {

  case class LineItem(id: String, name: String)
  case class Customer(
      name: String,
      cart: List[LineItem],
      wishlist: List[LineItem],
      purchases: List[LineItem]
  )

  val consumer: WaitingConsumer = new WaitingConsumer()

  private val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  private val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  broker.followOutput(consumer, OutputFrame.OutputType.STDOUT)

  consumer.waitUntil({frame: OutputFrame =>
    frame.getUtf8String.contains("started")}, 60, TimeUnit.SECONDS)

  private val bootstrap: String = broker.getBootstrapServers

  private val purchaseInputTopicName = "purchaseTopic"
  private val wishlistInputTopicName = "wishlistInputTopic"
  private val cartInputTopicName     = "cartInputTopic"
  private val topics                 = List(purchaseInputTopicName, wishlistInputTopicName, cartInputTopicName)

  private val customerOutputTopicName = "customerOutputTopic"

  private val itemSerializer: Serializer[LineItem]     = implicitly
  private val itemSerde: Serde[LineItem]               = implicitly

  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName)
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  private val customerIds: List[String] = List("Shaun Shopper", "Berta Buyer", "Peter Purchaser")

  private val lineItems: Seq[LineItem] = (1 to 5) map { _ =>
    LineItem(Random.alphanumeric.take(3).mkString, Random.alphanumeric.take(7).mkString)
  }

  "must cogroup items" in {

    KafkaSpecHelper.createTopic(adminClient, purchaseInputTopicName, 1, 1)
    KafkaSpecHelper.createTopic(adminClient, wishlistInputTopicName, 1, 1)
    KafkaSpecHelper.createTopic(adminClient, cartInputTopicName, 1, 1)

    produceTestData

    val topology = makeTopology()
    println(topology.describe())
    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.start()

    val consumer = new KafkaConsumer[String, Customer](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      implicitly[Deserializer[Customer]]
    )
    consumer.subscribe(List(customerOutputTopicName).asJavaCollection)
    KafkaSpecHelper.fetchAndProcessRecords(consumer, pause = 500, maxAttempts = 10, abortOnFirstRecord = false)

    streams.close()
  }

  private def produceTestData = {
    val lineItemProducer = new KafkaProducer[String, LineItem](
      streamsConfiguration,
      Serdes.stringSerde.serializer(),
      itemSerializer
    )

    lineItems foreach { item =>
      val topic = Random.shuffle(topics).head
      val customer = Random.shuffle(customerIds).head
      info(s"producing $topic record for customer $customer: $item")
      val record = new ProducerRecord[String, LineItem](topic, customer, item)
      lineItemProducer.send(record).get()
    }
  }

  def makeTopology(): Topology = {

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
          customer.copy(name = customerName, wishlist = item :: customer.wishlist) }
      )
      .cogroup[LineItem](
        groupedCart,
        { case (customerName, item, customer) =>
          info(s"cogrouping cart: $customerName, $item, $customer")
          customer.copy(name = customerName, cart = item :: customer.cart) }
      )
      .aggregate(Customer("CUSTOMER_NAME_PLACEHOLDER", Nil, Nil, Nil)) // Materialized implicit

    customerTable.toStream(Named.as("outStream")).peek{ case (k, v) => info(s"writing $k : $v to $customerOutputTopicName")}.to(customerOutputTopicName)

    builder.build()
  }

}
