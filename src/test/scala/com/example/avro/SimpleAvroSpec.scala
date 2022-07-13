package com.example.avro

import com.example.SpecBase
import com.example.avro.specificrecords.NameRecord
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.admin.{
  AdminClient,
  AdminClientConfig,
  ListTopicsResult,
  TopicListing
}
import org.apache.kafka.streams.{
  KeyValue,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}
import org.apache.kafka.streams.scala.kstream.{ Consumed, Produced }
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.BeforeAndAfterAll

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

// needs "org.apache.kafka" % "kafka-streams-avro-myRecordSerde" dep to work
class SimpleAvroSpec extends SpecBase with BeforeAndAfterAll {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster =
    EmbeddedSingleNodeKafkaCluster.build()

  private val SCHEMA_REGISTRY_SCOPE    = suiteName
  private val MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE

  var brokerPort = ""

  private val inputTopic  = "inputTopic"
  private val outputTopic = "outputTopic"

  override def beforeAll(): Unit = {
    privateCluster.start()

    // already has a :prefix
    brokerPort = privateCluster.bootstrapServers()
    println(brokerPort)
    println("creating topics")
    createTopics()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    privateCluster.stop()
    super.afterAll()
  }

  def createTopics() {
    privateCluster.createTopic(inputTopic, 2, 1)
    privateCluster.waitForTopicsToBePresent(inputTopic)
  }

  "test topics are being created" in {
    val localhostConfig = "localhost" + brokerPort
    println(localhostConfig)
    val adminProperties = new Properties()
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, localhostConfig)
    val adminClient: AdminClient                    = AdminClient.create(adminProperties)
    val listTopics: ListTopicsResult                = adminClient.listTopics()
    val listings: mutable.Map[String, TopicListing] = listTopics.namesToListings().get().asScala
    listings foreach println
  }

  "test avro messages are being written" in {

    val keySerde = Serdes.stringSerde

    streamsConfiguration.put(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      MOCK_SCHEMA_REGISTRY_URL
    )

    // XXX - the Serde has be configured as well - how to do this for a real application?
    // otherwise, SR will be null and you will run into an issue:
    // https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/AbstractKafkaAvroSerializer.java#L96
    // InvalidConfigurationException
    // "You must configure() before serialize()"
    // " or use serializer constructor with SchemaRegistryClient"
    val valueSerde: SpecificAvroSerde[NameRecord] = new SpecificAvroSerde[NameRecord]
    valueSerde.configure(streamsConfiguration.asInstanceOf[java.util.Map[String, String]], false)

    builder
      .stream(inputTopic)(Consumed.`with`(keySerde, valueSerde))
      .peek((k, v) => info(s"peeking into input stream: ${k} : ${v}"))
      .mapValues { v =>
        v
      }
      .to(outputTopic)(Produced.`with`(keySerde, valueSerde))

    val topology: Topology = builder.build()
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTestTopic: TestInputTopic[String, NameRecord] = topologyTestDriver
      .createInputTopic(inputTopic, keySerde.serializer(), valueSerde.serializer())

    val outputTestTopic: TestOutputTopic[String, NameRecord] = topologyTestDriver
      .createOutputTopic(outputTopic, keySerde.deserializer(), valueSerde.deserializer())

    val inputValues =
      List(NameRecord("one"), NameRecord("two"), NameRecord("three")).map(r => (r.name, r))
    val testRecords = inputValues.map { case (k, v) => new KeyValue(k, v) }.asJava
    inputTestTopic.pipeKeyValueList(testRecords)

    val outputRecords: util.List[KeyValue[String, NameRecord]] =
      outputTestTopic.readKeyValuesToList()

    outputRecords mustBe testRecords
  }

}
