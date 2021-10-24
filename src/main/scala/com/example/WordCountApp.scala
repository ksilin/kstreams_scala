package com.example

import com.example.WordCount.Config
import io.confluent.common.utils.TestUtils
import kafka.tools.StreamsResetter
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import wvlet.log.LogSupport

import java.util.Properties
import scala.jdk.CollectionConverters._

object WordCountApp extends App with LogSupport {

  import scopt.OParser
  val optParserBuilder = OParser.builder[Config]

  val appName = "wordCountApp"

  val parser = {
    import optParserBuilder._

    OParser.sequence(
      programName(appName),
      head("word count", "v0.1"),
      // option -i, --inputtopic
      opt[String]('i', "inputtopic")
        .action((x, c) => c.copy(inputTopic = x))
        .text("input topic is a string"),
      // option -o, --outputtopic
      opt[String]('i', "outputtopic")
        .action((x, c) => c.copy(outputTopic = x))
        .text("output topic is a string")
    )
  }

  val cloudProps: CloudProps = CloudProps.create()
  val props: Properties = cloudProps.commonProps.clone().asInstanceOf[Properties]
  val fallBackConfig: Config = Config()

  val config: Config = OParser.parse(parser, args, fallBackConfig)  match {
    case Some(config) => config
    case _ =>
      warn(s"failed to parse arguments, using fallback config: $fallBackConfig ")
      fallBackConfig
  }

  val streamsProps: Properties = new Properties()
  private val suiteName1: String       = appName
  streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName1)
  streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  streamsProps.put(
    StreamsConfig.STATE_DIR_CONFIG,
    TestUtils.tempDirectory().getAbsolutePath
  )
  streamsProps.put(
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
    0
  ) // pipe events through immediately
  streamsProps.put(
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
    0
  ) // pipe events through immediately


  val fullProps: Properties = new Properties()
  fullProps.putAll(props)
  fullProps.putAll(streamsProps)
  fullProps.putAll(CloudProps.cloudStreamsConfiguration)

  val builder = new StreamsBuilder()

  val topo: Topology = WordCount.createTopology(builder, config.inputTopic, config.outputTopic)
  info(s"$appName topology: ")
  info(topo.describe())

  val adminClient = AdminClient.create(fullProps)
  setupTopics(adminClient, config)

  val streams: KafkaStreams = new KafkaStreams(builder.build, fullProps)
  streams.cleanUp() // remove local data before start
  streams.start()

  sys.addShutdownHook(streams.close())

  def setupTopics(adminClient: AdminClient, config: Config ): Unit ={

    // StreamsResetter.main()
    TopicHelper.deleteTopicsByPrefix(adminClient, appName)
    TopicHelper.createTopic(adminClient, config.inputTopic, numberOfPartitions = 2)
    TopicHelper.createTopic(adminClient, config.outputTopic, numberOfPartitions = 1)
  }

}
