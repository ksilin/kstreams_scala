package com.example

import com.example.util.FutureConverter
import io.confluent.common.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import java.util.Properties

class SpecBase extends AnyFreeSpecLike with LogSupport with Matchers with FutureConverter {

  val streamsConfiguration: Properties = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, this.suiteName)
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "DUMMY_CONFIG")
  // we want to see the topology exactly as we created it:
  streamsConfiguration.put(
    StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
    StreamsConfig.NO_OPTIMIZATION
  )
  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, this.suiteName)
  // streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  // streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  // allowing caching, but putting upper bound on the time records remain in cache
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  streamsConfiguration.put(
    StreamsConfig.STATE_DIR_CONFIG,
    TestUtils.tempDirectory().getAbsolutePath
  )
  streamsConfiguration.put(
    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
    0
  ) // pipe events through immediately
  streamsConfiguration.put(
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
    10
  ) // pipe events through immediately

  val builder: StreamsBuilder = new StreamsBuilder()

}
