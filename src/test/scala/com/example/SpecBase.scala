package com.example

import io.confluent.common.utils.TestUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import java.util.Properties

class SpecBase extends AnyFreeSpecLike with LogSupport with Matchers {

  val streamsConfiguration: Properties = new Properties()
  private val suiteName1: String       = this.suiteName
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, suiteName1)
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
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
    0
  ) // pipe events through immediately

  val builder: StreamsBuilder = new StreamsBuilder()

}