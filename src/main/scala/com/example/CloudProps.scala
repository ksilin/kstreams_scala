package com.example

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }
import org.apache.kafka.streams.StreamsConfig

import java.net.URL
import java.util.Properties

case class CloudProps(bootstrapBroker: String, apiKey: String, apiSecret: String) {

  val commonProps: Properties = new Properties()
  commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker)
  commonProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
  commonProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
  commonProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")

  val saslString: String =
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${apiKey}" password="${apiSecret}";""".stripMargin
  commonProps.setProperty("sasl.jaas.config", saslString)
}

case object CloudProps {
  def create(configFileUrl: Option[URL] = None): CloudProps = {
    val config = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    CloudProps(
      config.getString("cluster.bootstrap"),
      config.getString("cluster.key"),
      config.getString("cluster.secret")
    )
  }

  val cloudStreamsConfiguration: Properties = new Properties()
  cloudStreamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3)
}
