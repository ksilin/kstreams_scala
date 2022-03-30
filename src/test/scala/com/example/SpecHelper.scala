package com.example

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, ConsumerRecords }
import wvlet.log.LogSupport

import java.time
import scala.jdk.CollectionConverters._

object SpecHelper extends LogSupport {

  // assumes consumer is already subscribed
  def fetchAndProcessRecords[K, V](
      consumer: Consumer[K, V],
      process: ConsumerRecord[K, V] => Unit = { r: ConsumerRecord[K, V] =>
        info(s"${r.topic()} | ${r.partition()} | ${r.offset()}: ${r.key()} | ${r.value()}")
      },
      filter: ConsumerRecord[K, V] => Boolean = { _: ConsumerRecord[K, V] => true },
      abortOnFirstRecord: Boolean = true,
      maxAttempts: Int = 100,
      pause: Int = 100
  ): Iterable[ConsumerRecord[K, V]] = {
    val duration: time.Duration                    = java.time.Duration.ofMillis(100)
    var found                                      = false
    var records: Iterable[ConsumerRecord[K, V]]    = Nil
    var allRecords: Iterable[ConsumerRecord[K, V]] = Nil
    var attempts                                   = 0
    while (!found && attempts < maxAttempts) {
      val consumerRecords: ConsumerRecords[K, V] = consumer.poll(duration)

      attempts = attempts + 1
      found = !consumerRecords.isEmpty && abortOnFirstRecord
      if (!consumerRecords.isEmpty) {
        info(s"fetched ${consumerRecords.count()} records on attempt $attempts")
        records = consumerRecords.asScala.filter(filter)
        allRecords ++= records
        records foreach { r => process(r) }
      }
      Thread.sleep(pause)
    }
    if (attempts >= maxAttempts)
      info(s"no data received in $attempts attempts")
    allRecords
  }

}
