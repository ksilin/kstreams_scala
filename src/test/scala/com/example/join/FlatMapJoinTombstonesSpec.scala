package com.example.join

import com.example.serde.{GsonDeserializer, GsonSerializer}
import com.example.SpecBase
import com.example.util.KafkaSpecHelper
import io.circe.generic.auto._
import nequi.circe.kafka._
import net.christophschubert.cp.testcontainers.{CPTestContainerFactory, ConfluentServerContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Named, ValueJoiner}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.testcontainers.containers.Network

import _root_.scala.jdk.CollectionConverters._
import _root_.scala.util.Random

/*
  The purpose - for an entity (here - a parcel), we get several types of events (here only two) - CREATED and DELETED.
  The idea is to split the delete event into two - the DELETE event proper and a tombstone via flatMap.
  The tombstone is used to clean state stores after processing the DELETED event.

  With a simple topology - no joins, no state stores - the events behave as expected. First comes the CREATED event, then the DELETEd event and then the tombstone

  With a slightly more complex topology, things get weird:

  * no joins
  * stream-table join - not convenient, since only table-table & stream-globalKtable joins support FK joins
  * table-table join - shoudl work
 */
class FlatMapJoinTombstonesSpec extends SpecBase {

  case class Address(id: String, street: String)
  case class ParcelEvent(
      id: String,
      addressId: String,
      address: Address,
      event: String,
      createdAt: Long,
      updatedAt: Long
  )

  private val containerFactory = new CPTestContainerFactory(Network.newNetwork())
  private val broker: ConfluentServerContainer =
    containerFactory
      .createConfluentServer()
  broker.start()

  private val bootstrap: String = broker.getBootstrapServers

  private val addressInputTopicName      = "addressInputTopic"
  private val parcelEventsInputTopicName = "parcelEventsInputTopic"
  private val outputTopicName            = "outputTopic"

  // implicit serdes cant work with tombstones / null values
  private val parcelSerializer   = new GsonSerializer[ParcelEvent]
  private val parcelDeserializer = new GsonDeserializer[ParcelEvent](classOf[ParcelEvent])
  private val parcelSerde: WrapperSerde[ParcelEvent] =
    new WrapperSerde(parcelSerializer, parcelDeserializer)

  private val addressSerializer: Serializer[Address]     = implicitly
  private val addressDeserializer: Deserializer[Address] = implicitly
  private val addressSerde: Serde[Address]               = implicitly

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, s"${suiteName}-group")
  private val adminClient: AdminClient = AdminClient.create(streamsConfiguration)

  private val parcelIds: List[String] =
    (1 to 2).toList map (_ => "parcel-" + Random.alphanumeric.take(3).mkString)
  private val addressIds: List[String] =
    (1 to 2).toList map (_ => "address-" + Random.alphanumeric.take(3).mkString)

  private val CREATED = "CREATED"
  private val DELETED = "DELETED"

  KafkaSpecHelper.createTopic(adminClient, addressInputTopicName, 1, 1)
  KafkaSpecHelper.createTopic(adminClient, parcelEventsInputTopicName, 1, 1)
  KafkaSpecHelper.createTopic(adminClient, outputTopicName, 1, 1)


  private val consumedParcelIput: Consumed[String, ParcelEvent] = Consumed.`with`(Serdes.stringSerde, parcelSerde).withName("parcelEventInput")
  private val consumedAddressInput: Consumed[String, Address] = Consumed.`with`(Serdes.stringSerde, addressSerde).withName("addressInput")

  private val parcelEventAddressJoiner: ValueJoiner[ParcelEvent, Address, ParcelEvent] = (parcel: ParcelEvent, addr: Address) => {
    if (parcel == null) {
      warn("tombstone event made it to the join")
    }
    info(s"joining $parcel with $addr")
    // parcel.copy(address=addr) throws an exception
    ParcelEvent(
      id = parcel.id,
      addressId = parcel.addressId: String,
      address = addr,
      event = parcel.event,
      createdAt = parcel.createdAt,
      updatedAt = System.currentTimeMillis()
    )
  }

  "must process all records and tombstones in sequence" in {

    val topology: Topology = makeTopologyWithFKJoin()
    println(topology.describe())

    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.cleanUp()
    streams.start()

    produceAddresses(addressIds)
    createAndProduceParcels(parcelIds, CREATED)
    Thread.sleep(1000)
    createAndProduceParcels(parcelIds, DELETED)

    val consumer = new KafkaConsumer[String, ParcelEvent](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      parcelDeserializer
    )
    consumer.subscribe(List(outputTopicName).asJavaCollection)
    val records: Iterable[ConsumerRecord[String, ParcelEvent]] = KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = 500,
      maxAttempts = 10,
      abortOnFirstRecord = false
    )
    records.foreach(r => warn(r))
    streams.close()
  }

  "must process all records and tombstones in sequence without joins" in {

    val topology: Topology = makeTopologyNoJoin()
    println(topology.describe())

    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.cleanUp()
    streams.start()

    createAndProduceParcels(parcelIds, CREATED)
    Thread.sleep(1000)
    createAndProduceParcels(parcelIds, DELETED)

    val consumer = new KafkaConsumer[String, ParcelEvent](
      streamsConfiguration,
      Serdes.stringSerde.deserializer(),
      parcelDeserializer
    )
    consumer.subscribe(List(outputTopicName).asJavaCollection)
    val records: Iterable[ConsumerRecord[String, ParcelEvent]] = KafkaSpecHelper.fetchAndProcessRecords(
      consumer,
      pause = 500,
      maxAttempts = 10,
      abortOnFirstRecord = false
    )
    records.foreach(r => warn(r))
    streams.close()
  }

  def makeTopologyWithFKJoin(): Topology = {

    val parcelEventStream: KStream[String, ParcelEvent] =
      builder.stream(parcelEventsInputTopicName)(consumedParcelIput)

    val addressStream: KStream[String, Address] = builder.stream(addressInputTopicName)(consumedAddressInput)

    val addrTable: KTable[String, Address] = addressStream.toTable(Named.as("addresses"))

    // this is where the sausage is made
    val flatMapped: KStream[String, ParcelEvent] = parcelEventStream.flatMapValues((k, pe) =>
      if (pe.event == DELETED) {
        info(s"flatMap: producing a tombstone after the delete event for key $k")
        List(pe, null.asInstanceOf[ParcelEvent])
      } else List(pe)
    )

    val peeked = flatMapped.peek((k, v) => info(s"after flatMap $k, $v to $outputTopicName"))
    val parcelTable: KTable[String, ParcelEvent] = flatMapped.toTable(
      Named.as("parcelEventTable"),
      Materialized.`with`(Serdes.stringSerde, parcelSerde).withCachingDisabled()
    )
    parcelTable.toStream.peek((k, v) => info(s"peek parcelTable $k, $v to $outputTopicName"))

    // Table cannot leftJoin a Stream, so joining from the stream side
    val addressAndParcel: KTable[String, ParcelEvent] = parcelTable.join(
      addrTable,
      (p: ParcelEvent) => p.addressId,
      parcelEventAddressJoiner,
      Materialized.`with`(Serdes.stringSerde, parcelSerde).withCachingDisabled()
    )
    addressAndParcel.toStream
      .peek((k, v) => info(s"writing $k, $v to $outputTopicName"))
      .to(outputTopicName)(Produced.`with`(Serdes.stringSerde, parcelSerde))

    builder.build()
  }

  def makeTopologyNoJoin(): Topology = {

    val parcelEventStream: KStream[String, ParcelEvent] =
      builder.stream(parcelEventsInputTopicName)(consumedParcelIput)

    // this is where the sausage is made
    val flatMapped: KStream[String, ParcelEvent] = parcelEventStream.flatMapValues((k, pe) =>
      if (pe.event == DELETED) {
        info(s"flatMap: producing a tombstone after the delete event for key $k")
        List(pe, null.asInstanceOf[ParcelEvent])
      } else List(pe)
    )

    //val peeked = flatMapped.peek((k, v) => info(s"after flatMap $k, $v to $outputTopicName"))
    val parcelTable: KTable[String, ParcelEvent] = flatMapped.toTable(
      Named.as("parcelEventTable"),
      Materialized.`with`(Serdes.stringSerde, parcelSerde)
    )

    // Table cannot leftJoin a Stream, so joining from the stream side
    parcelTable.toStream
      .peek((k, v) => info(s"writing $k, $v to $outputTopicName"))
      .to(outputTopicName)(Produced.`with`(Serdes.stringSerde, parcelSerde))

    builder.build()
  }




  private def produceAddresses(addressIds: List[String]): Unit = {

    val addressProducer = new KafkaProducer[String, Address](
      streamsConfiguration,
      Serdes.stringSerde.serializer(),
      addressSerializer
    )

    addressIds foreach { addId =>
      val address = Address(addId, Random.alphanumeric.take(5).mkString)
      val now     = System.currentTimeMillis()
      val record =
        new ProducerRecord[String, Address](addressInputTopicName, null, now, addId, address)
      val sent = addressProducer.send(record).get()
      info(s"address sent with id $addId & ts $now : ${sent.offset()}")
    }
  }

  private def createAndProduceParcels(parcelIds: List[String], event: String): Unit = {

    val parcelCreatedProducer = new KafkaProducer[String, ParcelEvent](
      streamsConfiguration,
      Serdes.stringSerde.serializer,
      parcelSerializer
    )

    parcelIds.zipWithIndex foreach { case (id, i) =>
      val now         = System.currentTimeMillis() + (Math.pow(2, i) * 1000).toLong
      val addressId   = Random.shuffle(addressIds).head
      val parcelEvent = ParcelEvent(id, addressId, null, event, now, now)
      val sent: RecordMetadata = parcelCreatedProducer
        .send(
          new ProducerRecord[String, ParcelEvent](
            parcelEventsInputTopicName,
            null,
            now,
            id,
            parcelEvent
          )
        )
        .get()
      info(s"parcel event produced with id $id & event $event : ${sent.offset()}")
    }
  }

}
