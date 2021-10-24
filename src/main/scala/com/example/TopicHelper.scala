package com.example

import org.apache.kafka.clients.admin.{
  AdminClient,
  CreateTopicsResult,
  DeleteTopicsResult,
  NewTopic,
  TopicDescription
}
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.config.TopicConfig
import wvlet.log.LogSupport

import java.util
import java.util.Collections
import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.CollectionConverters.asJava
import scala.util.Try

object TopicHelper extends LogSupport with FutureConverter {

  val metadataWait             = 2000
  val defaultReplicationFactor = 3 // 3 for cloud, 1 for local would make sense

  def createTopic(
      adminClient: AdminClient,
      topicName: String,
      numberOfPartitions: Int = 1,
      replicationFactor: Short = 3,
      skipExistanceCheck: Boolean = false
  ): Either[String, String] = {

    val needsDeletion =
      !doesTopicHaveSameConfig(adminClient, topicName, numberOfPartitions, replicationFactor)
    if (needsDeletion) {
      warn(s"existing topic $topicName has different config, deleting")
      deleteTopic(adminClient, topicName)
      waitForTopicToBeDeleted(adminClient, topicName)
    }

    val needsCreation = skipExistanceCheck || !doesTopicExist(adminClient, topicName)
    if (needsCreation) {
      info(s"Creating topic ${topicName}")

      val configs: Map[String, String] =
        if (replicationFactor < 3) Map(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "1")
        else Map.empty

      val newTopic: NewTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor)
      newTopic.configs(configs.asJava)
      info(s"with config $newTopic")
      try {
        val topicsCreationResult: CreateTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic))
        topicsCreationResult.all().get()
        Right(s"successfully created topic $topicName")
      } catch {
        case e: Throwable =>
          debug(e)
          Left(e.getMessage)
      }
    } else {
      val topicExistsMsg = s"topic $topicName already exists, skipping"
      info(topicExistsMsg)
      Left(topicExistsMsg)
    }
  }

  def createOrTruncateTopic(
      adminClient: AdminClient,
      topicName: String,
      numberOfPartitions: Int = 1,
      replicationFactor: Short = 3
  ): Any =
    if (doesTopicExist(adminClient, topicName)) {
      println(s"truncating topic $topicName")
      truncateTopic(adminClient, topicName, numberOfPartitions, replicationFactor)
    } else {
      println(s"creating topic $topicName")
      createTopic(
        adminClient,
        topicName,
        numberOfPartitions,
        replicationFactor,
        skipExistanceCheck = true
      )
    }

  def deleteTopic(adminClient: AdminClient, topicName: String): Any =
    deleteTopics(adminClient, List(topicName))

  def deleteTopics(adminClient: AdminClient, topicNames: Iterable[String]): Any = {
    info(s"deleting topics $topicNames")
    try {
      val topicDeletionResult: DeleteTopicsResult =
        adminClient.deleteTopics(topicNames.asJavaCollection)
      topicDeletionResult.all().get()
    } catch {
      case e: Throwable => debug(e)
    }
  }

  def deleteTopicsByPrefix(adminClient: AdminClient, topicPrefix: String): Any = {

    val upperCasePrefix = topicPrefix.toUpperCase

    val getTopicNames: Future[util.Set[String]] = adminClient.listTopics().names().toScalaFuture
    val topicNames                              = Await.result(getTopicNames, 60.seconds).asScala
    val topicsToDelete: mutable.Set[String] =
      topicNames.filter(_.toUpperCase.startsWith(upperCasePrefix))
    deleteTopics(adminClient, topicsToDelete)
  }

  // TODO - eliminate waiting by catching the exceptions on retry
  val truncateTopic: (AdminClient, String, Int, Short) => Unit =
    (adminClient: AdminClient, topic: String, partitions: Int, replicationFacor: Short) => {

      val javaTopicSet = asJava(Set(topic))
      info(s"deleting topic $topic")
      val deleted: Try[Void] = Try {
        Await.result(adminClient.deleteTopics(javaTopicSet).all().toScalaFuture, 10.seconds)
      }
      waitForTopicToBeDeleted(adminClient, topic)

      Thread.sleep(metadataWait)
      info(s"creating topic $topic")
      val created: Try[Void] = Try {
        val newTopic =
          new NewTopic(
            topic,
            partitions,
            replicationFacor
          ) // need to box the short here to prevent ctor ambiguity
        val createTopicsResult: CreateTopicsResult = adminClient.createTopics(asJava(Set(newTopic)))
        Await.result(createTopicsResult.all().toScalaFuture, 10.seconds)
      }
      waitForTopicToExist(adminClient, topic)
      Thread.sleep(metadataWait)
    }

  val waitForTopicToExist: (AdminClient, String) => Unit =
    (adminClient: AdminClient, topic: String) => {
      var topicExists = false
      while (!topicExists) {
        Thread.sleep(100)
        topicExists = doesTopicExist(adminClient, topic)
        if (!topicExists) info(s"topic $topic still does not exist")
      }
    }

  val waitForTopicToBeDeleted: (AdminClient, String) => Unit =
    (adminClient: AdminClient, topic: String) => {
      var topicExists = true
      while (topicExists) {
        Thread.sleep(100)
        topicExists = doesTopicExist(adminClient, topic)
        if (topicExists) info(s"topic $topic still exists")
      }
    }

  val doesTopicExist: (AdminClient, String) => Boolean =
    (adminClient: AdminClient, topic: String) => {
      val names = Await.result(adminClient.listTopics().names().toScalaFuture, 10.seconds)
      names.contains(topic)
    }

  // a topic that does not exist is assumed to have the same config as any other topic
  val doesTopicHaveSameConfig: (AdminClient, String, Int, Short) => Boolean =
    (adminClient: AdminClient, topic: String, partitionCount: Int, replicationFactor: Short) => {

      if (doesTopicExist(adminClient, topic)) {

        val topicDescriptions: mutable.Map[String, TopicDescription] = Await
          .result(adminClient.describeTopics(List(topic).asJava).all().toScalaFuture, 60.seconds)
          .asScala

        topicDescriptions.get(topic).exists { desc =>
          val partitions: mutable.Buffer[TopicPartitionInfo] = desc.partitions().asScala
          info(s"topic $topic partitions: $partitions")
          val replicaCounts: Set[Int] = partitions.map(_.replicas().size()).toSet
          info(s"topic $topic replica count: $replicaCounts")
          partitions.size == partitionCount && replicaCounts.size == 1 && replicaCounts.head == replicationFactor
        }
      } else true
    }

}
