package com.example

import org.apache.kafka.clients.admin.{
  AdminClient,
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
import scala.jdk.FutureConverters._
import scala.jdk.javaapi.CollectionConverters.asJava
import scala.util.Try

case object TopicHelper extends LogSupport {

  // TODO - make configurable to better fit diff environments
  val retryWaitMs    = 500
  val metadataWaitMs = 30000
  val maxRetries     = 10
  val defaultNumOfPartitions = 1.shortValue // 3 for cloud, 1 for local would make sense
  val defaultReplicationFactor = 3.shortValue // 3 for cloud, 1 for local would make sense

  def createTopic(
      adminClient: AdminClient,
      topicName: String,
      numberOfPartitions: Short = 1,
      replicationFactor: Short = defaultReplicationFactor,
      skipExistanceCheck: Boolean = false
  ): Either[String, String] = {

    // TODO - add flag - do not delete anything without confirmation via call flag
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

      val tryCreateTopics = Try {
        val topicsCreationResult =
          adminClient.createTopics(Collections.singleton(newTopic)).all().toCompletionStage.asScala
        Await.result(topicsCreationResult, metadataWaitMs.seconds)
      }
      tryCreateTopics.toEither.fold(
        e => Left(s"creation of topic $topicName failed: ${e.getMessage}"),
        _ => Right(s"topic $topicName successfully created")
      )
    } else {
      val topicExistsMsg = s"topic $topicName already exists, skipping"
      info(topicExistsMsg)
      Right(topicExistsMsg)
    }
  }

  def createOrTruncateTopic(
      adminClient: AdminClient,
      topicName: String,
      numberOfPartitions: Short = defaultNumOfPartitions,
      replicationFactor: Short = defaultReplicationFactor
  ): Either[String, String] =
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

  def deleteTopic(adminClient: AdminClient, topicName: String): Either[String, String] =
    deleteTopics(adminClient, List(topicName))

  def deleteTopics(
      adminClient: AdminClient,
      topicNames: Iterable[String]
  ): Either[String, String] = {
    info(s"deleting topics $topicNames")
    val tryDelete: Try[Void] = Try {
      val getTopicDeletionResult: Future[Void] =
        adminClient.deleteTopics(topicNames.asJavaCollection).all().toCompletionStage.asScala
      Await.result(getTopicDeletionResult, metadataWaitMs.seconds)
    }
    tryDelete.toEither.fold(
      e => Left(s"deletion of topics $topicNames failed: ${e.getMessage}"),
      _ => Right(s"topics $topicNames successfully deleted")
    )
  }

  def deleteTopicsByPrefix(
      adminClient: AdminClient,
      topicPrefix: String
  ): Either[String, String] = {

    val upperCasePrefix = topicPrefix.toUpperCase

    val getTopicNames: Future[util.Set[String]] =
      adminClient.listTopics().names().toCompletionStage.asScala
    val topicNames = Await.result(getTopicNames, metadataWaitMs.seconds).asScala
    val topicsToDelete: mutable.Set[String] =
      topicNames.filter(_.toUpperCase.startsWith(upperCasePrefix))
    deleteTopics(adminClient, topicsToDelete)
  }

  val truncateTopic: (AdminClient, String, Short, Short) => Either[String, String] =
    (adminClient: AdminClient, topic: String, partitions: Short, replicationFactor: Short) => {
      for {
        topicDeleted            <- deleteTopic(adminClient, topic)
        topicConfirmedAsDeleted <- waitForTopicToBeDeleted(adminClient, topic)
        topicCreated <- createTopic(
          adminClient,
          topic,
          partitions,
          replicationFactor,
          skipExistanceCheck = true
        )
        topicConfirmedAsCreated <- waitForTopicToExist(adminClient, topic)
      } yield topicConfirmedAsCreated
    }

  val waitForTopicToExist: (AdminClient, String) => Either[String, String] =
    (adminClient: AdminClient, topic: String) => waitForTopic(adminClient, topic, true)

  val waitForTopicToBeDeleted: (AdminClient, String) => Either[String, String] =
    (adminClient: AdminClient, topic: String) => waitForTopic(adminClient, topic, false)

  private val waitForTopic: (AdminClient, String, Boolean) => Either[String, String] =
    (adminClient: AdminClient, topic: String, toExist: Boolean) => {
      val condition   = if (toExist) "exists" else "is deleted"
      var topicExists = !toExist
      var retries     = 0
      while ((topicExists != toExist) && retries < maxRetries) {
        Thread.sleep(retryWaitMs)
        topicExists = doesTopicExist(adminClient, topic)
        retries = retries + 1
      }
      if (topicExists == toExist) Right(s"topic $topic $condition")
      else
        Left(
          s"aborting wait until topic $topic $condition after $maxRetries reties with $retryWaitMs ms backoff"
        )
    }

  val doesTopicExist: (AdminClient, String) => Boolean =
    (adminClient: AdminClient, topic: String) => {
      val names =
        Await.result(
          adminClient.listTopics().names().toCompletionStage.asScala,
          metadataWaitMs.seconds
        )
      names.contains(topic)
    }

  // a topic that does not exist is assumed to have the same wordCountConfig as any other topic
  val doesTopicHaveSameConfig: (AdminClient, String, Int, Short) => Boolean =
    (adminClient: AdminClient, topic: String, partitionCount: Int, replicationFactor: Short) => {

      if (doesTopicExist(adminClient, topic)) {
        val topicDescriptions: mutable.Map[String, TopicDescription] = Await
          .result(
            adminClient
              .describeTopics(List(topic).asJava)
              .allTopicNames()
              .toCompletionStage
              .asScala,
            metadataWaitMs.seconds
          )
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
