package com.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{ Branched, BranchedKStream, KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }

import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BranchJoinMergeTranslationsSpec extends SpecBase {

  val wordInputTopicName = "inputTopic"

  val translationInputTopicNameEn = "translationsEn"
  val translationInputTopicNameRu = "translationsRu"
  val translationOutputTopicName  = "translationOutputTopic"

  val translationBranchPrefix = "translationBranch-"
  val russianBranch           = "russian"
  val englishBranch           = "english"

  val cyrillicBlock: Character.UnicodeBlock = Character.UnicodeBlock.CYRILLIC

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit",
    "И теперь пошли русские слова"
  )

  val wordPattern: Pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val traslationsEn: Map[String, String] = Map(
    "hello" -> "hallo",
    "all"   -> "alle",
    // "streams" -> "",
    "lead" -> "führen",
    "to"   -> "zu",
    //"join"    -> "",
    "kafka"  -> "kafka",
    "summit" -> "summit",
  )

  val traslationsRu: Map[String, String] = Map(
    "и"       -> "und",
    "теперь"  -> "jetzt",
    "пошли"   -> "kommen",
    "русские" -> "russische",
    "слова"   -> "wörter"
  )

  val expectedTransaltions = Map(
    "all"     -> "alle",
    "streams" -> null,
    "summit"  -> "summit",
    "слова"   -> "wörter",
    "lead"    -> "führen",
    "русские" -> "russische",
    "пошли"   -> "kommen",
    "kafka"   -> "kafka",
    "и"       -> "und",
    "hello"   -> "hallo",
    "to"      -> "zu",
    "join"    -> null,
    "теперь"  -> "jetzt",
  )

  "must translate from english & russian to german" in {

    // --- topo

    val textLines: KStream[Int, String] = builder.stream(wordInputTopicName)

    val translationsEn: KTable[String, String] = builder.table(translationInputTopicNameEn)
    val translationsRu: KTable[String, String] = builder.table(translationInputTopicNameRu)

    val wordSplit: KStream[String, String] = textLines
      .flatMap { (_, v: String) =>
        val words = wordPattern.split(v.toLowerCase)
        List.from(words).map(w => (w, w))
      }

    // using .exists for now, for actual translations, might want to narrow to .all
    val branched: BranchedKStream[String, String] = wordSplit
      .split(Named.as(translationBranchPrefix))
      .branch(
        (_, v) => v.exists(c => Character.UnicodeBlock.of(c).equals(cyrillicBlock)),
        Branched.as(russianBranch)
      )
    val allBranched: Map[String, KStream[String, String]] =
      branched.defaultBranch(Branched.as(englishBranch))

    val joinedEn: KStream[String, String] =
      allBranched(translationBranchPrefix + englishBranch).leftJoin(translationsEn)((_, r) => r)
    val joinedRu: KStream[String, String] =
      allBranched(translationBranchPrefix + russianBranch).leftJoin(translationsRu)((_, r) => r)

    val merged: KStream[String, String] = joinedEn.merge(joinedRu)
    merged.to(translationOutputTopicName)

    // --- topo

    val topology: Topology = builder.build()
    info(topology.describe())

    val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

    val inputTopic: TestInputTopic[Integer, String] = topologyTestDriver.createInputTopic(
      wordInputTopicName,
      Serdes.Integer().serializer(),
      Serdes.String().serializer()
    )
    val translationInputTopicEn: TestInputTopic[String, String] =
      topologyTestDriver.createInputTopic(
        translationInputTopicNameEn,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )
    val translationInputTopicRu: TestInputTopic[String, String] =
      topologyTestDriver.createInputTopic(
        translationInputTopicNameRu,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )
    val translationOutputTopic: TestOutputTopic[String, String] =
      topologyTestDriver.createOutputTopic(
        translationOutputTopicName,
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
      )

    traslationsEn.foreach { case (k: String, v: String) => translationInputTopicEn.pipeInput(k, v) }
    traslationsRu.foreach { case (k: String, v: String) => translationInputTopicRu.pipeInput(k, v) }
    inputTopic.pipeValueList(inputValues.asJava)

    val outputRecords: mutable.Map[String, String] =
      translationOutputTopic.readKeyValuesToMap().asScala
    outputRecords must contain theSameElementsAs expectedTransaltions
  }

}
