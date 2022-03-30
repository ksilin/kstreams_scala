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

  // prepare two translation tables
  // input stream of sentences in diff languages
  // split input into stream of words
  // branch input into two streams by unicode set
  // assumption is that one of them is russian and the other is english
  // left join russian words with russian translations & do same for english
  // merge translated words into single output

  // TODO - do we need to branch first, or can we join with all translations first and branch afterwards?

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

  val translationsEn: Map[String, String] = Map(
    "hello" -> "hallo",
    "all"   -> "alle",
    // "streams" -> "",
    "lead" -> "führen",
    "to"   -> "zu",
    //"join"    -> "",
    "kafka"  -> "kafka",
    "summit" -> "summit",
  )

  val translationsRu: Map[String, String] = Map(
    "и"       -> "und",
    "теперь"  -> "jetzt",
    "пошли"   -> "kommen",
    "русские" -> "russische",
    "слова"   -> "wörter"
  )

  val expectedTranslations = Map(
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

    // --- define topo

    val textLines: KStream[Int, String] = builder.stream(wordInputTopicName)

    val translationsEnTable: KTable[String, String] = builder.table(translationInputTopicNameEn)
    val translationsRuTable: KTable[String, String] = builder.table(translationInputTopicNameRu)

    val inputSplitByWord: KStream[String, String] = textLines
      .flatMap { (_, v: String) =>
        val words = wordPattern.split(v.toLowerCase)
        List.from(words).map(w => (w, w))
      }

    // using .exists for now, for actual translations, might want to narrow to .all
    val inputWordsBranchOffCyrillicUnicodeBlock: BranchedKStream[String, String] = inputSplitByWord
      .split(Named.as(translationBranchPrefix))
      .branch(
        (_, v) => v.exists(c => Character.UnicodeBlock.of(c).equals(cyrillicBlock)),
        Branched.as(russianBranch)
      )
    val inputWordsNameDefaultBranch: Map[String, KStream[String, String]] =
      inputWordsBranchOffCyrillicUnicodeBlock.defaultBranch(Branched.as(englishBranch))

    val joinedEn: KStream[String, String] =
      inputWordsNameDefaultBranch(translationBranchPrefix + englishBranch).leftJoin(translationsEnTable)((_, r) => r)
    val joinedRu: KStream[String, String] =
      inputWordsNameDefaultBranch(translationBranchPrefix + russianBranch).leftJoin(translationsRuTable)((_, r) => r)

    val merged: KStream[String, String] = joinedEn.merge(joinedRu)
    merged.to(translationOutputTopicName)

    // --- instantiate topo

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

    // insert test data
    translationsEn.foreach { case (k: String, v: String) => translationInputTopicEn.pipeInput(k, v) }
    translationsRu.foreach { case (k: String, v: String) => translationInputTopicRu.pipeInput(k, v) }
    inputTopic.pipeValueList(inputValues.asJava)

    val outputRecords: mutable.Map[String, String] =
      translationOutputTopic.readKeyValuesToMap().asScala
    outputRecords must contain theSameElementsAs expectedTranslations
  }

}
