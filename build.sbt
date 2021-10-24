// *****************************************************************************
// Build settings
// *****************************************************************************

inThisBuild(
  Seq(
    organization := "example.com",
    organizationName := "ksilin",
    startYear := Some(2021),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := "2.13.4",
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-Ywarn-unused:imports",
      //"-Xfatal-warnings",
    ),
    resolvers ++= Seq(
      "confluent" at "https://packages.confluent.io/maven",
      "ksqlDb" at "https://ksqldb-maven.s3.amazonaws.com/maven",
      "confluentJenkins" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200715032424/1/maven/",
      "confluentJenkins2" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200916191548/1/maven/",
      Resolver.sonatypeRepo("releases"),
      Resolver.bintrayRepo("wolfendale", "maven"),
      Resolver.bintrayRepo("ovotech", "maven"),
      "mulesoft" at "https://repository.mulesoft.org/nexus/content/repositories/public/",
      Resolver.mavenLocal
    ),
    scalafmtOnCompile := true,
    dynverSeparator := "_", // the default `+` is not compatible with docker tags
  )
)

// *****************************************************************************
// Projects
// *****************************************************************************

lazy val kstreams_scala =
  project
    .in(file("."))
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
        library.kafka,
        library.kstreams,
        library.kstreamsScala,
        library.kstreamsTestUtils,
        library.kafkaAvroSerializer,
        library.circeKafka,
        library.kafkaStreamsCirce,
        library.betterFiles,
        library.config,
        library.scopt,
        library.circeGeneric,
        library.airframeLog,
        library.logback,
        // library.log4j,
        // library.slfLog4j  % Test,
        library.scalatest % Test
      ),
    )

// *****************************************************************************
// Project settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    // Also (automatically) format build definition together with sources
    Compile / scalafmt := {
      val _ = (Compile / scalafmtSbt).value
      (Compile / scalafmt).value
    },
  )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val kafka             = "2.8.0"
      val confluent         = "6.2.1"
      val circeKafka        = "2.7.0"
      val circe             = "0.13.0"
      val kafkaStreamsCirce = "0.6.3"
      val betterFiles       = "3.9.1"
      val config            = "1.4.1"
      val scopt             = "4.0.1"
      val airframeLog       = "20.12.1"
      val logback           = "1.2.3"
      val scalatest         = "3.2.0"
      val log4j             = "1.2.17"
      val slfLog4j          = "1.7.30"
    }
    val clients             = "org.apache.kafka"      % "kafka-clients"            % Version.kafka
    val kstreams            = "org.apache.kafka"      % "kafka-streams"            % Version.kafka
    val kstreamsScala       = "org.apache.kafka"     %% "kafka-streams-scala"      % Version.kafka
    val kstreamsTestUtils   = "org.apache.kafka"      % "kafka-streams-test-utils" % Version.kafka
    val kafka               = "org.apache.kafka"     %% "kafka"                    % Version.kafka
    val kafkaAvroSerializer = "io.confluent"          % "kafka-avro-serializer"    % Version.confluent
    val circeKafka          = "com.nequissimus"      %% "circe-kafka"              % Version.circeKafka
    val circeGeneric        = "io.circe"             %% "circe-generic"            % Version.circe
    val kafkaStreamsCirce   = "com.goyeau"           %% "kafka-streams-circe"      % Version.kafkaStreamsCirce
    val betterFiles         = "com.github.pathikrit" %% "better-files"             % Version.betterFiles
    val config              = "com.typesafe"          % "config"                   % Version.config
    val scopt               = "com.github.scopt"     %% "scopt"                    % Version.scopt
    val airframeLog         = "org.wvlet.airframe"   %% "airframe-log"             % Version.airframeLog
    val logback             = "ch.qos.logback"        % "logback-classic"          % Version.logback
    val log4j               = "log4j"                 % "log4j"                    % Version.log4j
    val slfLog4j            = "org.slf4j"             % "slf4j-log4j12"            % Version.slfLog4j
    val scalatest           = "org.scalatest"        %% "scalatest"                % Version.scalatest
  }
