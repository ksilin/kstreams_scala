// *****************************************************************************
// Build settings
// *****************************************************************************

inThisBuild(
  Seq(
    organization := "example.com",
    organizationName := "ksilin",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("semver-spec"),
    startYear := Some(2021),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := "2.13.8",
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
      "jitpack" at "https://jitpack.io",
      Resolver.mavenLocal
    ),
    scalafmtOnCompile := true,
    dynverSeparator := "_", // the default `+` is not compatible with docker tags,
  )
)

enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging, JibPlugin)

jibBaseImage := "openjdk:11-jre"
// jibName := "jibApp"
jibTags := List("v" + version.value)
jibUseCurrentTimestamp := true

docker / dockerfile := {
  val appDir: File = stage.value
  val targetDir    = "/app"

  new Dockerfile {
    from("openjdk:11-jre")
    entryPoint(s"$targetDir/bin/${executableScriptName.value}")
    copy(appDir, targetDir, chown = "daemon:daemon")
  }
}

docker / imageNames := Seq(
  ImageName(
    namespace = Some("docker.io"), //organization.value),
    repository = "kostja/ksilin",  //name.value,
    tag = Some("v" + version.value)
  )
)

// we dont have no mappings
// Docker / mappings := mappings.value

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
        library.kstreamsAvroSerde,
        library.kstreamsScala,
        library.kstreamsTestUtils,
        library.kafkaAvroSerializer,
        library.circeKafka,
        library.kafkaStreamsCirce,
        library.gson,
        library.ksqlDbTestUtil,
        library.betterFiles,
        library.config,
        library.scopt,
        library.circeGeneric,
        library.airframeLog,
        library.logback,
        library.testcontainers,
        // library.log4j,
        // library.slfLog4j  % Test,
        library.kstreams % Test,
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
      val kafka             = "3.1.1"
      val confluent         = "7.1.2"
      val circeKafka        = "3.1.0"
      val circe             = "0.13.0"
      val kafkaStreamsCirce = "0.6.3"
      val gson = "2.9.0"
      val testContainers = "0.2.1"
      val betterFiles       = "3.9.1"
      val config            = "1.4.1"
      val scopt             = "4.0.1"
      val airframeLog       = "22.2.0"
      val logback           = "1.2.11"
      val scalatest         = "3.2.11"
      val log4j             = "1.2.17"
      val slfLog4j          = "1.7.30"
    }
    val clients             = "org.apache.kafka"      % "kafka-clients"            % Version.kafka
    val kstreams            = "org.apache.kafka"      % "kafka-streams"            % Version.kafka
    val kstreamsAvroSerde   = "io.confluent"          % "kafka-streams-avro-serde" % Version.confluent
    val kstreamsScala       = "org.apache.kafka"     %% "kafka-streams-scala"      % Version.kafka
    val kstreamsTestUtils   = "org.apache.kafka"      % "kafka-streams-test-utils" % Version.kafka
    val kafka               = "org.apache.kafka"     %% "kafka"                    % Version.kafka
    val kafkaAvroSerializer = "io.confluent"          % "kafka-avro-serializer"    % Version.confluent
    val circeKafka          = "com.nequissimus"      %% "circe-kafka"              % Version.circeKafka
    val circeGeneric        = "io.circe"             %% "circe-generic"            % Version.circe
    val gson  = "com.google.code.gson" % "gson" % Version.gson
    val kafkaStreamsCirce   = "com.goyeau"           %% "kafka-streams-circe"      % Version.kafkaStreamsCirce
    val ksqlDbTestUtil      = "io.confluent.ksql"     % "ksqldb-test-util"         % Version.confluent
    val betterFiles         = "com.github.pathikrit" %% "better-files"             % Version.betterFiles
    val config              = "com.typesafe"          % "config"                   % Version.config
    val scopt               = "com.github.scopt"     %% "scopt"                    % Version.scopt
    val airframeLog         = "org.wvlet.airframe"   %% "airframe-log"             % Version.airframeLog
    val logback             = "ch.qos.logback"        % "logback-classic"          % Version.logback
    val log4j               = "log4j"                 % "log4j"                    % Version.log4j
    val slfLog4j            = "org.slf4j"             % "slf4j-log4j12"            % Version.slfLog4j
    val testcontainers =
      "com.github.christophschubert" % "cp-testcontainers" % Version.testContainers
    val scalatest           = "org.scalatest"        %% "scalatest"                % Version.scalatest
  }
