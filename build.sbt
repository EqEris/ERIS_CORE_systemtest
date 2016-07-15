import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.docker.Cmd
//
// First enable the plugins we want for this project
enablePlugins(JavaAppPackaging)
enablePlugins(GitVersioning)

name := "ERIS_CORE_Systemtest"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("" +
  "-unchecked",
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8")

parallelExecution in Test := false

testOptions in Test += Tests.Argument("-oD")

resolvers ++= Seq(
  "Typesafe repository snapshots"    at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases"     at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
  "Twitter Repository"               at "http://maven.twttr.com",
  "Websudos releases"                at "https://dl.bintray.com/websudos/oss-releases/",
  "Sphonic releases"                 at "https://nexus.equeris.com/nexus/content/repositories/sphonic-releases-local/",
  "EqEris releases"                  at "https://nexus.equeris.com/nexus/content/repositories/eqeris-releases-local/",
  "EqEris snapshots"                 at "https://nexus.equeris.com/nexus/content/repositories/eqeris-snapshots-local/"
)

libraryDependencies ++= {
  val scalaTestVersion = "2.2.4"
  val restAssuredVersion = "2.4.1"
  val typesafeConfigVersion = "1.2.1"
  val scalaLoggingVersion = "3.1.0"
  val logbackVersion = "1.1.2"
  val argonautVersion = "6.1"
  val scalajHttpVersion = "1.1.3"
  val dataStaxCassandraVersion = "2.1.6"
  val JodaTimeVersion = "2.7"
  val seleniumJava = "2.35.0"
  val ghostDriver = "1.1.0"
  val DAGCommonsVersion = "7.1-SNAPSHOT"

  Seq(
    "com.eqeris" %%  "dag-commons" % DAGCommonsVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "com.jayway.restassured" % "rest-assured" % restAssuredVersion % "test",
    "com.typesafe" % "config" % typesafeConfigVersion % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion % "test",
    "ch.qos.logback" %  "logback-classic" % logbackVersion % "test",
    "io.argonaut" %% "argonaut" % argonautVersion % "test",
    "org.scalaj" %% "scalaj-http" % scalajHttpVersion % "test",
    "com.datastax.cassandra" % "cassandra-driver-core" % dataStaxCassandraVersion % "test",
    "joda-time"  %  "joda-time" % JodaTimeVersion % "test",
    "org.joda" % "joda-convert" % "1.2" % "test",
    "org.seleniumhq.selenium" % "selenium-java" % seleniumJava % "test",
    "com.github.detro.ghostdriver" % "phantomjsdriver" % ghostDriver % "test"
  )
}