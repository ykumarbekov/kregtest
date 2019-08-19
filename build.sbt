import sbt.Keys._


name := "ksregtest"

version := "0.1"

scalaVersion := "2.11.8"

val kafkaVersion = "2.3.0"
val scalaLoggingVersion = "3.7.2"
val logbackVersion = "1.2.3"
val postgreSQLVersion = "42.2.5"
val jsonVersion = "20180130"
val okhttpVersion = "4.0.0"
val json2avroVersion = "0.2.5"
val avroSerializerVersion = "5.2.1"
val scalatestVersion = "3.0.8"
val scalaloggingVersion = "3.9.2"

mainClass in Compile := Some("MainApp")
mainClass in (Compile, packageBin) := Some("MainApp")

mainClass in assembly := Some("MainApp")
assemblyJarName in assembly := "ksregtest.jar"

resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "org.postgresql" % "postgresql" % postgreSQLVersion,
  "org.json" % "json" % jsonVersion,
  "com.squareup.okhttp3" % "okhttp" % okhttpVersion,
  "tech.allegro.schema.json2avro" % "converter" % json2avroVersion,
  "io.confluent" % "kafka-avro-serializer" % avroSerializerVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion
)



