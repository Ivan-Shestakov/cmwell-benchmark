name := "cmwell-benchmark"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-http_2.12" % "10.0.11",
  "com.typesafe.akka" % "akka-stream-contrib_2.12" % "0.8",
  "io.spray" % "spray-json_2.12" % "1.3.4",
  "org.scalatest" % "scalatest_2.12" % "3.0.1",
  "org.scalactic" % "scalactic_2.12" % "3.0.1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5")