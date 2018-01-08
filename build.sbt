name := "cmwell-benchmark"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.2.5",
  "io.gatling" % "gatling-test-framework" % "2.2.5",
  "com.typesafe.akka" % "akka-http_2.11" % "10.0.11",
  "com.typesafe.akka" % "akka-stream-contrib_2.11" % "0.8",
  "io.spray" % "spray-json_2.11" % "1.3.4",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.scalatest" % "scalatest_2.11" % "3.0.1",
  "org.scalactic" % "scalactic_2.11" % "3.0.1")