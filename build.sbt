name := "priority-amqp-test"

version := "0.1"

scalaVersion := "2.12.8"

val buckyVersion = "1.3.2"

libraryDependencies ++= Seq(
  "com.itv" %% "bucky-core" % buckyVersion,
  "com.itv" %% "bucky-rabbitmq" % buckyVersion,
  "org.scalatest" %% "scalatest" % "3.0.7" % Test

)