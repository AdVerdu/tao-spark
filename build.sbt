organization := "org.corerda"
name := "TaoSpark"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.10"

val scalatestVersion = "3.2.10"
val scoptVersion = "4.1.0"
val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.corerda" %% "mapnet" % "0.1.1-SNAPSHOT",
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "com.github.scopt" %% "scopt" % scoptVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0"
)