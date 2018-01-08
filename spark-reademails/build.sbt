name := "spark-reademails"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion  % Provided,
  "org.apache.commons" % "commons-email" % "1.5")

parallelExecution in Test := false
fork in Test := true
