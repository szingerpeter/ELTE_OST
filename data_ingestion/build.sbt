name := "Data ingestion"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.12" % "3.0.1",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.0.1",
    "org.apache.spark" % "spark-sql_2.12" % "3.0.1",
    "org.apache.spark" % "spark-streaming_2.12" % "3.0.1"
)

excludeFilter in Compile := "script.scala"
