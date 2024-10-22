name := "forecasting"

version := "1.0"
scalaVersion := "2.12.2"

libraryDependencies ++= Seq("org.apache.flink" %% "flink-scala" % "1.12.0",
  "org.apache.flink" %% "flink-clients" % "1.12.0",
  "org.apache.flink" %% "flink-streaming-scala" % "1.12.0",
  "org.apache.flink" %% "flink-connector-kafka" % "1.12.0",
  "com.typesafe.akka" %% "akka-http-core" % "10.2.2")
