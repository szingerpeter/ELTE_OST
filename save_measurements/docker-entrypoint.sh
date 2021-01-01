#!/bin/bash

echo "packaging scala source code"
if ! [[ -d "target" ]]; then
  echo ""
  sbt package
fi

echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"

echo "waiting for cassandra service"
./wait-for-it.sh cassandra:9042 --timeout=100 -- echo "cassandra is up"

echo "start streaming"
spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1","com.datastax.spark:spark-cassandra-connector_2.12:3.0.0" --conf spark.cassandra.connection.host="cassandra" --class App target/scala-2.12/save-measurements_2.12-1.0.jar 

echo "keep running"
sleep infinity
