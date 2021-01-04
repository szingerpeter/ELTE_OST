#!/bin/bash

echo "packaging scala source code"
if ! [[ -d "target" ]]; then
  sbt package
fi

echo "waiting for cassandra service"
./wait-for-it.sh cassandra:9042 --timeout=100 -- echo "cassandra is up"

echo "waiting some time"
sleep 100

echo "start batch processing"
spark-submit --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0" --conf spark.cassandra.connection.host="cassandra" --class App target/scala-2.12/batch-processing_2.12-1.0.jar 


