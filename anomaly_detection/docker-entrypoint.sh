#!/bin/bash
#Should be in DOCKERFILE
echo "hello"
cd $SPARK_HOME/conf
cp log4j.properties.template log4j.properties
cd -

echo "start streaming"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 src/detection_process.py

