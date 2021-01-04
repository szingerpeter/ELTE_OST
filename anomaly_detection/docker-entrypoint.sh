#!/bin/bash
#Should be in DOCKERFILE
echo "hello"
cd $SPARK_HOME/conf
cp log4j.properties.template log4j.properties
cd -


#!/bin/bash
echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"


echo "start anomaly detection service"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 src/detection_process.py

