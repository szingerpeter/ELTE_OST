#!/bin/bash

echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"

echo "waiting for cassandra service"
./wait-for-it.sh cassandra:9042 --timeout=100 -- echo "cassandra is up"

cd /opt/app/src/main/python

python server.py &
python rest-server.py &

cd /opt/app/
# sbt
echo "packaging scala source code"
sbt package
echo "running scala program"
sbt run
echo "exiting docker-entrypoint.sh"
