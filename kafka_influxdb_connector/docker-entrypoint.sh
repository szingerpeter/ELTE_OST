#!/bin/bash

echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"

echo "Listenning to kafka messages"
python3 ./src/run.py

