#!/bin/bash

echo "Starting the influxdb server..."
influxd --reporting-disabled &

while true; do
    IS_ALIVE=$(curl --write-out '%{http_code}' --silent --output /dev/null localhost:8086/ping)
    if [ ${IS_ALIVE} -eq 204 ]; then
        break
    fi
    sleep 1
done

echo "Cleaning buckets from previous initialization..."
influx bucket delete -n clustering -o elte --token eit
influx bucket create -n clustering -o elte --token eit

influx bucket delete -n annomaly -o elte --token eit
influx bucket create -n annomaly -o elte --token eit

echo "Creating user profile..."
yes | influx setup \
    --bucket clustering \
    --token eit \
    --org elte \
    --password adminadmin \
    --username admin \
    --retention 100000w

echo "Staying awake..."
sleep infinity