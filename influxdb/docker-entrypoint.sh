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

echo "Creating user profile..."
yes | influx setup \
    --bucket ost_sm \
    --token eit \
    --org elte \
    --password adminadmin \
    --username admin \
    --retention 100000w

echo "Staying awake..."
sleep infinity