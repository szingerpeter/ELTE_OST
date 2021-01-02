#!/bin/bash

# Create default keyspace for single node cluster
if [[ ! -z "$CASSANDRA_KEYSPACE" ]]; then
  CQL="CREATE KEYSPACE IF NOT EXISTS $CASSANDRA_KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
  until echo $CQL | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later"
    sleep 5
  done &
fi

for fn in schema/*.cql; do
  until cat $fn | cqlsh; do
    sleep 10
  done &
  echo "executed ${fn}"
done

exec /docker-entrypoint.sh "$@"
