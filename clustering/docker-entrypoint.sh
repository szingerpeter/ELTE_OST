echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"

flink run -d /opt/clustering/project.jar