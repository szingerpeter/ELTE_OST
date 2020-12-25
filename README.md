## How to run

- Build the containers: `docker-compose -f docker-compose.yml up -d`
- Run a kafka producer to test flink package: `docker-compose exec kafka bash` and run `kafka-console-producer.sh --topic test --bootstrap-server kafka:9093`.
- Run the flink program: `docker-compose exec forecasting bash` and `sbt run -Dsbt.rootdir=true`.
- Now you can write kafka messages in the producer and will see them consumed by flink.
