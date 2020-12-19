# Data ingestion

The module is responsible to simulate data ingestion.

## Method

First, convert '\*.h5' files to '\*.csv' files. Then, we process the '\*.csv' files using spark-streaming.

## How to run

On your host machine in this directory, you need to have a /data/ folder with the original dataset (.zip). 

Build the image: `docker build -t test .`
Run the image: `docker run --mount src=$(pwd)/data/,target=/opt/app/mounted_data/,type=bind,readonly -it --name test test bash` and run `bash docker-entrypoint.sh` (wait until it finishes)
Enter the container: `docker exec -it test bash` and run `zookeeper-server-start.sh config/zookeeper.properties` (you started zookeeper, keep it running)
Enter the container (in a new terminal): `docker exec -it test bash` and run `kafka-server-start.sh config/server.properties` (you started the kafka server, keep it running)
Enter the container (in a new terminal): `docker exec -it test bash` and run `kafka-topics.sh --create --topic test --bootstrap-server localhost:9092` (you created a topic called 'test'), then run `kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092` (you started a kafka consumer listening to the 'test' topic)
Enter the container (in a new terminal): `docker exec -it test bash` and run `spark-shell --packages "org.apache.spark":"spark-sql-kafka-0-10_2.12":"3.0.1" -i src/main/scala/script.scala` (this will send data to 'test' kafka topic)

## TODO
- packaging the scala project and submitting via spark-submit did not work (fix it)
- make different services for zookeeper, kafka and data ingestion
