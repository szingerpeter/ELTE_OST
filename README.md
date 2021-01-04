# ELTE - OST, SM Term project
The term project of 2nd year EIT Data Science students (...) for the combined course project of Open-Source Technologies and Stream Mining course.

## Modules
- data ingestion: preprocessing, reading and streaming the source data

## How to run
`docker-compose up` will start all previously modules and their dependent services.

at the point of data ingestion:
You need to copy the original \.zip file to the folder 'data\_ingestion/data' and:

`docker-compose up` will start all services. data\_ingestion takes relatively long, so be patient. To check if Kafka receives any messages log into the Kafka service by `docker-compose exec kafka bash` and run `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning`
- Run the servers and flink program: `docker-compose exec forecasting bash`. If you want to run in the same command line, append '&' after calling the python servers. `cd /opt/app/src/main/python` `python server.py` `python rest-server.py` `cd /opt/app` `sbt run`. Leave console running and wait for flink to setup. When you see the SLF4J logs, it will be completed, and you should see a new connection printed in the console. This means that flink has been connected to the server.
- Test flink pipeline: run a kafka producer with `docker-compose exec kafka bash` and `kafka-console-producer.sh --topic test --bootstrap-server kafka:9093`. Write kafka messages in the producer and you should see them consumed by flink. It is important that the message has the correct format (e.g: {"timestamp":1.2835944E9,"location_id":0,"measurement":276.0400085449219})
- Test rest server: make a curl request with timestamp and location_id as parameters: `curl "localhost:5000/forecast?timestamp=1&location_id=1"`. Output should be a JSON with the previous parameters and the predicted value of the model.
