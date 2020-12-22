# ELTE - OST, SM Term project
The term project of 2nd year EIT Data Science students (...) for the combined course project of Open-Source Technologies and Stream Mining course.

## Modules
- data ingestion: preprocessing, reading and streaming the source data

## How to run
`docker-compose up` will start all previously modules and their dependent services.

at the point of data ingestion:
You need to copy the original \.zip file to the folder 'data\_ingestion/data' and:

`docker-compose up` will start all services. data\_ingestion takes relatively long, so be patient. To check if Kafka receives any messages log into the Kafka service by `docker-compose exec kafka bash` and run `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning`
