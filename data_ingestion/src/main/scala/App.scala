import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.streaming.kafka._


object App {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Data ingestion").getOrCreate()

        val schema = new StructType().add("timestamp", DoubleType).add("location_id", IntegerType).add("measurement", DoubleType)

        val file = spark.readStream.schema(schema).format("csv").load("/opt/app/data/2018_electric_power_data/adapt/").withColumn("value", concat(col("timestamp"), lit(" "), col("location_id"), lit(" "), col("measurement")))

        file.writeStream.format("console").start()//.awaitTermination(10000)

/*        file
            .writeStream
            .format("kafka")
            .option("checkpointLocation", "/tmp/")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "test")
            .start()
            .awaitTermination(10000)
*/
        spark.stop()
    }
}

/*
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

spark = 
SparkSession.builder.appName("CrossCorrelation").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 1)


val measurements = ssc.textFileStream("file:///opt/app/data/2018_electric_power_data/adapt/")
*/

/*
sbt package
(cd src/ && exec sbt package)
spark-submit --class App --master local[1] src/target/scala-2.12/data-ingestion_2.12-1.0.jar

spark-submit --class App --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 target/scala-2.12/data-ingestion_2.12-1.0.jar 
*/


//file.writeStream.format("console").start()//.awaitTerminationOrTimeout(1000)
//.awaitTerminationOrTimeout(10000)
//file.writeStream.format("csv").option("checkpointLocation", "/opt/").option("path", "/opt/app/").start()

/*

sudo docker run --mount src=$(pwd)/data/,target=/opt/app/mounted_data/,type=bind,readonly -d --name test test

sudo docker exec -it test bash

zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
kafka-topics.sh --describe --topic test --bootstrap-server localhost:9092
kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
*/




