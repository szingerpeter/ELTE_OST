import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming._

object App {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Data ingestion").getOrCreate()

        val schema = new StructType().add("timestamp", DoubleType).add("location_id", IntegerType).add("measurement", DoubleType)

        val file = spark.readStream.schema(schema).format("csv").load("test").select(concat($"timestamp", lit(" "), $"location_id", lit(" "), $"measurement") as "x", lit("") as "value")

        file
            .writeStream
            .format("kafka")
            .option("checkpointLocation", "/tmp/")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "test")
            .start()
    }
}


//file.writeStream.format("console").start()//.awaitTerminationOrTimeout(1000)
//.awaitTerminationOrTimeout(10000)
//file.writeStream.format("csv").option("checkpointLocation", "/opt/").option("path", "/opt/app/").start()

/*
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
kafka-topics.sh --describe --topic test --bootstrap-server localhost:9092
kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
*/




