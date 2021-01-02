import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.streaming.kafka._


object App {
    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("Data ingestion")
          .config("spark.master", "local")
          .getOrCreate()

        val schema = new StructType()
          .add("timestamp", DoubleType)
          .add("location_id", IntegerType)
          .add("measurement", DoubleType)

        val file = spark
          .readStream.schema(schema)
          .format("csv")
          .load("/opt/app/data/2018_electric_power_data/adapt/")
          .withColumn("value", concat(col("timestamp"), lit(" "), col("location_id"), lit(" "), col("measurement")))

        //file.writeStream.format("console").start().awaitTermination(10000)

        file
            .writeStream
            .format("kafka")
            .option("checkpointLocation", "/tmp/")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "test")
            .start()
            .awaitTermination()
    }
}
