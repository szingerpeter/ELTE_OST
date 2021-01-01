import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import com.datastax.spark.connector._

import java.sql.Timestamp


object App {

    val cassandraTriggerFrequency = "60 seconds"//"10 minutes"
    val watermarkLength = "60 seconds"//"10 minutes"
    val windowLength = "30 seconds"//"60 minutes"
    val windowSliding = "10 seconds"//"30 minutes"

    def main(args: Array[String]) {

        val conf = new SparkConf(true)
               //.set("spark.cassandra.connection.host", "cassandra")
               .set("spark.master", "local")

        val spark = SparkSession
               .builder
               .appName("Save measurements")
               .config(conf)
               .getOrCreate()

        import spark.implicits._

        val jsonSchema = StructType(Array(
                StructField("timestamp", DoubleType),
                StructField("location_id", IntegerType),
                StructField("measurement", DoubleType)
        ))

        //read stream
        val source = spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "kafka:9093")
                    .option("subscribe", "test")
                    .option("startingOffsets", "earliest")
                    .load()
                    .withWatermark("timestamp", watermarkLength)
                    .selectExpr("CAST(key as STRING)", "CAST(value as STRING)", "timestamp").as[(String, String, Timestamp)]
                    .withColumn("json", from_json(col("value"), jsonSchema))


        //save measurements
        val sink = source
            .writeStream
            .foreachBatch( (batchDf: DataFrame, batchId: Long) => {
                saveMeasurements(batchDf)
                saveStatistics(batchDf)})
            .trigger(Trigger.ProcessingTime(cassandraTriggerFrequency))
            .outputMode("append")
            .start()

        sink.awaitTermination()
    }

    def saveMeasurements(df: DataFrame): Unit = {
        df
            .selectExpr("json.timestamp", "json.location_id", "json.measurement as value")
            .filter(col("value").isNotNull)
            .write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "test")
            .option("table", "measurements")
            .mode("append")
            .save()
    }

    def saveStatistics(df: DataFrame): Unit = {
        df
            .selectExpr("from_unixtime(json.timestamp) as measurement_timestamp", "json.location_id", "json.measurement as value", "timestamp")
            .groupBy(
                window(col("timestamp"), windowLength, windowSliding),
                col("location_id")
            ).agg(
                max("measurement_timestamp") as "max_measurement_time",
                map(
                    lit("count"), count("*"),
                    lit("null_count"), sum(when(col("value").isNull, 1).otherwise(0)),
                    lit("zero_count"), sum(when(col("value") === 0, 1).otherwise(0)),
                    lit("min"), coalesce(min("value"), lit(0)),
                    lit("max"), coalesce(max("value"), lit(0)),
                    lit("mean"), coalesce(mean("value"), lit(0)),
                    lit("std"), coalesce(stddev_samp("value"), lit(0)),
                ) as "statistics"
            )
            .withColumn("location_id", when(col("location_id").isNull, -1).otherwise(col("location_id")))
            .withColumn("streaming_delay", unix_timestamp(col("window.end")) - unix_timestamp(col("max_measurement_time")))
            .write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "test")
            .option("table", "streaming_statistics")
            .mode("append")
            .save()
    }

}


