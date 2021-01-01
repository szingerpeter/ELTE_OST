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

import spark.implicits._

val cassandraTriggerFrequency = "10 seconds"//"10 minutes"
val watermarkLength = "10 seconds"//"10 minutes"
val windowLength = "30 seconds"//"60 minutes"
val windowSliding = "10 seconds"//"30 minutes"

val jsonSchema = StructType(Array(
    StructField("timestamp", DoubleType),
    StructField("location_id", IntegerType),
    StructField("measurement", DoubleType)
))

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

//read stream
val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9093")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .withWatermark("timestamp", watermarkLength)
    .selectExpr("CAST(key as STRING)", "CAST(value as STRING)", "timestamp").as[(String, String, Timestamp)]
    .withColumn("json", from_json(col("value"), jsonSchema))


//process stream
val measurements = df
    .writeStream
    .foreachBatch( (batchDf: DataFrame, batchId: Long) => {
        saveMeasurements(batchDf)})
    .trigger(Trigger.ProcessingTime(cassandraTriggerFrequency))
    .outputMode("append")
    .start()

measurements.awaitTermination()


