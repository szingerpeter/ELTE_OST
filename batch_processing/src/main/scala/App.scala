import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.datastax.spark.connector._

import java.sql.Timestamp

object App {

    def main(args: Array[String]) {

        val conf = new SparkConf(true)
               //.set("spark.cassandra.connection.host", "cassandra")
               .set("spark.master", "local")

        val spark = SparkSession
               .builder
               .appName("Save measurements")
               .config(conf)
               .getOrCreate()
        
        val df = spark
                .read
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test").option("table", "measurements")
                .load

        val daily_statistics = df
                .groupBy(
                    col("location_id"), 
                    col("timestamp").cast("Date") as "date")
                .agg(
                    count("*") as "count", 
                    min("value") as "min_value", 
                    max("value") as "max_value", 
                    mean("value") as "mean_value", 
                    stddev("value") as "std_value", 
                    sum(when(col("value").isNull, 1).otherwise(0)) as "null_count_value")

        daily_statistics
                .write
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test")
                .option("table", "daily_statistics")
                .mode("append")
                .save
    }
}
