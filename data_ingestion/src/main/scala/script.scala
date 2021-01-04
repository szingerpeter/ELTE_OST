import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


val schema = new StructType()
  .add("timestamp", DoubleType)
  .add("location_id", IntegerType)
  .add("measurement", DoubleType)
  
val weather_schema = new StructType()
  .add("date", StringType)
  .add("max", DoubleType)
  .add("min", DoubleType)
  .add("location_id", IntegerType)

val file = spark
  .readStream
  .schema(schema)
  .format("csv")
  .load("/opt/app/data/2018_electric_power_data/adapt/")
  //.withColumn("value", concat(col("timestamp"), lit((" "), col("location_id"), lit(" "), col("measurement")))

val weather_file = spark
  .readStream.schema(weather_schema)
  .format("csv")
  .load("/opt/app/data/weather-data/")

val out = file
  .withColumn("value", to_json(struct(file.columns.map(col(_)): _*)))
  .withColumn("key", col("location_id").cast(StringType))

val weather_out = weather_file
  .withColumn("value", to_json(struct(weather_file.columns.map(col(_)): _*)))
  .withColumn("key", col("date").cast(StringType))

out
  .writeStream
  .format("kafka")
  .option("checkpointLocation", "/tmp/")
  .option("kafka.bootstrap.servers", "kafka:9093")
  .option("topic", "test")
  .start()
  //.awaitTermination()

weather_out
  .writeStream
  .format("kafka")
  .option("checkpointLocation", "/tmp/weather/")
  .option("kafka.bootstrap.servers", "kafka:9093")
  .option("topic", "weather")
  .start()
  .awaitTermination()

