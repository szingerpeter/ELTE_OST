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

val file = spark
  .readStream
  .schema(schema)
  .format("csv")
  .load("/opt/app/data/2018_electric_power_data/adapt/")
  //.withColumn("value", concat(col("timestamp"), lit((" "), col("location_id"), lit(" "), col("measurement")))

val out = file
  .withColumn("value", to_json(struct(file.columns.map(col(_)): _*)))
  .withColumn("key", col("location_id").cast(StringType))


out
  .writeStream
  .format("kafka")
  .option("checkpointLocation", "/tmp/")
  .option("kafka.bootstrap.servers", "kafka:9093")
  .option("topic", "test")
  .start()
  .awaitTermination()

