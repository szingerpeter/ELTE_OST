# -*- coding: utf-8 -*-
"""
Consumes existing topic of measurements and creates a topic with 
added detected anomalies

For the implementation of the Anomaly detection, check Spark_Utils

"""
from kafka import KafkaConsumer, KafkaProducer
from Utils import *
from json import dumps, loads
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField, StringType, LongType, FloatType, DoubleType, TimestampType,IntegerType
import pyspark.sql.functions as F
import pandas as pd


batch_size = 20
bootstrap_servers_config = ['Kafka:9093']
incoming_topic = "test"
outgoing_topic = "anomaly"


def send_spark_df_to_kafka(df, producer, outgoing_topic):
    """
    More efficient would be to use the the connection to a Kafka topic described in 
    https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    (Did not work, this is a workaround using python-kafka)
    """
    records = df.collect()
    for record in records:
        message = record.asDict()
        res = producer.send(outgoing_topic, value=message)
    return res


    
if __name__ == "__main__":
    #Initialize spark session and KafkaConsumer&Producer
    spark = SparkSession.builder.master("local") \
                        .appName('SparkTEST2') \
                        .getOrCreate()
                     
    consumer = KafkaConsumer(
         incoming_topic,
         bootstrap_servers = bootstrap_servers_config,
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')),
         api_version=(0,11,5))
    
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers_config,
          api_version=(0,11,5),
          value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    counter = 0 
    
    #Load_parameters for model
    day_avg_param_df = spark.read.option("header",True).csv("src/resources/day_of_week_estimates.csv")
    hour_avg_df_param_df = spark.read.option("header",True).csv("src/resources/hour_of_day_estimates.csv")
    std_param = 150
    
  
    for message in consumer:   
        #cast message as pd dataframe
        message = message.value
        print(message)
        df = pd.DataFrame(data = message, index = [counter])
        #collect batch-messages temporarily in pandas df
        if counter == 0:
            batch_df = df          
        batch_df = batch_df.append(df)
        counter += 1
        
        #Every <batch-size> iteration: tansform to spark df and process batch
        if counter%batch_size == 0:
            #transform pandas df to spark df
            spark_batch_df = spark.createDataFrame(batch_df)
            #process batch...
            spark_batch_df = spark_batch_df.withColumn("measurements", F.col("measurement"))
            spark_batch_df = spark_batch_df.withColumn("timestamp", F.col("timestamp").cast("long"))
            #Fit estimation model to batch_df
            fitted_df = apply_simple_model(spark_batch_df, day_avg_param_df, hour_avg_df_param_df, std_param)
            #Send to Kafka topic 
            res = send_spark_df_to_kafka(fitted_df, producer, outgoing_topic)
            print("batch sent")
            #Reset pandas df to last row
            batch_df = df
            
             




     

