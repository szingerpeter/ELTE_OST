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
bootstrap_servers_config = ['localhost:9092']
incoming_topic = "test"
outgoing_topic = "anomaly"


def send_spark_df_to_kafka(df, producer, outgoing_topic):
    """
    input: spark-df, instance of KafkaProducer
    Writes spark df to kafka topic.
    
    There is a spark internal function for that, but I struggled making that 
    work, so this is a solution using the KafkaProducer from kafka-python

    """
    records = df.collect()
    for record in records:
        message = dict( timestamp=record["timestamp"],
                    location_id=record["location_id"],
                    measurement=record["measurement"],
                    anomaly=record["outlier"])
        res = producer.send(outgoing_topic, value=message)
    return res 

def process_pyspark_batch_df(spark_batch_df):
    """
    Adds time-features and detected anomalies to the dataframe

    """    
    #Remove this later 
    spark_batch_df = spark_batch_df.withColumn("measurements", F.col("measurement"))
    spark_batch_df = spark_batch_df.withColumn("timestamp", F.col("timestamp").cast("long"))
    #The parameters are fitted on the batch atm todo: load parameters 
    day_param, hour_param, std_param = create_simple_model(spark_batch_df)
    #flag anomalies for batch 
    fitted_df = apply_simple_model(spark_batch_df, day_param, hour_param, std_param)
    return fitted_df
        
 
    
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
         value_deserializer=lambda x: loads(x.decode('utf-8')))
    
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers_config,
          api_version=(0,11,5),
          value_serializer=lambda x: dumps(x).encode('utf-8'))
       
    counter = 0 
  
    for message in consumer:   
        #cast message as pd dataframe
        message = message.value
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
            #process batch
            fitted_df = process_pyspark_batch_df(spark_batch_df)
            #Send to Kafka topic 
            res = send_spark_df_to_kafka(fitted_df, producer, outgoing_topic)
            print("batch sent")
            #Reset pandas df to last row
            batch_df = df
            
             




     

