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
import pyspark.sql.functions as F
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

batch_size = 20
bootstrap_servers_config = ["kafka:9093"]
incoming_topic = "test"
outgoing_topic = "anomaly"

def create_point(payload):
    return Point('Result') \
        .tag('location-id', payload['location_id']) \
        .tag('outlier', payload['outlier']) \
        .field('measurements', float(payload['measurements'])) \
        .field('predictions', float(payload['predictions'])) \
        .field('residuals', float(payload['residuals'])) \
        .time(payload['TimestampType'])
    
def send_point(point, write_api):
    return write_api.write(bucket=influxdb_config['bucket'], org=influxdb_config['org'], record=point)


def send_spark_df_to_influx(df, write_api):
    """
    More efficient would be to use the the connection to a Kafka topic described in 
    https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    (Did not work, this is a workaround using python-kafka)
    """
    records = df.collect()
    for record in records:
        message = record.asDict()
        point = create_point(message)
        send_point(point, write_api)


    
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
    
    influxdb_config = dict(
    url='127.0.0.1:8086',
    token='eit',
    org='elte',
    bucket='ost_sm')


    influxdb_client = InfluxDBClient(
        url=influxdb_config['url'],
        token=influxdb_config['token'],
        org=influxdb_config['org'])
    
    write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
    query_api = influxdb_client.query_api()
    counter = 0 
    
    #Load_parameters for model
    day_avg_param_df = spark.read.option("header",True).csv("src/resources/day_of_week_estimates.csv")
    hour_avg_df_param_df = spark.read.option("header",True).csv("src/resources/hour_of_day_estimates.csv")
    std_param = 150
    
  
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
            #process batch...
            spark_batch_df = spark_batch_df.withColumn("measurements", F.col("measurement"))
            spark_batch_df = spark_batch_df.withColumn("timestamp", F.col("timestamp").cast("long"))
            #Fit estimation model to batch_df
            fitted_df = apply_simple_model(spark_batch_df, day_avg_param_df, hour_avg_df_param_df, std_param)
            #Send to Kafka topic 
            res = send_spark_df_to_influx(fitted_df, write_api)
            print("batch sent")
            #Reset pandas df to last row
            batch_df = df
            
             




     

