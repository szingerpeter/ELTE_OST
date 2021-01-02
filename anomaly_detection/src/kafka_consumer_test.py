# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 18:39:26 2021

@author: Gebruiker
"""
from kafka import KafkaConsumer
from Spark_Utils import *
from json import loads
import glob
import csv
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField, StringType, LongType, FloatType, DoubleType, TimestampType,IntegerType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd
from pyspark import SparkContext, SparkConf

def create_empty_df(spark_session):
    spark = spark_session
    schema = StructType([
      StructField("location_id", IntegerType(), True),
      StructField("measurement", FloatType(), True),
      StructField("timestamp", TimestampType(), True)
      ])
    
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    return empty_df

def add_to_df(df, data, spark):
     sc = spark.sparkContext
     rdd_data = sc.parallelize(data)
     new_df = spark.createDataFrame(rdd_data, schema) 
     df = df.union(new_df)
     return df
 
    
if __name__ == "__main__":

    spark = SparkSession.builder.master("local") \
                        .appName('SparkTEST2') \
                        .getOrCreate()
            
    sc = spark.sparkContext
    
    consumer = KafkaConsumer(
        'test',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')))
    
     
    counter = 0 
    
    for message in consumer:   
        #first creates a pandas df
        counter += 1
        message = message.value
        print(message)
        df = pd.DataFrame(data = message, index = [counter])
        if counter == 1:
            batch_df = df
        batch_df = batch_df.append(df)
        
        if counter%20 == 0:
            sdf = spark.createDataFrame(sum_df)
            #Remove this later 
            sdf = sdf.withColumn("measurements", F.col("measurement"))
            sdf = sdf.withColumn("timestamp", F.col("timestamp").cast("long"))
            fitted_sdf = fit_simple_model(sdf)
            break  
 


#nsdf = sdf.withColumn("timestamp", F.col("timestamp").cast("long"))



     

