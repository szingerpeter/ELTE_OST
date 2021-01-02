# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 15:11:44 2020

@author: Gebruiker
"""

import glob
import csv
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField, StringType, FloatType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd


def csv_to_json(csvFilePath):
    jsonArray = []
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        # convert each csv row into python dict
        for row in csvReader:
            jsonArray.append(row)
    return jsonArray

def create_sample_df_queque(data_path, spark_session):
    spark = spark_session
    schema = StructType([
      StructField("location_id", StringType(), True),
      StructField("measurements", FloatType(), True),
      StructField("timestamp", TimestampType(), True)
      ])
    
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    df = empty_df
    count = 0
    rdd_queque = []
    
    for index, path in enumerate(data_path):
        count+=1
        if count%20 == 0:
            rdd_queque.append(df)
            df = empty_df
        data = csv_to_json(path)
        new_df = spark.createDataFrame(data) 
        df = df.union(new_df)
    rdd_queque.append(df)
    return rdd_queque


def add_time_features(df):
    df = df.withColumn("TimestampType",F.to_timestamp(F.col('timestamp').cast('long')))
    df = df.withColumn("minute_of_day", F.hour(F.col("TimestampType"))*60 + (F.minute(F.col("TimestampType")) ))\
         .withColumn('yearday', F.dayofyear(F.col("TimestampType")))\
         .withColumn('hourofday', F.hour(F.col("TimestampType")))\
         .withColumn('dayofweek', F.dayofweek(F.col("TimestampType")))\
         .withColumn('WeekOfYear', F.weekofyear(F.col("TimestampType")))
    return df



def fit_simple_model(df):
    #Add columns
    df = add_time_features(df)
    #Detrending
    w = (Window.partitionBy("location_id").orderBy(F.col("yearday").cast("long")).rowsBetween(-20, 0))
    #w = (Window.partitionBy("location_id").orderBy(F.col("yearday").cast("long")).rangeBetween(-10, 0))
    df = df.withColumn("trend", F.avg("measurements").over(w))
    df = df.withColumn("measurements-trend", F.col("measurements") - F.col("trend"))
    #calculate_days
    w = (Window.partitionBy([F.col("location_id"), F.col("dayofweek")]))
    df = df.withColumn("day_average", F.avg("measurements-trend").over(w))
    #substract_that_
    df = df.withColumn("measurements-weekly", F.col("measurements-trend") - F.col("day_average"))
    #calculate_hours
    w = (Window.partitionBy([F.col("location_id"), F.col("hourofday")]))
    df = df.withColumn("hour_average", F.avg("measurements-weekly").over(w))
    #substract_that_
    df = df.withColumn("residuals", F.col("measurements-weekly") - F.col("hour_average"))
    return df

def create_hour_avg_df(df):
    new_df = df.select("location_id","hourofday","hour_average")
    new_df = new_df.dropDuplicates(["location_id","hourofday"])
    return new_df
    
def create_day_avg_df(df):
    new_df = df.select("location_id","dayofweek","day_average")
    new_df = new_df.dropDuplicates(["location_id","dayofweek"])
    return new_df

def create_simple_model(df):
    df = add_time_features(df)
    df = fit_simple_model(df)
    hour_avg_df = create_hour_avg_df(df)
    day_avg_df = create_day_avg_df(df)
    std = df.agg({'residuals': 'stddev'}).collect()[0][0]
    return  day_avg_df, hour_avg_df, std

def apply_outlier_criterion(df, cf=0.95,std=149):
    z_scores = {0.90:1.65,0.95: 1.96, 0.99:2.576 }
    critical_value = std*z_scores[cf]
    #will be replaces with something more sophisticated...
    df = df.withColumn("outlier", F.abs(F.col("residuals")) > critical_value) 
    return df


def apply_simple_model(df, day_df, hour_df,std=150):
    new_df = df.dropna()
    new_df = add_time_features(new_df)
    #Detrending
    w = (Window.partitionBy("location_id").orderBy(F.col("yearday").cast("long")).rowsBetween(-20, 0))
    #w = (Window.partitionBy("location_id").orderBy(F.col("yearday").cast("long")).rangeBetween(-10, 0))
    new_df = new_df.withColumn("trend", F.avg("measurements").over(w))
    #Add day avg
    new_df = new_df.join(day_df, ["location_id","dayofweek"], how='full')
    #Add day avg
    new_df = new_df.join(hour_df, ["location_id","hourofday"], how='full')
    new_df = new_df.na.fill(0)
    new_df = new_df.withColumn("predictions", F.col("trend") + F.col("day_average") + F.col("hour_average"))
    new_df = new_df.withColumn("residuals", F.col("measurements")-F.col("predictions")  )
    new_df = apply_outlier_criterion(new_df, std=std) 
    new_df = new_df.dropna()
    return new_df