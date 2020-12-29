import glob
import csv
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField, StringType, FloatType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder.master("local") \
                    .appName('SparkTEST') \
                    .getOrCreate()
    
# Generate data
dataset_path = 'C:/Users/Gebruiker/Documents/ost/project/data_ingestion/data/2018_electric_power_data/train/Xm1'
data_path = glob.glob(dataset_path + '/*.csv')


def csv_to_json(csvFilePath):
    jsonArray = []
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        # convert each csv row into python dict
        for row in csvReader:
            jsonArray.append(row)
    return jsonArray

def create_sample_df_queque(data_path):
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
        if count%50 == 0:
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
         .withColumn('dayofweek', F.dayofweek(F.col("TimestampType")))\
         .withColumn('WeekOfYear', F.weekofyear(F.col("TimestampType")))
    return df


def decompose_ts(df):
    #Compute_20_day_trend
    w = (Window.partitionBy("location_id").orderBy(F.col("yearday").cast("long")).rangeBetween(-30, 0))
    df = df.withColumn("trend", F.avg("measurements").over(w))
    df = df.withColumn("measurement-trend", F.col("measurements") - F.col("trend"))
    #Compute weekly cycle
    w = (Window.partitionBy("location_id").orderBy(F.col("dayofweek").cast("long")).rangeBetween(-1, 0))
    df = df.withColumn("week_cycle", F.avg("measurement-trend").over(w))
    df = df.withColumn("measurement-week", F.col("measurement-trend") - F.col("week_cycle"))
    #Compute daily cycle, rolling average over the last 3h
    w = (Window.partitionBy("location_id").orderBy(F.col("minute_of_day").cast("long")).rangeBetween(-210, 0))
    df = df.withColumn("day_cycle", F.avg("measurement-week").over(w))
    #Residuals     of the day to analyze
    df = df.withColumn("residuals_day", F.col("measurement-week") - F.col("day_cycle")) 
    #Smaller changes 
    w = (Window.partitionBy("location_id").orderBy(F.col("minute_of_day").cast("long")).rangeBetween(-30, 0))
    df = df.withColumn("moving_avg_30min", F.avg("residuals_day").over(w))
    #Residuals to analyze
    df = df.withColumn("residuals", F.col("residuals_day") - F.col("moving_avg_30min")) 
    return df

def apply_outlier_criterion(df):
    #will be replaces with something more sophisticated...
    df = df.withColumn("outlier", F.abs(F.col("residuals")) > 200) 
    return df




if __name__ == "__main__":
    
    sample_dfs = create_sample_df_queque(data_path = data_path )
    sample_dfs[0].show()
    processed_dfs = [add_time_features(df) for df in sample_dfs]
    decomposed_tss = [decompose_ts(df) for df in processed_dfs]
    tss_with_outliers = [apply_outlier_criterion(df) for df in decomposed_tss]
    for df in tss_with_outliers:
        df.show()
    
spark.stop()





