# -*- coding: utf-8 -*-
"""
Estimates model parameters
ATM based on sample data from CSVs, try to connect to DB

"""

from Utils import *
import pandas as pd
import glob

dataset_path = "C:/Users/Gebruiker/Documents/ost/project/data_ingestion/data/2018_electric_power_data_csv/train/Xm1"
data_path = glob.glob(dataset_path + '/*.csv')

if __name__ == "__main__":
    #Initialize spark session and KafkaConsumer&Producer
    spark = SparkSession.builder.master("local") \
                        .appName('SparkTEST2') \
                        .getOrCreate()
                        
    #Should be from DB in the future
    measurement_df = create_spark_df_from_csv(data_path, spark)
    #Fitting Model Parameters
    day_avg_df, hour_avg_df, std =  create_simple_model(measurement_df)
    #Save, should be connected to DB in the future
    hour_avg_df.toPandas().to_csv("hour_of_day_estimates.csv", index=False)
    day_avg_df.toPandas().to_csv("day_of_week_estimates.csv", index=False)




