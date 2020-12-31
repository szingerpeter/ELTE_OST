# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 15:13:54 2020

@author: Gebruiker
"""

from Spark_Utils import *
import glob
import csv
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField, StringType, FloatType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd
import sys


# Generate data
#dataset_path = 'C:/Users/Gebruiker/Documents/ost/project/data_ingestion/data/2018_electric_power_data/train/Xm1'




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(file=sys.stderr)
        sys.exit(-1)
    #Usage: python model_fitting.py <dataset path>

    dataset_path = sys.argv[1]
    data_path = glob.glob(dataset_path + '/*.csv')
    
    spark = SparkSession.builder.master("local") \
                    .appName('SparkTEST') \
                    .getOrCreate()
    
    #Fit model
    sample_df = create_sample_df_queque(data_path = data_path, spark_session = spark) [0]
    day_param, hour_param, std_param = create_simple_model(sample_df)
    #flag anomalies
    fitted_df = apply_simple_model(sample_df , day_param, hour_param, std_param)
    day_param.show()
    hour_param.show()
    
    
    
    
    
    