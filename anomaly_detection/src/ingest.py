# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 18:08:23 2021

@author: Gebruiker
"""

  
import sys
import csv
import tqdm
import numpy as np
from datetime import datetime, timedelta
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer

if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0,11,5),
              value_serializer=lambda x: dumps(x).encode('utf-8'))

    n_cluster = 5
    n_measurements = 100000
    n_locations = 10

    start = 1246562100
    start_time = datetime.fromtimestamp(start)

    with tqdm.tqdm(total=n_measurements*n_locations) as pbar:
        for i in range(n_measurements): #number of measurements
            for j in range(n_locations): #locations
                pbar.update()

                time = start_time + timedelta(seconds=i)
                k = np.random.randint(0, n_cluster)
                record = dict(
                    timestamp=datetime.timestamp(time),
                    location_id=j,
                    measurement=np.random.normal(loc=k, scale=1/(k + 1e-4)))
                res = producer.send('test', value=record)
                sleep(0.05)