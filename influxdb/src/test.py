import sys
import csv
import tqdm
import numpy as np
from datetime import datetime, timedelta
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

influxdb_config = dict(
    url='127.0.0.1:8086',
    token='eit',
    org='elte',
    bucket='ost_sm'
)

influxdb_client = InfluxDBClient(
    url=influxdb_config['url'],
    token=influxdb_config['token'],
    org=influxdb_config['org']
)
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
query_api = influxdb_client.query_api()

if __name__ == "__main__":

    n_cluster = 5
    n_measurements = 20
    n_locations = 10

    start = 1246562100
    start_time = datetime.fromtimestamp(start)

    with tqdm.tqdm(total=n_measurements*n_locations) as pbar:
        for i in range(n_measurements): #number of measurements
            for j in range(n_locations): #locations
                pbar.update()

                time = start_time + timedelta(hours=5*i)
                k = np.random.randint(0, n_cluster)

                point = Point('Measurement') \
                    .tag('location-id', j) \
                    .field('measurement', np.random.normal(loc=k, scale=1/(k + 1e-4))) \
                    .time(time)

                res = write_api.write(bucket=influxdb_config['bucket'], org=influxdb_config['org'], record=point)
                sleep(0.001)