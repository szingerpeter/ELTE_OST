import sys
from json import loads
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

kafka_consumer = KafkaConsumer(
    'influxdb',
    bootstrap_servers=['127.0.0.1:9092'],
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

influxdb_config = dict(
    url='127.0.0.1:8086',
    token='JjasaxuYksI5ozIgODwU4pZ3dDhes3u5pPEW0gM18E3fWbStwL7I4rG7eSA9APKNPdu56MblFOrCavLxIrB1Ag==',
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

for payload in kafka_consumer:
    for locations_info in payload.value:
        for measurement in locations_info:
            print(measurement)
            timestamp = measurement['timestamp']
            location_id = measurement['location_id']
            value = measurement['measurement']

            point = Point('Measurement') \
                .tag('location-id', location_id) \
                .field('value', value)

            res = write_api.write(bucket=influxdb_config['bucket'], org=influxdb_config['org'], record=point)
            print(measurement)