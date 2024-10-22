from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class Connector:

    def __init__(self, kafka_topic = 'influxdb', bucket='ost_sm'):

        self._kafka_consumer = KafkaConsumer(
            'kafka_topic',
            bootstrap_servers=['kafka:9093'],
            enable_auto_commit=True,
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

        self._influxdb_config = dict(
            url='127.0.0.1:8086',
            token='eit',
            org='elte',
            bucket=bucket
        )

        self._influxdb_client = InfluxDBClient(
            url=self._influxdb_config['url'],
            token=self._influxdb_config['token'],
            org=self._influxdb_config['org']
        )

        self._write_api = self._influxdb_client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._influxdb_client.query_api()

    def _process(self, payload=dict()):
        """Abstract function
        Input:
        payload: json object coming from kafka

        Example:
        Point('Measurement') \
            .tag('location-id', j) \
            .tag('cluster_id', k) \
            .field('measurement', np.random.normal(loc=k, scale=k)) \
            .time(time)
        """
        pass

    def write_db(self, point):
        self._write_api.write(
            bucket=self._influxdb_config['bucket'], 
            org=self._influxdb_config['org'], 
            record=point
        )

    def listen(self):
        for payload in self._kafka_consumer:
            self._process(payload.value)

class AnnomalyConnector(Connector):

    def __init__(self, kafka_topic='anomaly_influxdb', bucket='ost_sm_annomaly'):
        super(AnnomalyConnector, self).__init__(kafka_topic=kafka_topic, bucket=bucket)

    def _process(self, payload):
        self.write_db(
            Point('Result') \
                .tag('location-id', payload['location_id']) \
                .tag('outlier', payload['outliers']) \
                .field('measurements', payload['measurements']) \
                .field('predictions', payload['predictions']) \
                .field('residuals', payload['residuals']) \
                .field('value', payload['hour_average']) \
                .field('value', payload['day_average']) \
                .time(payload['timestamp'])
        )

class ClusteringConnector(Connector):

    def __init__(self, kafka_topic='influxdb', bucket='ost_sm'):
        super(ClusteringConnector, self).__init__(kafka_topic=kafka_topic, bucket=bucket)

    def _process(self, payload):
        for measurement in payload:
            self.write_db(
                Point('Clustering') \
                    .tag('location-id', measurement['location_id']) \
                    .tag('cluster_id', measurement['cluster_id']) \
                    .field('measurement', measurement['measurement']) \
                    .field('cluster_id', measurement['cluster_id']) \
                    .time(measurement['timestamp'])
            )