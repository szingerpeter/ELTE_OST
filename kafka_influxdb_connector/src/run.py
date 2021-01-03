from multiprocessing import Process
from connector import AnnomalyConnector, ClusteringConnector

if __name__ == "__main__":
    clustering = ClusteringConnector(kafka_topic='influxdb')
    clustering_process = Process(target=clustering.listen)
    clustering_process.start()

    annomaly = AnnomalyConnector(kafka_topic='anomaly')
    annomaly_process = Process(target=annomaly.listen)
    annomaly_process.start()
    

