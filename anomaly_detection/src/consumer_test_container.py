# -*- coding: utf-8 -*-
"""
For Debugging Purposes
"""
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

bootstrap_servers_config = ['kafka:9093']
incoming_topic = "test"

    
if __name__ == "__main__":
                     
    consumer = KafkaConsumer(
         incoming_topic,
         bootstrap_servers = bootstrap_servers_config,
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')),
         api_version=(0,11,5))

    count = 0
    for message in consumer:
        if count < 10:
            print(message)
            count += 1
        else:
            exit()
