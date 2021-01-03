# -*- coding: utf-8 -*-
"""
For Debugging Purposes
"""
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

bootstrap_servers_config = ['localhost:9092']
incoming_topic = "test"

    
if __name__ == "__main__":
                     
    consumer = KafkaConsumer(
         incoming_topic,
         bootstrap_servers = bootstrap_servers_config,
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')))
    
  
    for message in consumer:   
        print(message)
            
             




     

