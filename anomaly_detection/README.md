# Anomaly detection

Simple anomaly detection using spark dataframes/pyspark. 
detection_process.py reads from the kafka topic "test" and produces a new kafka topic. 

# To Do 

- Set up Docker Environment
- Connect to Docker-compose
- Maybe seperate settings in a config file

At the moment, the model is fitted and applied to small batches at the time. 
For better results, seperate the anomaly detection into:
 - 1. Model fitting in large batches on a greater amount of data, save parameters in DB or file
 - 2. Applying the model "live" on small batches, using the parameters from 1.)
 
 Eventually:
 - Add visualization



