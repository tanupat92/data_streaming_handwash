#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import json
import pandas as pd 
def producer_f(filename, topic, broker_addr):
    # prep data
    data = pd.read_csv(filename)
    time_points = data.time.unique()
    
    # create producer
    producer = KafkaProducer(bootstrap_servers=broker_addr,api_version=(2,0,2),
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
                             
    delivered_records = 0
    old_time = time_points[0]-1
    for time in time_points:
        points = data.loc[data.time==time,:].reset_index()
        for i in range(points.shape[0]):
            delivered_records+=1
            value = {"time": str(time), "x":points.loc[i, "x"], "y":points.loc[i, "y"]}
            key = str(points.loc[i, "ID"])
            producer.send(topic, {key: value})
            print("\nProduced input tuple {}: {} {}".format(delivered_records-1, key, value))
            
        sleep(time-old_time)
        old_time= time
        if time==199:
            break
    producer.flush()
    print("\nDone with producing data to topic {}.".format(topic))

data_pipe = 'latent_positions_day_2.csv'
broker_addr = '10.0.0.50:29092'

producer_f(data_pipe, "movement_day2", broker_addr)

