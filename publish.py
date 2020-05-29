from datetime import datetime
from google.cloud import pubsub_v1
import Rubik as r
import sys
import time 
import random

project_id = 'CLOUDPROJECT'
topic_name = 'PUBSUBTOPIC'

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_name)

def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
        except:
            print('Handle {} for {}'.format(f.exception(), data))

    return callback

def publish(data):
    future = publisher.publish(topic_path, data = data.encode('utf-8'))

    future.add_done_callback(get_callback(future, data))

def generate_data():
    return str((str(r.generate_cube()), str(datetime.now())))

if __name__ == '__main__':

    while(True):
        publish(generate_data())
        time.sleep(1)
