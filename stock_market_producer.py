import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json

producer = KafkaProducer(bootstrap_servers=['13.211.152.170:9092'], #change ip here
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8')
                         ,api_version=(0,10,2)
                        )
producer.send(topic='demo_testing', value={'allen1': 'chen1'})
# producer.flush()