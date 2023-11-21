import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json

consumer = KafkaConsumer(
    'demo_testing',
    bootstrap_servers=['13.211.152.170:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    api_version=(0,11,5)

)

for c in consumer:
    print(c.value)