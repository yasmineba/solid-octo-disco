from confluent_kafka import Producer
import time
import json

p = Producer({'bootstrap.servers': 'kafka:9092'})

def delivery_report(err, msg):

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}'.format(msg.topic()))

while True:

    p.produce('mytopic', json.dumps({'key': 'some_key', 'value': 'hello kafka'}), callback=delivery_report)
    p.flush()
    time.sleep(3)

