from confluent_kafka import Consumer, KafkaError

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'display-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['region1'])

# Streaming loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print("Received message: {}".format(msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
