from kafka import KafkaProducer
import json
import time

def connect_to_kafka():
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka. Retrying in 5 seconds... ({retries}/{max_retries})")
            retries += 1
            time.sleep(5)
    raise ConnectionError("Failed to connect to Kafka after multiple retries.")

producer = connect_to_kafka()

def send_message(topic, message):
    producer.send(topic, message)
    producer.flush()

# Example usage
send_message('trino-topic', {'Name': 'Alex'})

