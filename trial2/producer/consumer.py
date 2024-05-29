from confluent_kafka import Consumer, KafkaException, KafkaError, admin
from confluent_kafka.admin import AdminClient
from pymongo import MongoClient
import sys
import json

kafka_bootstrap_servers = 'kafka:9092'
group_id = 'consumer-group'

def list_kafka_topics():
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    topics_metadata = admin_client.list_topics(timeout=10)
    topics = list(topics_metadata.topics.keys())
    return topics

def consume_and_store_all_topics():
    topics = list_kafka_topics()
    if not topics:
        print("No topics found to consume")
        return
    
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(topics)
    
    # Connect to MongoDB
    client = MongoClient('mongodb://mongo:27017')
    db = client.kafka_messages

    print("Consuming data from all Kafka topics and storing in MongoDB")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    continue

            # Get the topic name and store the message in the corresponding collection
            topic_name = msg.topic()
            collection = db[topic_name]
            collection.insert_one({'message': msg.value().decode('utf-8')})
            print(f"Stored message from topic '{topic_name}': {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    consume_and_store_all_topics()

