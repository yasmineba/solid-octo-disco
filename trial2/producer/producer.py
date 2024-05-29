import os
import json
import sys
import requests
from pymongo import MongoClient
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaError, Producer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client['kafka_messages']


def create_kafka_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})

    topic_list = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
    fs = admin_client.create_topics(topic_list)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")


def list_kafka_topics():
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    topics_metadata = admin_client.list_topics(timeout=10)
    topics = [topic for topic in topics_metadata.topics if not topic.startswith("__")]  # Filter out internal topics
    return topics


def produce_data(table_name, kafka_topic):
    create_kafka_topic(kafka_topic)

    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
    catalog, schema, tab_name = table_name.split('.')
    headers = {
        'X-Trino-User': 'user',
        'X-Trino-Catalog': catalog,
        'X-Trino-Schema': schema
    }

    query = f"SELECT * FROM {tab_name}"

    response = requests.post(f'http://trino:8080/v1/statement', data=query, headers=headers)

    response_json = response.json()

    while 'nextUri' in response.text:
        next_uri = response_json['nextUri']
        response = requests.get(next_uri)
        if response.status_code == 200:
            response_json = response.json()
        else:
            break

    logger.info(response_json)

    jsn = json.dumps(response_json)
    producer.produce(kafka_topic, jsn.encode('utf-8'))
    producer.flush()


def consume_data(kafka_topic):
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([kafka_topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.debug("No message received in the current poll interval.")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    logger.info("End of partition reached {0}/{1}".format(msg.topic(), msg.partition()))
                    continue
                else:
                    logger.error("Consumer error: {}".format(msg.error()))
                    break
            else:
                # Parse the received message assuming it's in JSON format
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f'Received message: {message_data}')

                    # Insert the message into MongoDB
                    collection = mongo_db[kafka_topic]  # Use the Kafka topic name as the collection name
                    collection.insert_one(message_data)
                    logger.info('Message stored in MongoDB')

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error while storing message to MongoDB: {e}")
    finally:
        consumer.close()

def store_kafka_topics(topics):
    collection = mongo_db['topics']
    collection.insert_many([{'topic': topic} for topic in topics])
    logger.info("Kafka topics stored in MongoDB.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python script.py <command> [arguments]")
        sys.exit(1)

    command = sys.argv[1]

    if command == 'create_topic':
        if len(sys.argv) != 3:
            logger.error("Usage: python script.py create_topic <topic_name>")
            sys.exit(1)
        topic_name = sys.argv[2]
        create_kafka_topic(topic_name)

    elif command == 'list_topics':
        topics = list_kafka_topics()
        for topic in topics:
            print(topic)
        store_kafka_topics(topics)

    elif command == 'consume_topic':
        if len(sys.argv) != 3:
            logger.error("Usage: python script.py consume_topic <topic_name>")
            sys.exit(1)
        topic_name = sys.argv[2]
        consume_data(topic_name)

    elif command == 'produce_data':
        if len(sys.argv) != 4:
            logger.error("Usage: python script.py produce_data <table_name> <kafka_topic>")
            sys.exit(1)
        table_name = sys.argv[2]
        kafka_topic = sys.argv[3]
        produce_data(table_name, kafka_topic)

    else:
        logger.error("Invalid command. Use one of: create_topic, list_topics, consume_topic")

