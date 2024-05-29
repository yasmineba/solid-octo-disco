from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from confluent_kafka import Consumer, KafkaError
import pymongo
import json
import sys

# MongoDB Configuration
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client.kpi_database

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'spark-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

# Spark Session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", StringType(), True),
    # Define schema as per your data
])

# Function to process each RDD
def process(message):
    try:
        data_dict = json.loads(message)
        df = spark.createDataFrame([data_dict], schema=schema)
        # Perform your KPI calculations here
        kpi_result = df.groupBy("some_field").count().collect()
        db.kpi_collection.insert_many(kpi_result)
    except Exception as e:
        print("Error processing message:", e)

# Streaming loop
def consume_kafka_topic(topic):
    consumer.subscribe([topic])
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
        process(msg.value().decode('utf-8'))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: consumer.py <kafka_topic>")
        sys.exit(1)

    kafka_topic = sys.argv[1]
    consume_kafka_topic(kafka_topic)

consumer.close()

