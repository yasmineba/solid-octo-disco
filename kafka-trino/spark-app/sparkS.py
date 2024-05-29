from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json
from pyspark.sql.types import StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .config("spark.logLevel", "DEBUG") \
    .getOrCreate()

def process_kafka_topic(topic, mongo_uri):
    # Read stream from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .load()

    # Convert binary value column to string
    value_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Infer schema dynamically from JSON sample
    sample_json = value_df.select("value").first()[0]
    schema = schema_of_json(sample_json)

    # Process Kafka data using the inferred schema
    json_df = value_df.select(from_json(col("value"), schema).alias("data"))
    flattened_df = json_df.select("data.*")

    # Write stream to MongoDB
    query = flattened_df.writeStream \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", "default") \
        .option("collection", topic) \
        .start()  # Start the streaming query

    return query

# Define the topics to be processed
topics = ["trial2", "mytopic"]

# Process and write stream for each topic
queries = []
for topic in topics:
    mongo_uri = f"mongodb://mongodb:27017"
    query = process_kafka_topic(topic, mongo_uri)
    queries.append(query)

# Await termination for all streams
for query in queries:
    query.awaitTermination()

