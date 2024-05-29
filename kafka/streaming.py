from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Create a DataFrame that streams data from Kafka
kafkaStream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mytopic") \
    .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .load()

# Select the 'value' column and cast it as a string, then append 'Hello Spark!' to each line
parsed = kafkaStream.select(concat(col("value").cast("string"), lit(" Hello Spark!")).alias("Result"))

# Write the DataFrame to the console
query = parsed.writeStream.outputMode("append").format("console").start()

query.awaitTermination()

