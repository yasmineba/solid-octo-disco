# project-root/spark-streaming/spark_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkMongo") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/mydb.mycollection") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "trino-topic") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([StructField("Name", StringType(), True)])

json_df = df.select(from_json(col("value"), schema).alias("Name"))

query = json_df \
    .writeStream \
    .format("mongo") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()
