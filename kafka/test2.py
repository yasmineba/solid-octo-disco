from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FileTest").getOrCreate()

df = spark.read.text("/home/yasmine/Desktop/kafka/test.txt")

# Select the 'value' column directly
parsed = df.select(col("value"))

# Add "and hello spark" to the end of each message
parsed = parsed.withColumn("value", col("value") + " and hello spark")

parsed.show()

