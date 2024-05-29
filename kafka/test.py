from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["Name", "Value"])
df.show()
