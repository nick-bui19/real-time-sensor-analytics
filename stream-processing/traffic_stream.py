from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Define schema for incoming Kafka data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("sensor_type", StringType()) \
    .add("value", IntegerType())

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("TrafficStreamProcessor") \
    .getOrCreate()

# Read stream from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (bytes) to JSON using our schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Output to console (for now)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()