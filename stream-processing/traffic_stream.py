from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("TrafficStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("location", StringType()) \
    .add("sensor_type", StringType()) \
    .add("value", IntegerType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("sensor_type") == "traffic")

# Aggregate: avg cars per hour per intersection
agg_df = parsed_df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window(col("timestamp"), "1 hour"),
        col("location")
    ).agg(
        avg("value").alias("avg_traffic_count")
    )

# Print to console
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()