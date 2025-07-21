from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max as spark_max, min as spark_min, count
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("TrafficStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka messages
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

# Parse and filter traffic sensor data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("sensor_type") == "traffic")

# Aggregate average traffic count per hour per location (existing)
hourly_df = parsed_df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window(col("timestamp"), "1 hour"),
        col("location")
    ).agg(
        avg("value").alias("avg_traffic_count")
    ).select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("location"),
        col("avg_traffic_count")
    )

#5-minute real-time aggregations for dashboard
realtime_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("location")
    ).agg(
        avg("value").alias("avg_traffic_count"),
        spark_max("value").alias("max_traffic_count"),
        spark_min("value").alias("min_traffic_count"),
        count("value").alias("data_points")
    ).select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("location"),
        col("avg_traffic_count"),
        col("max_traffic_count"),
        col("min_traffic_count"),
        col("data_points")
    )

# Define sink to PostgreSQL for hourly data
def write_hourly_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/traffic_data") \
        .option("dbtable", "hourly_traffic_summary") \
        .option("user", "nickbui") \
        .option("password", "dummy") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Define sink to PostgreSQL for real-time data
def write_realtime_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/traffic_data") \
        .option("dbtable", "realtime_traffic_summary") \
        .option("user", "nickbui") \
        .option("password", "dummy") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start both streaming queries
hourly_query = hourly_df.writeStream \
    .foreachBatch(write_hourly_to_postgres) \
    .outputMode("update") \
    .queryName("hourly_aggregation") \
    .start()

realtime_query = realtime_df.writeStream \
    .foreachBatch(write_realtime_to_postgres) \
    .outputMode("update") \
    .queryName("realtime_aggregation") \
    .start()

# Wait for both queries
hourly_query.awaitTermination()