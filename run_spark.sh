#!/bin/bash

"$SPARK_HOME/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  stream-processing/traffic_stream.py