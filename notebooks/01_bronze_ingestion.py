# Databricks notebook source
# 01_bronze_ingestion.py - Ingests landing events to Bronze Delta table via Auto Loader

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
)
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Logistics_Bronze_Ingestion").getOrCreate()

source_path = "abfss://raw-logistics-landing@stlogisticsdatalake.dfs.core.windows.net/events/"
checkpoint_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/checkpoints/bronze/"
bronze_table_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/bronze/logistics_events"

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("shipment_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("carrier_id", StringType(), True),
    StructField("origin_hub_id", StringType(), True),
    StructField("destination_hub_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature_celsius", DoubleType(), True),
    StructField("status_code", IntegerType(), True),
])

print("[INFO] Starting Auto Loader streaming ingestion...")

streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path + "schema/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .schema(schema)
    .load(source_path)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

query = (
    streaming_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_table_path)
)

print(f"[INFO] Ingestion streaming active: {query.id}")
