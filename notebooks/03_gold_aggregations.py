# Databricks notebook source
# 03_gold_aggregations.py - Builds business marts: Daily Carrier OTIF & Hub Turnaround KPIs

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Logistics_Gold_Aggregations").getOrCreate()

silver_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/silver/fact_shipments"
gold_kpi_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/gold/kpi_carrier_otif_daily"

silver_df = spark.read.format("delta").load(silver_path)

# Calculate Daily Carrier OTIF (On-Time In-Full) performance
gold_otif = (
    silver_df
    .groupBy("event_date", "carrier_id", "origin_hub_id", "destination_hub_id")
    .agg(
        F.count("shipment_id").alias("total_shipments"),
        F.sum(F.when(F.col("status_code") == 200, 1).otherwise(0)).alias("on_time_shipments"),
        F.sum(F.when(F.col("status_code") == 400, 1).otherwise(0)).alias("delayed_shipments"),
        F.avg("temperature_celsius").alias("avg_transit_temp_c")
    )
    .withColumn(
        "otif_percentage",
        F.round((F.col("on_time_shipments") / F.col("total_shipments")) * 100.0, 2)
    )
)

(
    gold_otif.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_kpi_path)
)

# Optimize Delta layout with Z-ORDER
spark.sql(f"OPTIMIZE delta.`{gold_kpi_path}` ZORDER BY (carrier_id, event_date)")
print("[SUCCESS] Gold aggregates refreshed and optimized with Z-ORDER.")
