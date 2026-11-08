# Databricks notebook source
# 02_silver_transformation.py - Cleanses, validates, deduplicates and upserts to Silver Delta table

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Logistics_Silver_Transform").getOrCreate()

bronze_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/bronze/logistics_events"
silver_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/silver/fact_shipments"
quarantine_path = "abfss://delta-lakehouse@stlogisticsdatalake.dfs.core.windows.net/silver/quarantine_shipments"

# 1. Read latest micro-batch / changes from Bronze
bronze_df = spark.read.format("delta").load(bronze_path)

# 2. Schema and physical bounds validation
valid_condition = (
    F.col("shipment_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    (F.col("latitude").between(-90.0, 90.0) | F.col("latitude").isNull()) &
    (F.col("longitude").between(-180.0, 180.0) | F.col("longitude").isNull())
)

valid_df = bronze_df.filter(valid_condition)
quarantined_df = bronze_df.filter(~valid_condition).withColumn("_rejection_reason", F.lit("Corrupt coordinates or missing identifiers"))

# 3. Route corrupt records to quarantine
if quarantined_df.count() > 0:
    quarantined_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[WARN] Routed {quarantined_df.count()} records to quarantine.")

# 4. Deduplicate on composite business key
window_spec = Window.partitionBy("shipment_id", "event_type").orderBy(F.col("event_timestamp").desc())
deduped_df = (
    valid_df.withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .withColumn("event_date", F.to_date("event_timestamp"))
)

# 5. Delta Lake Merge (Upsert)
if DeltaTable.isDeltaTable(spark, silver_path):
    target_table = DeltaTable.forPath(spark, silver_path)
    (
        target_table.alias("target")
        .merge(
            deduped_df.alias("source"),
            "target.shipment_id = source.shipment_id AND target.event_type = source.event_type"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[SUCCESS] Delta Lake Silver merge complete.")
else:
    deduped_df.write.format("delta").mode("overwrite").save(silver_path)
    print("[SUCCESS] Initialized Silver Delta table.")
