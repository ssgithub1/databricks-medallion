# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Widgets

dbutils.widgets.text("dataset_name", "nyc_taxi_yellow")
dbutils.widgets.dropdown("source_format", "parquet", ["parquet", "csv"])
dbutils.widgets.text("source_path", "")
dbutils.widgets.dropdown("write_mode", "table", ["table", "path"])
dbutils.widgets.text("base_path", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("bronze_table", "bronze_nyc_taxi_yellow")
dbutils.widgets.text("silver_table", "silver_nyc_taxi_yellow")
dbutils.widgets.text("gold_table_prefix", "gold_nyc_taxi_yellow")
dbutils.widgets.text(
    "primary_keys",
    "vendorid,tpep_pickup_datetime,tpep_dropoff_datetime,pulocationid,dolocationid",
)
dbutils.widgets.text("event_time_column", "tpep_pickup_datetime")
dbutils.widgets.text("dedupe_order_column", "_ingest_ts")
dbutils.widgets.dropdown("merge_mode", "append", ["append", "merge"])
dbutils.widgets.text("partition_cols", "pickup_date")

# COMMAND ----------
# Parameters

dataset_name = dbutils.widgets.get("dataset_name")
source_format = dbutils.widgets.get("source_format")
source_path = dbutils.widgets.get("source_path")
write_mode = dbutils.widgets.get("write_mode")
base_path = dbutils.widgets.get("base_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_table = dbutils.widgets.get("bronze_table")
event_time_column = dbutils.widgets.get("event_time_column")
partition_cols = dbutils.widgets.get("partition_cols")

# COMMAND ----------
# Utilities

# MAGIC %run ../00_config_and_utils

# COMMAND ----------
# Read source

if not source_path:
    raise ValueError("source_path widget must be provided.")

reader = spark.read.format(source_format)
if source_format == "csv":
    reader = reader.option("header", True).option("inferSchema", True)

raw_df = reader.load(source_path)

# COMMAND ----------
# Standardize columns

standardized_df = raw_df.select(
    [F.col(c).alias(c.lower().strip()) for c in raw_df.columns]
)

bronze_df = add_ingest_metadata(standardized_df)

# COMMAND ----------
# Write Bronze

qualified_bronze_table = qualify_table(catalog, schema, bronze_table)
bronze_output_path = bronze_path(base_path, dataset_name) if base_path else ""

write_delta(
    bronze_df,
    write_mode=write_mode,
    table_name=qualified_bronze_table,
    path=bronze_output_path,
    partition_cols=parse_csv_list(partition_cols),
    mode="append",
)

# COMMAND ----------
# Diagnostics

row_count = bronze_df.count()
print(f"Bronze row count: {row_count}")

if event_time_column in bronze_df.columns:
    stats = bronze_df.select(
        F.min(F.col(event_time_column)).alias("min_event_time"),
        F.max(F.col(event_time_column)).alias("max_event_time"),
    ).collect()[0]
    print(f"Event time range: {stats['min_event_time']} - {stats['max_event_time']}")
