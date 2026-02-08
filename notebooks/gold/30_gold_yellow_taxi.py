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

# MAGIC %run /Users/sshanmugam@zirous.com/databricks-medallion/notebooks/00_config_and_utils

# COMMAND ----------

# Utilities

# %run ../00_config_and_utils

# COMMAND ----------

# Parameters

dataset_name = dbutils.widgets.get("dataset_name")
write_mode = dbutils.widgets.get("write_mode")
base_path = dbutils.widgets.get("base_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
silver_table = dbutils.widgets.get("silver_table")
gold_table_prefix = dbutils.widgets.get("gold_table_prefix")
partition_cols = parse_csv_list(dbutils.widgets.get("partition_cols"))

# COMMAND ----------

# Read Silver

qualified_silver_table = qualify_table(catalog, schema, silver_table)
silver_input_path = silver_path(base_path, dataset_name) if base_path else ""

if write_mode == "table":
    silver_df = spark.read.table(qualified_silver_table)
else:
    silver_df = spark.read.format("delta").load(silver_input_path)

# COMMAND ----------

# Daily trip metrics

has_dropoff = "tpep_dropoff_datetime" in silver_df.columns

trip_duration_expr = None
if has_dropoff:
    trip_duration_expr = (
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
        / 60.0
    )

agg_exprs = [
    F.count(F.lit(1)).alias("trip_count"),
    F.sum(F.col("fare_amount")).alias("total_fare"),
    F.sum(F.col("total_amount")).alias("total_revenue"),
    F.avg(F.col("trip_distance")).alias("avg_distance"),
]

if trip_duration_expr is not None:
    agg_exprs.append(F.avg(trip_duration_expr).alias("avg_duration_min"))


daily_trip_metrics = silver_df.groupBy("pickup_date").agg(*agg_exprs)

# COMMAND ----------

# Hourly trip metrics

hourly_trip_metrics = silver_df.groupBy("pickup_date", "pickup_hour").agg(
    F.count(F.lit(1)).alias("trip_count"),
    F.sum(F.col("total_amount")).alias("total_revenue"),
)

# COMMAND ----------

# Write Gold

qualified_daily_table = qualify_table(
    catalog, schema, f"{gold_table_prefix}_daily_trip_metrics"
)
qualified_hourly_table = qualify_table(
    catalog, schema, f"{gold_table_prefix}_hourly_trip_metrics"
)

gold_base_path = gold_path(base_path, dataset_name) if base_path else ""

if write_mode == "table":
    write_delta(
        daily_trip_metrics,
        write_mode=write_mode,
        table_name=qualified_daily_table,
        path="",
        partition_cols=partition_cols,
        mode="overwrite",
    )
    write_delta(
        hourly_trip_metrics,
        write_mode=write_mode,
        table_name=qualified_hourly_table,
        path="",
        partition_cols=partition_cols,
        mode="overwrite",
    )
else:
    write_delta(
        daily_trip_metrics,
        write_mode=write_mode,
        table_name=qualified_daily_table,
        path=f"{gold_base_path}/daily_trip_metrics",
        partition_cols=partition_cols,
        mode="overwrite",
    )
    write_delta(
        hourly_trip_metrics,
        write_mode=write_mode,
        table_name=qualified_hourly_table,
        path=f"{gold_base_path}/hourly_trip_metrics",
        partition_cols=partition_cols,
        mode="overwrite",
    )

# COMMAND ----------

# Diagnostics

print(f"Daily trip metrics row count: {daily_trip_metrics.count()}")
print(f"Hourly trip metrics row count: {hourly_trip_metrics.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_nyc_taxi_yellow_daily_trip_metrics
