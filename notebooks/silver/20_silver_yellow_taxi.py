# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

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

# # Utilities

# %run ../00_config_and_utils

# COMMAND ----------

# Parameters

dataset_name = dbutils.widgets.get("dataset_name")
write_mode = dbutils.widgets.get("write_mode")
base_path = dbutils.widgets.get("base_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
primary_keys = parse_csv_list(dbutils.widgets.get("primary_keys"))
event_time_column = dbutils.widgets.get("event_time_column")
dedupe_order_column = dbutils.widgets.get("dedupe_order_column")
merge_mode = dbutils.widgets.get("merge_mode")
partition_cols = parse_csv_list(dbutils.widgets.get("partition_cols"))

# COMMAND ----------

# Read Bronze

qualified_bronze_table = qualify_table(catalog, schema, bronze_table)
bronze_input_path = bronze_path(base_path, dataset_name) if base_path else ""

if write_mode == "table":
    bronze_df = spark.read.table(qualified_bronze_table)
else:
    bronze_df = spark.read.format("delta").load(bronze_input_path)

# COMMAND ----------

# Schema enforcement for common Yellow Taxi fields

# NOTE: Field availability can differ by year; adjust as needed.
yellow_taxi_schema = T.StructType(
    [
        T.StructField("vendorid", T.IntegerType(), True),
        T.StructField("tpep_pickup_datetime", T.TimestampType(), True),
        T.StructField("tpep_dropoff_datetime", T.TimestampType(), True),
        T.StructField("passenger_count", T.IntegerType(), True),
        T.StructField("trip_distance", T.DoubleType(), True),
        T.StructField("ratecodeid", T.IntegerType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("pulocationid", T.IntegerType(), True),
        T.StructField("dolocationid", T.IntegerType(), True),
        T.StructField("payment_type", T.IntegerType(), True),
        T.StructField("fare_amount", T.DoubleType(), True),
        T.StructField("extra", T.DoubleType(), True),
        T.StructField("mta_tax", T.DoubleType(), True),
        T.StructField("tip_amount", T.DoubleType(), True),
        T.StructField("tolls_amount", T.DoubleType(), True),
        T.StructField("improvement_surcharge", T.DoubleType(), True),
        T.StructField("total_amount", T.DoubleType(), True),
        T.StructField("congestion_surcharge", T.DoubleType(), True),
        # Example for later versions:
        # T.StructField("airport_fee", T.DoubleType(), True),
    ]
)

cast_exprs = []
for field in yellow_taxi_schema.fields:
    if field.name in bronze_df.columns:
        cast_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))

other_cols = [c for c in bronze_df.columns if c not in {f.name for f in yellow_taxi_schema.fields}]

silver_df = bronze_df.select(*cast_exprs, *[F.col(c) for c in other_cols])

# COMMAND ----------

# Cast and clean

if event_time_column in silver_df.columns:
    silver_df = silver_df.withColumn(
        event_time_column, F.col(event_time_column).cast("timestamp")
    )

for col_name, dtype in silver_df.dtypes:
    if dtype == "string":
        silver_df = silver_df.withColumn(col_name, F.trim(F.col(col_name)))

for measure in ["trip_distance", "fare_amount", "total_amount"]:
    if measure in silver_df.columns:
        silver_df = silver_df.withColumn(
            measure,
            F.when(F.col(measure) < 0, F.lit(None)).otherwise(F.col(measure)),
        )

# COMMAND ----------

# Derived columns

if event_time_column in silver_df.columns:
    silver_df = silver_df.withColumn("pickup_date", F.to_date(F.col(event_time_column)))
    silver_df = silver_df.withColumn("pickup_hour", F.hour(F.col(event_time_column)))

# COMMAND ----------

# Dedupe

silver_df = dedupe_latest(silver_df, primary_keys, dedupe_order_column)

# COMMAND ----------

# Data quality

require_non_null(silver_df, primary_keys)

# COMMAND ----------

# Write Silver

qualified_silver_table = qualify_table(catalog, schema, silver_table)
silver_output_path = silver_path(base_path, dataset_name) if base_path else ""

if merge_mode == "merge" and write_mode == "table":
    if spark.catalog.tableExists(qualified_silver_table):
        silver_df.createOrReplaceTempView("silver_updates")
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in primary_keys]
        )
        merge_sql = f"""
            MERGE INTO {qualified_silver_table} AS target
            USING silver_updates AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    else:
        write_delta(
            silver_df,
            write_mode=write_mode,
            table_name=qualified_silver_table,
            path=silver_output_path,
            partition_cols=partition_cols,
            mode="overwrite",
        )
else:
    write_delta(
        silver_df,
        write_mode=write_mode,
        table_name=qualified_silver_table,
        path=silver_output_path,
        partition_cols=partition_cols,
        mode="overwrite",
    )

# COMMAND ----------

# Diagnostics

row_count = silver_df.count()
print(f"Silver row count: {row_count}")

if event_time_column in silver_df.columns:
    stats = silver_df.select(
        F.min(F.col(event_time_column)).alias("min_event_time"),
        F.max(F.col(event_time_column)).alias("max_event_time"),
        F.countDistinct(F.col("pickup_date")).alias("distinct_pickup_dates"),
    ).collect()[0]
    print(f"Event time range: {stats['min_event_time']} - {stats['max_event_time']}")
    print(f"Distinct pickup_date: {stats['distinct_pickup_dates']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_nyc_taxi_yellow
