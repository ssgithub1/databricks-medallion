# Databricks notebook source
# MAGIC %run ../00_config_and_utils


# COMMAND ----------

print("add_ingest_metadata exists?", "add_ingest_metadata" in globals())


# COMMAND ----------

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

# MAGIC %run /Users/sshanmugam@zirous.com/databricks-medallion/notebooks/00_config_and_utils

# COMMAND ----------

# Utilities

# %run ../00_config_and_utils

# COMMAND ----------

print("add_ingest_metadata exists?", "add_ingest_metadata" in globals())


# COMMAND ----------

from pyspark.sql import functions as F

files = [f.path for f in dbutils.fs.ls(source_path) if f.path.endswith(".parquet")]
print("Found files:", len(files))
for f in files:
    print(" -", f)

dfs = []
for fpath in files:
    df_i = spark.read.parquet(fpath)

    # Standardize column names early
    df_i = df_i.select([F.col(c).alias(c.lower().strip()) for c in df_i.columns])

    dfs.append(df_i)

# Union with missing columns allowed
raw_df = dfs[0]
for d in dfs[1:]:
    raw_df = raw_df.unionByName(d, allowMissingColumns=True)

print("Combined columns:", len(raw_df.columns))


# COMMAND ----------

from pyspark.sql import functions as F

files = [f.path for f in dbutils.fs.ls(source_path) if f.path.endswith(".parquet")]
print("Found files:", len(files))

dfs = []
for fpath in files:
    df_i = spark.read.parquet(fpath)

    # Standardize columns early
    df_i = df_i.select([F.col(c).alias(c.lower().strip()) for c in df_i.columns])

    # Add per-file metadata BEFORE union (because _metadata won't survive union reliably)
    df_i = (
        df_i
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(fpath))
        .withColumn("_batch_id", F.expr("uuid()"))
    )

    # Optional: cast known "money" columns to double to avoid schema drift
    money_like = [
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
    ]
    for c in money_like:
        if c in df_i.columns:
            df_i = df_i.withColumn(c, F.col(c).cast("double"))

    dfs.append(df_i)

raw_df = dfs[0]
for d in dfs[1:]:
    raw_df = raw_df.unionByName(d, allowMissingColumns=True)

bronze_df = raw_df  # already has ingest metadata


# COMMAND ----------

money_like = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
]

for c in money_like:
    if c in raw_df.columns:
        raw_df = raw_df.withColumn(c, F.col(c).cast("double"))


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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_nyc_taxi_yellow
