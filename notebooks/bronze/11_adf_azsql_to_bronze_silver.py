# Databricks notebook source
# MAGIC %md
# MAGIC # Azure SQL -> Bronze + Silver (metadata-driven, ADF-ready)
# MAGIC
# MAGIC This notebook is designed to be called by Azure Data Factory with table-level parameters.

# COMMAND ----------

import re
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# ---- Config: Secret scope name (configure once per workspace/environment) ----
SECRET_SCOPE = "kv-databricks-secrets"

# ---- Runtime widgets ----
dbutils.widgets.text("p_schema", "dbo")
dbutils.widgets.text("p_table", "")
dbutils.widgets.text("p_snapshot_date", "")  # required, YYYY-MM-DD
dbutils.widgets.text("p_target_catalog", "")  # optional for Unity Catalog
dbutils.widgets.text("p_bronze_schema", "bronze")
dbutils.widgets.text("p_silver_schema", "silver")
dbutils.widgets.text("p_source_system", "azsql")

# Optional performance tuning widget
# Example: 10000 (leave empty to use JDBC default)
dbutils.widgets.text("p_fetchsize", "")

# Optional JDBC partitioning widgets for large tables (POC default = disabled)
# dbutils.widgets.text("p_partition_column", "")
# dbutils.widgets.text("p_lower_bound", "")
# dbutils.widgets.text("p_upper_bound", "")
# dbutils.widgets.text("p_num_partitions", "")

# COMMAND ----------


def normalize_identifier(raw: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", (raw or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        raise ValueError("Identifier normalization resulted in empty name.")
    return cleaned


def require_param(name: str, value: str) -> str:
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required parameter: {name}")
    return str(value).strip()


def validate_date_yyyy_mm_dd(value: str, param_name: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except Exception as exc:
        raise ValueError(
            f"Invalid {param_name}='{value}'. Expected format YYYY-MM-DD"
        ) from exc


def fq_schema(catalog: str, schema_name: str) -> str:
    return f"{catalog}.{schema_name}" if catalog else schema_name


def fq_table(catalog: str, schema_name: str, table_name: str) -> str:
    return f"{catalog}.{schema_name}.{table_name}" if catalog else f"{schema_name}.{table_name}"


# COMMAND ----------

# ---- Resolve and validate parameters ----
p_schema = dbutils.widgets.get("p_schema").strip() or "dbo"
p_table = require_param("p_table", dbutils.widgets.get("p_table"))
p_snapshot_date = validate_date_yyyy_mm_dd(
    require_param("p_snapshot_date", dbutils.widgets.get("p_snapshot_date")),
    "p_snapshot_date",
)
p_target_catalog = dbutils.widgets.get("p_target_catalog").strip()
p_bronze_schema = dbutils.widgets.get("p_bronze_schema").strip() or "bronze"
p_silver_schema = dbutils.widgets.get("p_silver_schema").strip() or "silver"
p_source_system = dbutils.widgets.get("p_source_system").strip() or "azsql"
p_fetchsize = dbutils.widgets.get("p_fetchsize").strip()

source_schema_norm = normalize_identifier(p_schema)
source_table_norm = normalize_identifier(p_table)
source_system_norm = normalize_identifier(p_source_system)

base_target_name = f"{source_system_norm}_{source_schema_norm}_{source_table_norm}"
bronze_table_fqn = fq_table(p_target_catalog, p_bronze_schema, base_target_name)
silver_table_fqn = fq_table(p_target_catalog, p_silver_schema, base_target_name)

source_dbtable = f"{p_schema}.{p_table}"

print("Resolved inputs and targets:")
print(f"- JDBC dbtable      : {source_dbtable}")
print(f"- Snapshot date     : {p_snapshot_date}")
print(f"- Bronze target     : {bronze_table_fqn}")
print(f"- Silver target     : {silver_table_fqn}")

# COMMAND ----------

# ---- Read Azure SQL connection secrets ----
try:
    azsql_server = dbutils.secrets.get(SECRET_SCOPE, "azsql_server")
    azsql_db = dbutils.secrets.get(SECRET_SCOPE, "azsql_db")
    azsql_user = dbutils.secrets.get(SECRET_SCOPE, "azsql_user")
    azsql_password = dbutils.secrets.get(SECRET_SCOPE, "azsql_password")
except Exception as exc:
    raise RuntimeError(
        "Failed to resolve Azure SQL secrets from scope "
        f"'{SECRET_SCOPE}'. Ensure keys azsql_server/azsql_db/azsql_user/azsql_password exist."
    ) from exc

jdbc_url = (
    f"jdbc:sqlserver://{azsql_server}:1433;"
    f"database={azsql_db};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

jdbc_reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_dbtable)
    .option("user", azsql_user)
    .option("password", azsql_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
)

if p_fetchsize:
    jdbc_reader = jdbc_reader.option("fetchsize", p_fetchsize)

# Optional partitioned JDBC read pattern (disabled by default for POC):
# p_partition_column = dbutils.widgets.get("p_partition_column").strip()
# p_lower_bound = dbutils.widgets.get("p_lower_bound").strip()
# p_upper_bound = dbutils.widgets.get("p_upper_bound").strip()
# p_num_partitions = dbutils.widgets.get("p_num_partitions").strip()
# if all([p_partition_column, p_lower_bound, p_upper_bound, p_num_partitions]):
#     jdbc_reader = (
#         jdbc_reader.option("partitionColumn", p_partition_column)
#         .option("lowerBound", p_lower_bound)
#         .option("upperBound", p_upper_bound)
#         .option("numPartitions", p_num_partitions)
#     )

try:
    df_source = jdbc_reader.load()
except Exception as exc:
    raise RuntimeError(
        f"Failed JDBC read for '{source_dbtable}'. Verify connectivity, credentials, and source object."
    ) from exc

# Add snapshot metadata columns
snapshot_date_lit = F.to_date(F.lit(p_snapshot_date))
df_enriched = (
    df_source.withColumn("snapshot_date", snapshot_date_lit)
    .withColumn("snapshot_ts", F.current_timestamp())
)

extracted_count = df_enriched.count()
if extracted_count == 0:
    raise RuntimeError(
        f"Empty extract for '{source_dbtable}' at snapshot_date={p_snapshot_date}."
    )

print(f"Extracted rows: {extracted_count}")

# COMMAND ----------

# Ensure target schemas/databases exist
bronze_schema_fqn = fq_schema(p_target_catalog, p_bronze_schema)
silver_schema_fqn = fq_schema(p_target_catalog, p_silver_schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema_fqn}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema_fqn}")

# BRONZE: append snapshot, partition by snapshot_date
(
    df_enriched.write.format("delta")
    .mode("append")
    .partitionBy("snapshot_date")
    .saveAsTable(bronze_table_fqn)
)

bronze_written_count = df_enriched.count()
print(f"Bronze rows written (append): {bronze_written_count}")

# COMMAND ----------

# SILVER strategy: latest snapshot only, no history accumulation.
# We use the run's p_snapshot_date as the authoritative latest for this run.
# Silver is fully replaced each run with Bronze rows for that date.

df_bronze_latest = spark.table(bronze_table_fqn).where(F.col("snapshot_date") == F.to_date(F.lit(p_snapshot_date)))

silver_count = df_bronze_latest.count()
if silver_count == 0:
    raise RuntimeError(
        f"No Bronze rows found for snapshot_date={p_snapshot_date} in {bronze_table_fqn}."
    )

(
    df_bronze_latest.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table_fqn)
)

print(f"Silver rows written (latest snapshot only): {silver_count}")
print("Notebook run completed successfully.")
