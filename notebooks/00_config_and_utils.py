# Databricks notebook source
# Config and utility helpers for Medallion pipelines

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def parse_csv_list(value: str) -> list[str]:
    """Parse a comma-separated string into a list of trimmed values."""
    if value is None:
        return []
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items


def is_uc_mode(catalog: str, schema: str) -> bool:
    """Return True if both catalog and schema are provided for UC-qualified tables."""
    return bool(catalog and schema)


def qualify_table(catalog: str, schema: str, table: str) -> str:
    """Qualify table name with catalog and schema when provided."""
    if is_uc_mode(catalog, schema):
        return f"{catalog}.{schema}.{table}"
    return table


def bronze_path(base_path: str, dataset_name: str) -> str:
    return f"{base_path.rstrip('/')}/bronze/{dataset_name}"


def silver_path(base_path: str, dataset_name: str) -> str:
    return f"{base_path.rstrip('/')}/silver/{dataset_name}"


def gold_path(base_path: str, dataset_name: str) -> str:
    return f"{base_path.rstrip('/')}/gold/{dataset_name}"


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    """Add ingest metadata columns to a DataFrame."""
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.expr("uuid()"))
    )


def require_non_null(df: DataFrame, cols: list[str]) -> None:
    """Raise ValueError if any of the provided columns contain nulls."""
    if not cols:
        return
    null_counts = (
        df.select(
            [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols]
        ).collect()[0]
    )
    failures = {c: null_counts[c] for c in cols if null_counts[c] > 0}
    if failures:
        raise ValueError(f"Nulls detected in primary keys: {failures}")


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the latest row per key based on the order column."""
    if not keys:
        return df
    window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(
        F.col(order_col).desc_nulls_last()
    )
    return df.withColumn("_row_number", F.row_number().over(window_spec)).filter(
        F.col("_row_number") == 1
    ).drop("_row_number")


def write_delta(
    df: DataFrame,
    write_mode: str,
    table_name: str,
    path: str,
    partition_cols: list[str],
    mode: str,
) -> None:
    """Write a DataFrame to Delta as a table or path."""
    if write_mode == "table":
        df.write.format("delta").mode(mode).saveAsTable(table_name)
    else:
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        writer.save(path)
