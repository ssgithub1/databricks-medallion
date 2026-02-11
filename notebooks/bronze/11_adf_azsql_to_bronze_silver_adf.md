# ADF + Databricks POC: Azure SQL to Bronze/Silver

## ADF pipeline design (metadata-driven)

### 1) Linked services and datasets
1. **Azure SQL Database linked service**
   - Use managed identity or SQL auth as per sandbox setup.
2. **Azure SQL dataset** (for Lookup query)
   - Can be generic query-based dataset.
3. **Azure Databricks linked service**
   - Configure workspace URL + auth + existing cluster/job cluster.

### 2) Pipeline parameters (recommended)
- `p_schema_filter` (string, default: `dbo`)
- `p_batch_count` (int, default: `5`)

### 3) Lookup activity: `LKP_GetTables`
- Source: Azure SQL dataset.
- Use **Query**:

```sql
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_SCHEMA = '@{pipeline().parameters.p_schema_filter}'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

> If your ADF UI does not support inline dynamic content in SQL query text cleanly, build the query using a pipeline expression or use a stored procedure.

Alternative query when hardcoding schema for POC:

```sql
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

### 4) ForEach activity: `FE_Tables`
- Items:

```adf
@activity('LKP_GetTables').output.value
```

- Batch count:

```adf
@pipeline().parameters.p_batch_count
```

- Is sequential: `false` (for parallel table loads in batches).

### 5) Inside ForEach: Databricks Notebook activity `NB_LoadTable`
- Notebook path: `/Repos/.../notebooks/bronze/11_adf_azsql_to_bronze_silver`
- Base parameters (ADF dynamic content strings):

```text
p_schema         = @item().TABLE_SCHEMA
p_table          = @item().TABLE_NAME
p_snapshot_date  = @formatDateTime(utcnow(),'yyyy-MM-dd')
p_source_system  = azsql
p_bronze_schema  = bronze
p_silver_schema  = silver
```

Optional base parameters:

```text
p_target_catalog = <empty for non-Unity Catalog workspaces>
p_fetchsize      = 10000
```

### 6) Retries, timeout, and parallelism guidance
- On **Notebook activity**:
  - Retry: `2` to `3`
  - Retry interval: `00:01:00` to `00:05:00`
  - Timeout: set based on largest expected table (e.g., `01:00:00` for POC).
- On **ForEach**:
  - Start conservative batch count (`3` to `5`), then scale after observing SQL DB and cluster utilization.
- Operational recommendations:
  - Use smaller batch for very large/locking-sensitive source systems.
  - Use activity-level alerts on failure and include table name in failure diagnostics.

## Optional ADF JSON skeleton (trimmed)

```json
{
  "name": "pl_azsql_to_databricks_medallion_poc",
  "properties": {
    "parameters": {
      "p_schema_filter": { "type": "String", "defaultValue": "dbo" },
      "p_batch_count": { "type": "Int", "defaultValue": 5 }
    },
    "activities": [
      {
        "name": "LKP_GetTables",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY TABLE_SCHEMA, TABLE_NAME"
          },
          "firstRowOnly": false
        }
      },
      {
        "name": "FE_Tables",
        "type": "ForEach",
        "dependsOn": [
          { "activity": "LKP_GetTables", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "items": "@activity('LKP_GetTables').output.value",
          "isSequential": false,
          "batchCount": "@pipeline().parameters.p_batch_count",
          "activities": [
            {
              "name": "NB_LoadTable",
              "type": "DatabricksNotebook",
              "policy": {
                "retry": 2,
                "retryIntervalInSeconds": 120,
                "timeout": "01:00:00"
              },
              "typeProperties": {
                "notebookPath": "/Repos/.../notebooks/bronze/11_adf_azsql_to_bronze_silver",
                "baseParameters": {
                  "p_schema": "@item().TABLE_SCHEMA",
                  "p_table": "@item().TABLE_NAME",
                  "p_snapshot_date": "@formatDateTime(utcnow(),'yyyy-MM-dd')",
                  "p_source_system": "azsql",
                  "p_bronze_schema": "bronze",
                  "p_silver_schema": "silver"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
```

## Databricks SQL validation queries

Use `<catalog>.` prefix only when Unity Catalog is enabled.

### Bronze partitions and counts by snapshot

```sql
SELECT
  snapshot_date,
  COUNT(*) AS row_count
FROM bronze.azsql_dbo_yourtable
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;
```

### Silver contains only latest snapshot rows

```sql
SELECT
  MIN(snapshot_date) AS min_snapshot_date,
  MAX(snapshot_date) AS max_snapshot_date,
  COUNT(DISTINCT snapshot_date) AS distinct_snapshot_dates,
  COUNT(*) AS row_count
FROM silver.azsql_dbo_yourtable;
```

Expected: `distinct_snapshot_dates = 1`.

### Compare Bronze latest snapshot row count vs Silver

```sql
WITH bronze_latest AS (
  SELECT MAX(snapshot_date) AS latest_snapshot_date
  FROM bronze.azsql_dbo_yourtable
),
bronze_count AS (
  SELECT COUNT(*) AS bronze_latest_count
  FROM bronze.azsql_dbo_yourtable b
  CROSS JOIN bronze_latest l
  WHERE b.snapshot_date = l.latest_snapshot_date
),
silver_count AS (
  SELECT COUNT(*) AS silver_count
  FROM silver.azsql_dbo_yourtable
)
SELECT
  bc.bronze_latest_count,
  sc.silver_count,
  CASE WHEN bc.bronze_latest_count = sc.silver_count THEN 'MATCH' ELSE 'MISMATCH' END AS status
FROM bronze_count bc
CROSS JOIN silver_count sc;
```
