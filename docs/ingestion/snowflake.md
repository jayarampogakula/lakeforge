# Snowflake Ingestion Guide

Load data from Snowflake Data Warehouse into your Data Lakehouse.

## Overview

* Native Snowflake-Spark connector
* Automatic query pushdown
* Parallel data loading
* Zero-copy cloning support

## Prerequisites

* Snowflake account and credentials
* Virtual warehouse access
* Database and schema permissions

## Configuration

### Basic Setup

```python
from lakeforge.ingestion import SnowflakeLoader

spark = SparkSession.builder.appName("SnowflakeIngestion").getOrCreate()

loader = SnowflakeLoader(
    spark=spark,
    account="xy12345.us-east-1",
    user="etl_user",
    password=dbutils.secrets.get(scope="prod", key="snowflake-password"),
    database="ANALYTICS",
    schema="PUBLIC",
    warehouse="COMPUTE_WH",
    role="ETL_ROLE"
)

# Load table
df = loader.load_table("ORDERS")

# Write to Bronze
df.write.format("delta").mode("overwrite").save("/mnt/bronze/sf_orders")
```

### Custom Query

```python
query = """
    SELECT 
        o.*,
        c.customer_name,
        p.product_name
    FROM ORDERS o
    JOIN CUSTOMERS c ON o.customer_id = c.customer_id
    JOIN PRODUCTS p ON o.product_id = p.product_id
    WHERE o.order_date >= '2024-01-01'
"""

df = loader.load_query(query)
```

### Incremental Load

```python
df = loader.load_incremental(
    table_name="ORDERS",
    timestamp_column="MODIFIED_TIMESTAMP",
    last_timestamp="2024-06-01 00:00:00"
)
```

## Performance Optimization

* Enable autopushdown for query optimization
* Use appropriate warehouse size
* Leverage Snowflake clustering keys
* Monitor query history and optimization

## Troubleshooting

* **Authentication errors**: Verify account identifier format
* **Warehouse suspended**: Start warehouse before loading
* **Slow queries**: Check query profile in Snowflake console
