# Oracle Database Ingestion Guide

Load data from Oracle Database (on-premise and cloud) into your Data Lakehouse.

## Overview

The Oracle loader supports:
* Full table loads
* Incremental loads based on timestamps
* Custom SQL queries
* Partitioned parallel reads
* Both Oracle Database and Oracle Cloud

## Prerequisites

* Oracle JDBC driver (ojdbc8.jar or ojdbc11.jar)
* Database credentials
* Network connectivity
* Read permissions on source tables

## JDBC Driver Setup

### Install Oracle JDBC Driver

```bash
# Download from Oracle website or Maven
wget https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.1.0.0/ojdbc8-21.1.0.0.jar

# Install in Databricks
dbfs cp ojdbc8-21.1.0.0.jar dbfs:/FileStore/jars/
```

### Cluster Configuration

Add to cluster libraries:
* Maven: `com.oracle.database.jdbc:ojdbc8:21.1.0.0`
* Or upload JAR to DBFS

## Configuration

### Basic Table Load

```python
from lakeforge.ingestion import OracleLoader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OracleIngestion").getOrCreate()

# Initialize loader
loader = OracleLoader(
    spark=spark,
    host="oracle.company.com",
    port=1521,
    service_name="PROD",  # or SID
    user="etl_user",
    password=dbutils.secrets.get(scope="prod", key="oracle-password")
)

# Load table
df = loader.load_table("SALES.ORDERS")

# Write to Bronze
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .save("/mnt/bronze/orders")
```

### Partitioned Read (for large tables)

```python
# Use numeric column for partitioning
df = loader.load_table(
    table_name="SALES.ORDERS",
    num_partitions=10,
    partition_column="ORDER_ID",
    lower_bound=1,
    upper_bound=1000000
)
```

### Custom SQL Query

```python
query = """
    SELECT o.*, c.customer_name, c.customer_tier
    FROM SALES.ORDERS o
    JOIN SALES.CUSTOMERS c ON o.customer_id = c.customer_id
    WHERE o.order_date >= DATE '2024-01-01'
"""

df = loader.load_query(query)
```

### Incremental Load

```python
from pyspark.sql.functions import max as spark_max

# Get last loaded timestamp
last_timestamp = spark.read.format("delta") \
    .load("/mnt/bronze/orders") \
    .select(spark_max("modified_date")).collect()[0][0]

# Load only new/updated records
incremental_df = loader.load_incremental(
    table_name="SALES.ORDERS",
    timestamp_column="MODIFIED_DATE",
    last_timestamp=str(last_timestamp)
)

# Append to Bronze
incremental_df.write.format("delta") \
    .mode("append") \
    .save("/mnt/bronze/orders")
```

## Writing to Bronze Layer

### Full Load Pattern

```python
from pyspark.sql.functions import current_timestamp

# Add metadata
bronze_df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("ORACLE_PROD"))

# Write with overwrite
bronze_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_date") \
    .save("/mnt/bronze/orders")
```

### Incremental Load Pattern

```python
# Merge new data using Delta merge
from delta.tables import DeltaTable

bronze_table = DeltaTable.forPath(spark, "/mnt/bronze/orders")

bronze_table.alias("target").merge(
    incremental_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

## Advanced Configuration

### Connection Pooling

```python
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
```

### Large Object Handling (LOBs)

```python
# Configure for CLOB/BLOB columns
oracle_props = {
    "oracle.jdbc.defaultRowPrefetch": "20",
    "oracle.net.READ_TIMEOUT": "600000"
}
```

## Troubleshooting

### TNS Connection Errors
* Verify tnsnames.ora configuration
* Check listener status: `lsnrctl status`
* Test connectivity: `tnsping SERVICE_NAME`

### ORA-01000: Maximum open cursors exceeded
* Increase `open_cursors` parameter in Oracle
* Reduce partition count

### Performance Issues
* Use partitioned reads for tables > 10M rows
* Create indexes on partition columns
* Enable query pushdown
* Monitor AWR reports

### Character Encoding
* Set NLS_LANG environment variable
* Configure character set in JDBC URL

## Best Practices

1. **Secrets Management**: Use Databricks Secrets for passwords
2. **Partitioning**: Use numeric ID columns for parallel reads
3. **Incremental Loads**: Track watermarks in control table
4. **Schema Evolution**: Enable mergeSchema for flexibility
5. **Monitoring**: Log row counts and execution times
6. **Network**: Use VPN or private endpoints for production
