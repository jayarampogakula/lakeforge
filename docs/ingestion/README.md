# LakeForge Ingestion Guide

Comprehensive guide for ingesting data from various sources into your Data Lakehouse.

## Supported Data Sources

### Cloud Data Warehouses
* [Snowflake](snowflake.md) - Load from Snowflake data warehouse
* [BigQuery](bigquery.md) - Load from Google BigQuery
* [Redshift](redshift.md) - Load from AWS Redshift

### Relational Databases
* [Oracle](oracle.md) - Load from Oracle Database (on-premise and cloud)
* [PostgreSQL](postgres.md) - Load from PostgreSQL
* [MySQL](mysql.md) - Load from MySQL/MariaDB
* [Azure SQL](azure_sql.md) - Load from Azure SQL Database

### NoSQL & Document Databases
* [MongoDB](mongodb.md) - Load from MongoDB collections

### Cloud Storage
* [AWS S3](s3.md) - Load from Amazon S3 buckets
* [GCS](gcs.md) - Load from Google Cloud Storage
* [Parquet Files](parquet.md) - Load Parquet files from any location
* [Filesystem](filesystem.md) - Load from local, DBFS, or mounted storage

### Streaming & Messaging
* [Kafka](kafka.md) - Stream from Apache Kafka topics

### SaaS & Collaboration Tools
* [Jira](jira.md) - Load from Atlassian Jira
* [SharePoint](sharepoint.md) - Load from Microsoft SharePoint
* [Google Sheets](google_sheets.md) - Load from Google Sheets

## Quick Start

### 1. Installation

```python
# Install LakeForge
pip install lakeforge
```

### 2. Basic Usage

```python
from lakeforge.ingestion import OracleLoader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LakeForge").getOrCreate()

# Initialize loader
loader = OracleLoader(
    spark=spark,
    host="oracle.example.com",
    port=1521,
    service_name="PROD",
    user="etl_user",
    password="password"
)

# Load data
df = loader.load_table("SALES.ORDERS")

# Write to Bronze layer
df.write.format("delta").mode("overwrite").save("/mnt/bronze/orders")
```

### 3. Bronze Layer Pattern

All loaders follow the medallion architecture pattern:

```python
# Bronze: Raw data ingestion
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_date") \
    .save(f"{bronze_path}/{table_name}")
```

## Architecture Patterns

### Batch Ingestion
* Full load: Complete table snapshots
* Incremental load: Load only new/changed records
* Time-based: Filter by timestamp columns

### Streaming Ingestion
* Auto Loader: Cloud file ingestion
* Kafka streams: Real-time event processing
* CDC: Change data capture patterns

### Data Quality
* Schema validation
* Null handling
* Duplicate detection
* Data profiling

## Configuration

### Secrets Management

Use Databricks Secrets to store credentials:

```python
from databricks.sdk.runtime import *

password = dbutils.secrets.get(scope="production", key="oracle-password")
```

### Compute Configuration

Recommended cluster settings for different workloads:
* **Batch processing**: Standard clusters with Photon
* **Streaming**: Auto-scaling clusters with Delta Live Tables
* **Large datasets**: High-memory instances with partition tuning

## Troubleshooting

### Common Issues

1. **Connection timeouts**: Increase network timeout settings
2. **Memory errors**: Partition data appropriately
3. **Schema mismatches**: Enable schema evolution
4. **Permission errors**: Verify IAM roles and credentials

### Performance Optimization

* Use partitioned reads for large tables
* Enable predicate pushdown
* Configure appropriate parallelism
* Leverage cluster autoscaling

## Next Steps

* Review source-specific documentation
* Set up your first pipeline
* Configure monitoring and alerting
* Implement data quality checks
