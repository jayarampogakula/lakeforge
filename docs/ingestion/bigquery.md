# Google BigQuery Ingestion Guide

Load data from Google BigQuery into your Data Lakehouse.

## Overview

* Direct BigQuery connector for Spark
* Server-side filtering and aggregation
* Automatic schema detection
* Partition pruning support

## Prerequisites

* GCP project with BigQuery API enabled
* Service account with BigQuery Data Viewer role
* Service account JSON key file
* Temporary GCS bucket

## Configuration

### Setup Service Account

```python
from lakeforge.ingestion import BigQueryLoader

spark = SparkSession.builder.appName("BigQueryIngestion").getOrCreate()

loader = BigQueryLoader(
    spark=spark,
    project_id="my-gcp-project",
    credentials_path="/dbfs/secrets/gcp-service-account.json",
    temp_gcs_bucket="my-temp-bucket"
)

# Load table
df = loader.load_table(
    table="dataset.table_name",
    filter_clause="date >= '2024-01-01'"
)

# Write to Bronze
df.write.format("delta").mode("overwrite").save("/mnt/bronze/bq_data")
```

### Load with SQL Query

```python
query = """
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM `my-project.sales.orders`
    WHERE order_date >= '2024-01-01'
    AND status = 'completed'
"""

df = loader.load_query(query)
```

### Partitioned Table

```python
# Load specific partition
df = loader.load_partitioned_table(
    table="my-project.sales.orders",
    partition_field="order_date",
    partition_filter="2024-01-01"
)
```

## GCP Service Account Setup

```bash
# Create service account
gcloud iam service-accounts create databricks-bigquery \
    --display-name="Databricks BigQuery Loader"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:databricks-bigquery@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

# Create and download key
gcloud iam service-accounts keys create service-account.json \
    --iam-account=databricks-bigquery@my-project.iam.gserviceaccount.com
```

## Best Practices

* Use server-side filtering for large tables
* Leverage BigQuery's columnar storage
* Partition bronze tables appropriately
* Monitor BigQuery slot usage
