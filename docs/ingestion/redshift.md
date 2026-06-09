# AWS Redshift Ingestion Guide

Load data from Amazon Redshift into your Data Lakehouse with optimized UNLOAD operations.

## Overview

* Optimized data transfer via S3 UNLOAD
* Support for IAM role authentication
* Parallel data loading
* Query pushdown capabilities

## Prerequisites

* Redshift cluster endpoint and credentials
* S3 bucket for temporary data staging
* IAM role with Redshift and S3 permissions
* Redshift JDBC driver

## Configuration

### Basic Setup

```python
from lakeforge.ingestion import RedshiftLoader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RedshiftIngestion").getOrCreate()

loader = RedshiftLoader(
    spark=spark,
    host="my-cluster.abc123.us-east-1.redshift.amazonaws.com",
    port=5439,
    database="analytics",
    user="etl_user",
    password=dbutils.secrets.get(scope="prod", key="redshift-password"),
    temp_s3_bucket="s3a://my-bucket/temp/redshift/",
    iam_role="arn:aws:iam::123456789012:role/RedshiftUnloadRole"
)

# Load table
df = loader.load_table("public.orders")

# Write to Bronze
df.write.format("delta").mode("overwrite").save("/mnt/bronze/redshift_orders")
```

### Using Custom Query

```python
query = """
    SELECT 
        o.order_id,
        o.order_date,
        o.total_amount,
        c.customer_name,
        p.product_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= '2024-01-01'
"""

df = loader.load_query(query)
```

## IAM Role Setup

### Create IAM Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/temp/redshift/*",
        "arn:aws:s3:::my-bucket"
      ]
    }
  ]
}
```

### Attach Role to Redshift

```sql
-- In Redshift
GRANT USAGE ON SCHEMA public TO ROLE my_redshift_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE my_redshift_role;
```

## Bronze Layer Pattern

```python
from pyspark.sql.functions import current_timestamp, lit

bronze_df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("REDSHIFT"))

bronze_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("order_date") \
    .save("/mnt/bronze/orders")
```

## Performance Optimization

* Use UNLOAD for large tables (>1M rows)
* Set appropriate `maxlimit` for query results
* Enable query result caching
* Partition bronze tables by date

## Troubleshooting

* **Connection timeout**: Check security groups and VPC settings
* **S3 access denied**: Verify IAM role permissions
* **Slow performance**: Use UNLOAD instead of direct JDBC
