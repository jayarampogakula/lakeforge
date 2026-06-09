# PostgreSQL Ingestion Guide

Load data from PostgreSQL databases into your Data Lakehouse.

## Configuration

```python
from lakeforge.ingestion import PostgresLoader

spark = SparkSession.builder.appName("PostgresIngestion").getOrCreate()

loader = PostgresLoader(
    spark=spark,
    host="postgres.company.com",
    port=5432,
    database="production",
    user="etl_user",
    password=dbutils.secrets.get(scope="prod", key="postgres-password")
)

# Load table with partitioning
df = loader.load_table(
    table_name="public.orders",
    num_partitions=8,
    partition_column="order_id",
    lower_bound=1,
    upper_bound=1000000
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/pg_orders")
```

## CDC Pattern

```python
# Load changes since last run
df = loader.load_cdc(
    table_name="public.orders",
    timestamp_column="updated_at",
    last_timestamp="2024-06-01 00:00:00",
    deleted_flag_column="is_deleted"
)
```
