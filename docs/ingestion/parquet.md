# Parquet File Ingestion Guide

Load Parquet files with schema evolution support.

## Configuration

```python
from lakeforge.ingestion import ParquetLoader

spark = SparkSession.builder.appName("ParquetIngestion").getOrCreate()

loader = ParquetLoader(spark=spark)

# Load Parquet files
df = loader.load(
    path="/mnt/data/orders/*.parquet",
    merge_schema=True
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/parquet_orders")
```

## Partitioned Data

```python
df = loader.load_partitioned(
    base_path="/mnt/data/events",
    partition_filter="year=2024/month=06"
)
```

## Best Practices

* Enable schema merging for evolving schemas
* Use predicate pushdown for filtered reads
* Leverage columnar format benefits
* Partition by date for time-series data
