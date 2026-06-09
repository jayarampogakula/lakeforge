# Filesystem Ingestion Guide

Load files from various filesystem sources (DBFS, Volumes, mounted storage, local FS).

## Overview

* Auto Loader for incremental file ingestion
* Support for all major file formats
* Recursive directory scanning
* Schema evolution handling

## Configuration

### Load from Unity Catalog Volumes

```python
from lakeforge.ingestion import FilesystemLoader

spark = SparkSession.builder.appName("FilesystemIngestion").getOrCreate()

loader = FilesystemLoader(spark=spark)

# Load from Volume
df = loader.load_directory(
    path="/Volumes/catalog/schema/volume/data",
    file_format="parquet"
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/volume_data")
```

### Auto Loader Pattern

```python
stream_df = loader.load_with_autoloader(
    source_path="/Volumes/catalog/schema/volume/incoming/",
    file_format="json",
    schema_location="/Volumes/catalog/schema/volume/schemas/"
)

query = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/autoloader") \
    .start("/mnt/bronze/streaming_data")
```

### Recursive Directory Scan

```python
df = loader.load_directory(
    path="/dbfs/mnt/data/nested/",
    file_format="csv",
    recursive=True,
    header=True
)
```

## Best Practices

* Use Auto Loader for production pipelines
* Enable schema inference and evolution
* Partition bronze tables by ingestion_date
* Monitor file arrival patterns
