# Google Cloud Storage Ingestion Guide

Load files from GCS buckets.

## Configuration

```python
from lakeforge.ingestion import GCSLoader

spark = SparkSession.builder.appName("GCSIngestion").getOrCreate()

loader = GCSLoader(
    spark=spark,
    credentials_path="/dbfs/secrets/gcp-service-account.json"
)

# Load files
df = loader.load(
    path="gs://my-bucket/data/orders/",
    file_format="parquet"
)

# Load CSV
df = loader.load_csv(
    path="gs://my-bucket/data/customers.csv",
    header=True
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/gcs_data")
```
