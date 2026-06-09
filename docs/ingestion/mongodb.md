# MongoDB Ingestion Guide

Load data from MongoDB collections.

## Configuration

```python
from lakeforge.ingestion import MongoDBLoader

spark = SparkSession.builder.appName("MongoDBIngestion").getOrCreate()

loader = MongoDBLoader(
    spark=spark,
    connection_string="mongodb://host:27017",
    database="analytics"
)

# Load collection
df = loader.load_collection(collection="events")

# Load with aggregation pipeline
pipeline = [
    {"$match": {"status": "active"}},
    {"$project": {"_id": 1, "name": 1, "created_at": 1}}
]

df = loader.load_collection(
    collection="users",
    pipeline=pipeline
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/mongo_users")
```

## Best Practices

* Use aggregation pipelines for filtering
* Leverage MongoDB indexes
* Handle nested documents appropriately
* Monitor change streams for real-time data
