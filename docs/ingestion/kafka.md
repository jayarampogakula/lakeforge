# Kafka Ingestion Guide

Stream data from Apache Kafka topics into your Data Lakehouse.

## Overview

The Kafka loader supports:
* Real-time streaming ingestion
* Multiple serialization formats (JSON, Avro, String)
* Batch and streaming modes
* Configurable consumer options

## Prerequisites

* Kafka cluster access
* Bootstrap server addresses
* Topic permissions
* (Optional) Schema registry for Avro

## Configuration

### Basic Streaming

```python
from lakeforge.ingestion import KafkaLoader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaIngestion").getOrCreate()

# Initialize loader
loader = KafkaLoader(spark=spark)

# Stream from Kafka
stream_df = loader.load_stream(
    kafka_bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092",
    topic="orders",
    value_format="json",
    starting_offsets="earliest"
)

# Display schema
stream_df.printSchema()
```

### JSON Message Parsing

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema for JSON messages
json_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType())
])

# Parse JSON values
parsed_df = stream_df.select(
    col("key"),
    from_json(col("value"), json_schema).alias("data"),
    col("timestamp")
).select("key", "data.*", "timestamp")
```

### Writing to Bronze Layer

```python
from pyspark.sql.functions import current_timestamp, date_format

# Add metadata columns
bronze_df = parsed_df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd"))

# Write to Delta table
query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/kafka_orders") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_date") \
    .start("/mnt/bronze/kafka_orders")

query.awaitTermination()
```

## Advanced Configuration

### Consumer Options

```python
kafka_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";',
    "maxOffsetsPerTrigger": "10000"
}

stream_df = loader.load_stream(
    kafka_bootstrap_servers="kafka:9092",
    topic="events",
    kafka_options=kafka_options
)
```

### Multiple Topics

```python
# Subscribe to multiple topics
multi_topic_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders,payments,shipments") \
    .load()
```

### Batch Mode (Bounded Read)

```python
# Load specific offset range
batch_df = loader.load_batch(
    kafka_bootstrap_servers="kafka:9092",
    topic="orders",
    starting_offsets="earliest",
    ending_offsets="latest"
)
```

## Troubleshooting

### Connection Issues
* Verify bootstrap server addresses
* Check firewall rules and security groups
* Validate SASL/SSL configuration

### Performance Optimization
* Adjust `maxOffsetsPerTrigger` for throughput control
* Use appropriate trigger intervals
* Enable partition pruning
* Monitor consumer lag

### Schema Evolution
* Enable `mergeSchema` option
* Use schema registry for Avro
* Handle null values appropriately

## Best Practices

1. **Checkpointing**: Always set checkpoint location for fault tolerance
2. **Idempotency**: Design for at-least-once delivery semantics
3. **Monitoring**: Track consumer lag and processing latency
4. **Partitioning**: Partition bronze tables by date for efficient queries
5. **Error Handling**: Implement dead-letter queues for failed messages
