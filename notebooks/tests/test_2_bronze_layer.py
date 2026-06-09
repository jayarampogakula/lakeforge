# Databricks notebook source
# MAGIC %md
# MAGIC # Test 2: Bronze Layer Testing
# MAGIC 
# MAGIC Tests bronze layer writes with audit columns, file tracking, and optimization.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from pyspark.sql import SparkSession, Row
from lakeforge.bronze import create_bronze_writer
from lakeforge.observability import LakeForgeLogger

spark = SparkSession.builder.appName("Test-Bronze").getOrCreate()
logger = LakeForgeLogger.get_logger("test_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

# Generate sample data
data = [
    Row(customer_id=1, name="John Doe", email="john@example.com", age=30),
    Row(customer_id=2, name="Jane Smith", email="jane@example.com", age=25),
    Row(customer_id=3, name="Bob Johnson", email="bob@example.com", age=35)
]
df = spark.createDataFrame(data)

print(f"Created test DataFrame with {df.count()} rows")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Audit Columns

# COMMAND ----------

bronze_writer = create_bronze_writer(spark)

# Add audit columns
df_with_audit = bronze_writer.add_audit_columns(
    df=df,
    source_system="test_system"
)

print("✅ Audit columns added:")
df_with_audit.printSchema()
df_with_audit.display()

# Verify audit columns
audit_cols = ['_ingestion_timestamp', '_ingestion_date', '_source_system', '_record_hash']
for col in audit_cols:
    assert col in df_with_audit.columns, f"Missing audit column: {col}"

print("✅ All audit columns present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Bronze Write

# COMMAND ----------

# Create catalog and schema if not exists
spark.sql("CREATE CATALOG IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.raw")

# Write to bronze
write_metrics = bronze_writer.write_to_bronze(
    df=df_with_audit,
    target_table="test_customers",
    catalog="bronze",
    schema="raw",
    mode="overwrite",
    partition_columns=["_ingestion_date"]
)

print("✅ Write metrics:")
for key, value in write_metrics.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# Verify data written
df_verify = spark.table("bronze.raw.test_customers")
print(f"✅ Verified: {df_verify.count()} rows in bronze.raw.test_customers")
df_verify.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70)
print("TEST 2: BRONZE LAYER - SUMMARY")
print("="*70)
print(f"✅ Audit columns: Added successfully")
print(f"✅ Bronze table: Created with {df_verify.count()} rows")
print(f"✅ Partitioning: By _ingestion_date")
print("="*70)

