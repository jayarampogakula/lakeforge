# Databricks notebook source
# MAGIC %md
# MAGIC # Test 3: Schema Drift Detection
# MAGIC 
# MAGIC Tests schema drift detection, scoring, and evolution strategies.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from lakeforge.metadata import create_schema_drift_detector
from lakeforge.observability import LakeForgeLogger

spark = SparkSession.builder.appName("Test-Drift").getOrCreate()
logger = LakeForgeLogger.get_logger("test_drift")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Initial Table

# COMMAND ----------

# Create initial schema
initial_data = [
    Row(customer_id=1, name="John", age=30),
    Row(customer_id=2, name="Jane", age=25)
]
df_initial = spark.createDataFrame(initial_data)

spark.sql("CREATE CATALOG IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.raw")

df_initial.write.format("delta").mode("overwrite").saveAsTable("bronze.raw.test_drift_customers")
print("✅ Initial table created")
df_initial.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Detect Schema Changes

# COMMAND ----------

# Create modified schema (added column, changed type)
modified_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),  # Changed from Int to String
    StructField("email", StringType(), True)  # New column
])

modified_data = [
    Row(customer_id=3, name="Bob", age="35", email="bob@example.com")
]
df_modified = spark.createDataFrame(modified_data, schema=modified_schema)

print("Modified schema:")
df_modified.printSchema()

# COMMAND ----------

# Run drift detection
drift_detector = create_schema_drift_detector(spark)

drift_results = drift_detector.detect_drift(
    source_df=df_modified,
    target_table="test_drift_customers",
    catalog="bronze",
    schema="raw"
)

print("\n✅ Drift Detection Results:")
print(f"Has Drift: {drift_results['has_drift']}")
print(f"Drift Score: {drift_results['drift_score']}")
print(f"Total Drifts: {drift_results['total_drifts']}")
print(f"Datatype Changes: {len(drift_results.get('datatype_changes', []))}")
print(f"New Columns: {len(drift_results.get('new_columns', []))}")

if drift_results.get('datatype_changes'):
    print("\nDatatype Changes:")
    for change in drift_results['datatype_changes']:
        print(f"  - {change}")

if drift_results.get('new_columns'):
    print("\nNew Columns:")
    for col in drift_results['new_columns']:
        print(f"  - {col}")

# COMMAND ----------

# Get evolution recommendations
recommendations = drift_detector.get_schema_evolution_strategy(drift_results)

print("\n✅ Evolution Recommendations:")
for rec in recommendations['recommendations']:
    print(f"\nType: {rec['type']}")
    print(f"Action: {rec['action']}")
    print(f"Description: {rec['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70)
print("TEST 3: SCHEMA DRIFT - SUMMARY")
print("="*70)
print(f"✅ Drift Detection: {'Drift found' if drift_results['has_drift'] else 'No drift'}")
print(f"✅ Drift Score: {drift_results['drift_score']}")
print(f"✅ Changes Detected: {drift_results['total_drifts']}")
print(f"✅ Recommendations: {len(recommendations['recommendations'])}")
print("="*70)

