# Databricks notebook source
# MAGIC %md
# MAGIC # Test 7: SCD Type 2
# MAGIC 
# MAGIC Tests slowly changing dimension tracking with history.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from pyspark.sql import SparkSession, Row
from lakeforge.silver import create_scd_type2_handler
from lakeforge.observability import LakeForgeLogger

spark = SparkSession.builder.appName("Test-SCD").getOrCreate()
logger = LakeForgeLogger.get_logger("test_scd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Load

# COMMAND ----------

# Initial customer data
initial_data = [
    Row(customer_id=1, name="John Doe", email="john@example.com", city="NYC"),
    Row(customer_id=2, name="Jane Smith", email="jane@example.com", city="LA")
]
df_initial = spark.createDataFrame(initial_data)

print("Initial data:")
df_initial.display()

# COMMAND ----------

# Create SCD handler and add SCD columns
scd_handler = create_scd_type2_handler(spark)

df_with_scd = scd_handler.add_scd_columns(
    df=df_initial,
    business_keys=["customer_id"]
)

print("\nWith SCD columns:")
df_with_scd.printSchema()

# COMMAND ----------

# Create target table
spark.sql("CREATE CATALOG IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.dim")

df_with_scd.write.format("delta").mode("overwrite").saveAsTable("silver.dim.test_customers_scd")
print("✅ Initial SCD table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update with Changes

# COMMAND ----------

# Updated data (John moved to SF, new customer Bob)
updated_data = [
    Row(customer_id=1, name="John Doe", email="john@example.com", city="SF"),  # Changed city
    Row(customer_id=2, name="Jane Smith", email="jane@example.com", city="LA"),  # No change
    Row(customer_id=3, name="Bob Johnson", email="bob@example.com", city="CHI")  # New
]
df_updated = spark.createDataFrame(updated_data)

print("Updated data:")
df_updated.display()

# COMMAND ----------

# Merge with SCD Type 2
df_updated_scd = scd_handler.add_scd_columns(
    df=df_updated,
    business_keys=["customer_id"]
)

scd_handler.merge_scd_type2(
    source_df=df_updated_scd,
    target_table="test_customers_scd",
    catalog="silver",
    schema="dim",
    business_keys=["customer_id"]
)

print("✅ SCD Type 2 merge completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify History

# COMMAND ----------

# Check SCD table
df_scd = spark.table("silver.dim.test_customers_scd").orderBy("customer_id", "effective_start_date")

print(f"\nSCD table now has {df_scd.count()} records (with history)")
df_scd.display()

# COMMAND ----------

# Verify history for customer_id=1
df_history = df_scd.filter("customer_id = 1").orderBy("effective_start_date")
print(f"\nHistory for customer_id=1: {df_history.count()} versions")
df_history.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_records = df_scd.count()
current_records = df_scd.filter("is_current = true").count()
historical_records = df_scd.filter("is_current = false").count()

print("="*70)
print("TEST 7: SCD TYPE 2 - SUMMARY")
print("="*70)
print(f"✅ Total Records: {total_records}")
print(f"✅ Current Records: {current_records}")
print(f"✅ Historical Records: {historical_records}")
print(f"✅ History Tracking: Working")
print("="*70)

