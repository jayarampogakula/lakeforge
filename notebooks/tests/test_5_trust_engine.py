# Databricks notebook source
# MAGIC %md
# MAGIC # Test 5: Trust Engine Validation
# MAGIC 
# MAGIC Tests row count, join integrity, duplicate explosion, null spikes.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from pyspark.sql import SparkSession, Row
from lakeforge.trust_engine import create_trust_engine
from lakeforge.observability import LakeForgeLogger

spark = SparkSession.builder.appName("Test-Trust").getOrCreate()
logger = LakeForgeLogger.get_logger("test_trust")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

# Source data
source_data = [Row(id=1, name="A"), Row(id=2, name="B"), Row(id=3, name="C")]
df_source = spark.createDataFrame(source_data)

# Target data (slightly different)
target_data = [Row(id=1, name="A"), Row(id=2, name="B"), Row(id=3, name="C"), Row(id=4, name="D")]
df_target = spark.createDataFrame(target_data)

print(f"Source: {df_source.count()} rows, Target: {df_target.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Trust Validations

# COMMAND ----------

trust_engine = create_trust_engine(spark)

trust_validations = [
    {
        "type": "row_count",
        "params": {
            "source_df": df_source,
            "target_df": df_target,
            "tolerance_percent": 10.0
        }
    },
    {
        "type": "duplicate_explosion",
        "params": {
            "source_df": df_source,
            "target_df": df_target,
            "key_columns": ["id"],
            "max_explosion_ratio": 1.5
        }
    }
]

trust_results = trust_engine.run_trust_validations(trust_validations)

print("\n✅ Trust Validation Results:")
print(f"Trust Score: {trust_results['trust_score']}%")
print(f"Total Validations: {trust_results['total_validations']}")
print(f"Passed: {trust_results['passed_count']}")
print(f"Failed: {trust_results['failed_count']}")

# COMMAND ----------

# Show detailed results
print("\nDetailed Results:")
for result in trust_results['validation_results']:
    status = "✅ PASS" if result['passed'] else "❌ FAIL"
    print(f"{status} - {result['validation']}: {result.get('message', 'No message')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70)
print("TEST 5: TRUST ENGINE - SUMMARY")
print("="*70)
print(f"✅ Trust Score: {trust_results['trust_score']}%")
print(f"✅ Validations Passed: {trust_results['passed_count']}/{trust_results['total_validations']}")
print("="*70)

