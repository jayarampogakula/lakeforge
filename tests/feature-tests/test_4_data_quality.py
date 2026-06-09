# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Test 4: Data Quality Validation
# MAGIC
# MAGIC Tests DQ rules, quarantine generation, and scorecards.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from pyspark.sql import SparkSession, Row
from lakeforge.dq import create_dq_engine
from lakeforge.observability import LakeForgeLogger

spark = SparkSession.builder.appName("Test-DQ").getOrCreate()
logger = LakeForgeLogger.get_logger("test_dq")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data with Quality Issues

# COMMAND ----------

# Create data with known quality issues
data = [
    Row(customer_id=1, name="John Doe", email="john@example.com", age=30),
    Row(customer_id=2, name="Jane Smith", email="invalid-email", age=25),  # Bad email
    Row(customer_id=3, name="Bob", email=None, age=35),  # Null email
    Row(customer_id=3, name="Bob Duplicate", email="bob@example.com", age=35),  # Duplicate ID
    Row(customer_id=4, name="Alice", email="alice@example.com", age=150)  # Age out of range
]
df = spark.createDataFrame(data)

print(f"Created test data with {df.count()} rows")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run DQ Validations

# COMMAND ----------

dq_engine = create_dq_engine(spark)

dq_results = dq_engine.validate_dataframe(
    df=df,
    rules=[
        {
            "rule_name": "customer_id_not_null",
            "rule_type": "null_check",
            "column": "customer_id",
            "threshold": 0.0
        },
        {
            "rule_name": "email_not_null",
            "rule_type": "null_check",
            "column": "email",
            "threshold": 10.0  # Allow 10% nulls
        },
        {
            "rule_name": "customer_id_unique",
            "rule_type": "duplicate_check",
            "columns": ["customer_id"]
        },
        {
            "rule_name": "email_format",
            "rule_type": "regex_check",
            "column": "email",
            "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
        },
        {
            "rule_name": "age_range",
            "rule_type": "range_check",
            "column": "age",
            "min_value": 18,
            "max_value": 120
        }
    ]
)

print("\n✅ DQ Validation Results:")
print(f"Rules Executed: {dq_results['rules_executed']}")
print(f"Rules Passed: {dq_results['rules_passed']}")
print(f"Rules Failed: {dq_results['rules_failed']}")

# COMMAND ----------

# Show detailed results
print("\nDetailed Results:")
for result in dq_results['rule_results']:
    status = "✅ PASS" if result['passed'] else "❌ FAIL"
    print(f"{status} - {result['rule_name']}: {result.get('message', result.get('error', 'No message'))}")

# COMMAND ----------

# Generate DQ Scorecard
scorecard = dq_engine.generate_scorecard(dq_results)

print("\n✅ DQ Scorecard:")
for key, value in scorecard.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70)
print("TEST 4: DATA QUALITY - SUMMARY")
print("="*70)
print(f"✅ Total Rules: {dq_results['rules_executed']}")
print(f"✅ Passed: {dq_results['rules_passed']}")
print(f"✅ Failed: {dq_results['rules_failed']}")
print(f"✅ DQ Score: {dq_results['rules_passed']}/{dq_results['rules_executed']}")
print("="*70)
