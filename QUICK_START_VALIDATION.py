# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 LakeForge - Quick Start Validation Guide
# MAGIC 
# MAGIC ## Your Complete Guide to Proving LakeForge Works
# MAGIC 
# MAGIC This notebook is your command center for running the complete LakeForge validation suite.
# MAGIC 
# MAGIC **Time Required:** 50 minutes total
# MAGIC 
# MAGIC **What You'll Prove:**
# MAGIC * ✅ LakeForge catches join explosions automatically
# MAGIC * ✅ LakeForge detects schema drift in real-time
# MAGIC * ✅ LakeForge alerts on null spikes
# MAGIC * ✅ LakeForge validates referential integrity
# MAGIC * ✅ LakeForge generates trust scores for every pipeline
# MAGIC 
# MAGIC **Result:** Production-ready ETL framework + LinkedIn-worthy demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prerequisites Check

# COMMAND ----------

import sys

# Check Python version
python_version = sys.version_info
print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
assert python_version >= (3, 8), "❌ Python 3.8+ required"
print("✅ Python version OK")

# Check Spark
try:
    print(f"Spark version: {spark.version}")
    print("✅ Spark available")
except:
    print("❌ Spark not available")

# Check LakeForge
sys.path.append("/Workspace/Users/jayarampogakula@gmail.com/lakeforge")
try:
    from lakeforge import (
        create_csv_loader,
        create_bronze_writer,
        create_dq_engine,
        create_trust_engine
    )
    print("✅ LakeForge modules available")
except ImportError as e:
    print(f"❌ LakeForge import failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Catalog Setup

# COMMAND ----------

# Create catalog structure
CATALOG = "lakeforge_dev"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")

print(f"✅ Catalog structure ready: {CATALOG}")
print(f"   - {CATALOG}.bronze")
print(f"   - {CATALOG}.silver")
print(f"   - {CATALOG}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Directory Setup

# COMMAND ----------

import os

# Create data directory
DATA_DIR = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/data"
os.makedirs(DATA_DIR, exist_ok=True)
print(f"✅ Data directory: {DATA_DIR}")

# Create reports directory
REPORTS_DIR = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/reports"
os.makedirs(REPORTS_DIR, exist_ok=True)
print(f"✅ Reports directory: {REPORTS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Execution Roadmap
# MAGIC 
# MAGIC ### Run These Notebooks in Order:
# MAGIC 
# MAGIC 1. **Generate_Test_Datasets** (5 min)
# MAGIC    * Creates 7 CSV files (2 clean + 5 corrupted)
# MAGIC    * Location: `/lakeforge/tests/Generate_Test_Datasets`
# MAGIC    
# MAGIC 2. **BRONZE_Ingestion_Pipeline** (5 min)
# MAGIC    * CSV → Bronze Delta tables
# MAGIC    * Expected Trust Score: 90-95%
# MAGIC    * Location: `/lakeforge/pipelines/BRONZE_Ingestion_Pipeline`
# MAGIC    
# MAGIC 3. **SILVER_Transformation_Pipeline** (10 min) ⭐
# MAGIC    * Bronze → Silver with JOIN VALIDATION
# MAGIC    * Expected Trust Score: 85-90%
# MAGIC    * Location: `/lakeforge/pipelines/SILVER_Transformation_Pipeline`
# MAGIC    
# MAGIC 4. **GOLD_Aggregation_Pipeline** (10 min)
# MAGIC    * Silver → Gold metrics
# MAGIC    * Expected Trust Score: 90-95%
# MAGIC    * Location: `/lakeforge/pipelines/GOLD_Aggregation_Pipeline`
# MAGIC    
# MAGIC 5. **CHAOS_ETL_Disasters_Demo** (15 min) 🔥
# MAGIC    * 6 intentional disasters
# MAGIC    * Expected Trust Score: 10-30% (all caught!)
# MAGIC    * Location: `/lakeforge/tests/CHAOS_ETL_Disasters_Demo`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Success Criteria Tracker

# COMMAND ----------

# Track validation progress
success_criteria = {
    "Schema Drift Detection": "❓ Not Tested",
    "Join Explosion Detection": "❓ Not Tested",
    "Null Spike Detection": "❓ Not Tested",
    "Duplicate Key Detection": "❓ Not Tested",
    "Row Count Anomaly Detection": "❓ Not Tested",
    "Anti-Join Detection": "❓ Not Tested",
    "Bronze Tables Created": "❓ Not Tested",
    "Silver SCD Type 2": "❓ Not Tested",
    "Gold Aggregations": "❓ Not Tested",
    "Trust Report Generated": "❓ Not Tested"
}

def show_progress():
    print("=" * 70)
    print("LAKEFORGE VALIDATION PROGRESS")
    print("=" * 70)
    for criteria, status in success_criteria.items():
        print(f"{status} {criteria}")
    print("=" * 70)
    
    passed = sum(1 for s in success_criteria.values() if "✅" in s)
    total = len(success_criteria)
    pct = passed/total*100 if total > 0 else 0
    print(f"Progress: {passed}/{total} ({pct:.0f}%)")
    print("=" * 70)

show_progress()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Expected Outputs
# MAGIC 
# MAGIC After running all pipelines, you should have:
# MAGIC 
# MAGIC ### Delta Tables
# MAGIC * `lakeforge_dev.bronze.customers` (1000 rows + audit columns)
# MAGIC * `lakeforge_dev.bronze.transactions` (5000 rows + audit columns)
# MAGIC * `lakeforge_dev.silver.customer_transactions` (with SCD Type 2)
# MAGIC * `lakeforge_dev.gold.customer_metrics` (customer aggregations)
# MAGIC 
# MAGIC ### Trust Reports
# MAGIC * `/reports/bronze_trust_report.html`
# MAGIC * `/reports/silver_trust_report.html`
# MAGIC * `/reports/gold_trust_report.html`
# MAGIC * `/reports/disaster_simulation.html` (the showcase!)
# MAGIC 
# MAGIC ### Quarantine Tables
# MAGIC * `lakeforge_dev.bronze.customers_quarantine`
# MAGIC * `lakeforge_dev.bronze.transactions_quarantine`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: GitHub CSV Loading (Optional)
# MAGIC 
# MAGIC If you want to load CSV files from GitHub instead of generating locally:

# COMMAND ----------

# Example: Load from GitHub
import pandas as pd

def load_from_github(repo, branch, filepath):
    """Load CSV from GitHub raw URL"""
    url = f"https://raw.githubusercontent.com/{repo}/{branch}/{filepath}"
    df = pd.read_csv(url)
    return spark.createDataFrame(df)

# Example usage (uncomment when you have data in GitHub):
# customers_df = load_from_github("your-username/lakeforge-test-data", "main", "data/good_customers.csv")
# display(customers_df)

print("✅ GitHub CSV loading function defined")
print("   Use load_from_github(repo, branch, filepath) to load data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Demo Preparation Checklist
# MAGIC 
# MAGIC Before creating your LinkedIn demo:
# MAGIC 
# MAGIC * ☐ Run all 6 chaos scenarios
# MAGIC * ☐ Screenshot trust score dashboard
# MAGIC * ☐ Capture "6/6 disasters detected" output
# MAGIC * ☐ Export HTML trust report
# MAGIC * ☐ Document trust score progression: 95% → 15%
# MAGIC * ☐ Prepare 3-slide deck
# MAGIC * ☐ Write LinkedIn post
# MAGIC 
# MAGIC ### Suggested LinkedIn Post:
# MAGIC 
# MAGIC ```
# MAGIC We intentionally broke our data pipeline in 6 catastrophic ways.
# MAGIC LakeForge caught every single failure automatically.
# MAGIC No manual SQL debugging needed.
# MAGIC 
# MAGIC ✅ Join explosion (25x growth)
# MAGIC ✅ Schema drift (decimal → string)
# MAGIC ✅ Null spike (80% nulls)
# MAGIC ✅ Duplicate keys
# MAGIC ✅ Row count anomaly (99% data loss)
# MAGIC ✅ Referential integrity failure
# MAGIC 
# MAGIC This is what enterprise ETL monitoring should look like.
# MAGIC 
# MAGIC #DataEngineering #Databricks #DataQuality #ETL #OpenSource
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Ready to Start!
# MAGIC 
# MAGIC You're all set. Now run the notebooks in order:
# MAGIC 
# MAGIC 1. Generate_Test_Datasets
# MAGIC 2. BRONZE_Ingestion_Pipeline
# MAGIC 3. SILVER_Transformation_Pipeline
# MAGIC 4. GOLD_Aggregation_Pipeline
# MAGIC 5. CHAOS_ETL_Disasters_Demo (the showpiece!)
# MAGIC 
# MAGIC **Good luck! 🚀**

