"""
LakeForge Phase 1 - Complete Example Usage
Demonstrates CSV loading, Bronze writing, DQ validation, and SCD Type 2
"""

# ========================================
# 1. SETUP AND IMPORTS
# ========================================

from pyspark.sql import SparkSession
from lakeforge.observability.logger import get_logger
from lakeforge.metadata.config_parser import load_ingestion_config, load_dq_config
from lakeforge.ingestion.csv_loader import create_csv_loader
from lakeforge.bronze.bronze_writer import create_bronze_writer
from lakeforge.dq.dq_engine import create_dq_engine
from lakeforge.silver.scd_type2 import create_scd_type2_handler

# Initialize logger
logger = get_logger(name="lakeforge_example", level="INFO")
logger.info("Starting LakeForge Phase 1 example pipeline")

# Initialize Spark (automatically available in Databricks)
spark = SparkSession.builder.getOrCreate()


# ========================================
# 2. CSV INGESTION
# ========================================

logger.info("Step 1: CSV Ingestion")

# Create CSV loader
csv_loader = create_csv_loader(spark)

# Load CSV file
csv_path = "/path/to/your/sales_data.csv"
df = csv_loader.load_csv_with_metadata(
    file_path=csv_path,
    header=True,
    delimiter=",",
    encoding="utf-8",
    add_source_metadata=True
)

logger.info(f"CSV loaded successfully", rows=df.count(), columns=len(df.columns))

# Preview data
display(df.limit(10))


# ========================================
# 3. WRITE TO BRONZE LAYER
# ========================================

logger.info("Step 2: Writing to Bronze layer")

# Create Bronze writer
bronze_writer = create_bronze_writer(spark)

# Write to Bronze table with audit columns
bronze_metrics = bronze_writer.write_to_bronze(
    df=df,
    target_table="sales_raw",
    catalog="bronze",
    schema="sales",
    mode="append",
    partition_by=["_ingestion_date"],
    add_audit_columns=True,
    source_system="erp_system"
)

logger.info("Bronze write completed", **bronze_metrics)


# ========================================
# 4. DATA QUALITY VALIDATION
# ========================================

logger.info("Step 3: Running Data Quality checks")

# Create DQ engine
dq_engine = create_dq_engine(spark)

# Load DQ rules from config
dq_config_path = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/configs/dq/sample_dq_rules.yaml"
# dq_config = load_dq_config(dq_config_path)

# Define DQ rules manually for this example
dq_rules = [
    {
        "rule_name": "order_id_not_null",
        "rule_type": "null_check",
        "column": "order_id",
        "threshold": 0.0,
        "action": "quarantine",
        "severity": "error"
    },
    {
        "rule_name": "unique_order_id",
        "rule_type": "duplicate_check",
        "columns": ["order_id"],
        "allow_duplicates": False,
        "action": "quarantine",
        "severity": "error"
    },
    {
        "rule_name": "order_amount_positive",
        "rule_type": "range_check",
        "column": "order_amount",
        "min_value": 0,
        "max_value": 1000000,
        "threshold": 0.99,
        "action": "warn",
        "severity": "warning"
    }
]

# Read Bronze table for validation
bronze_df = spark.table("bronze.sales.sales_raw")

# Run validation
validation_results = dq_engine.validate_dataframe(
    df=bronze_df,
    rules=dq_rules,
    quarantine_failures=True
)

logger.info("DQ validation completed", 
           rules_passed=validation_results["rules_passed"],
           rules_failed=validation_results["rules_failed"])

# Display validation results
print(f"\nData Quality Results:")
print(f"Total Records: {validation_results['total_records']}")
print(f"Rules Executed: {validation_results['rules_executed']}")
print(f"Rules Passed: {validation_results['rules_passed']}")
print(f"Rules Failed: {validation_results['rules_failed']}")

for result in validation_results["rule_results"]:
    status = "✓" if result["passed"] else "✗"
    print(f"{status} {result['rule_name']}: {result.get('error', 'Passed')}")

# Write quarantine records if any
if validation_results.get("quarantine_df") and validation_results.get("quarantine_count", 0) > 0:
    dq_engine.write_quarantine_table(
        quarantine_df=validation_results["quarantine_df"],
        catalog="bronze",
        schema="sales",
        table_name="sales_raw_quarantine",
        validation_results=validation_results
    )
    logger.info("Quarantine records written", count=validation_results["quarantine_count"])

# Generate DQ scorecard
dq_engine.generate_scorecard(
    validation_results=validation_results,
    catalog="bronze",
    schema="sales",
    table_name="sales_raw"
)

logger.info("DQ scorecard generated")


# ========================================
# 5. SCD TYPE 2 PROCESSING
# ========================================

logger.info("Step 4: SCD Type 2 processing for customer dimension")

# Create SCD Type 2 handler
scd_handler = create_scd_type2_handler(spark)

# Simulate customer dimension source data
# In reality, this would come from your staging tables
customer_data = [
    {"customer_id": "C001", "customer_name": "John Doe", "email": "john@example.com", "city": "New York"},
    {"customer_id": "C002", "customer_name": "Jane Smith", "email": "jane@example.com", "city": "Los Angeles"},
    {"customer_id": "C003", "customer_name": "Bob Johnson", "email": "bob@example.com", "city": "Chicago"}
]

customer_df = spark.createDataFrame(customer_data)

# Merge using SCD Type 2
scd_metrics = scd_handler.merge_scd_type2(
    source_df=customer_df,
    target_table="customer_dimension",
    catalog="silver",
    schema="dimensions",
    key_columns=["customer_id"],
    tracked_columns=["customer_name", "email", "city"],
    create_if_not_exists=True
)

logger.info("SCD Type 2 merge completed", **scd_metrics)

print(f"\nSCD Type 2 Results:")
print(f"Records Inserted: {scd_metrics['records_inserted']}")
print(f"Records Updated: {scd_metrics['records_updated']}")
print(f"Records Unchanged: {scd_metrics['records_unchanged']}")

# View current customer records
current_customers = scd_handler.get_current_records(
    catalog="silver",
    schema="dimensions",
    table_name="customer_dimension"
)

display(current_customers)


# ========================================
# 6. PIPELINE SUMMARY
# ========================================

logger.info("Pipeline completed successfully!")

print("\n" + "="*60)
print("LAKEFORGE PHASE 1 PIPELINE SUMMARY")
print("="*60)
print(f"✓ CSV Ingestion: {bronze_metrics['rows_written']} rows")
print(f"✓ Bronze Layer: {bronze_metrics['table']}")
print(f"✓ DQ Validation: {validation_results['rules_passed']}/{validation_results['rules_executed']} rules passed")
print(f"✓ SCD Type 2: {scd_metrics['records_inserted']} records processed")
print("="*60)
