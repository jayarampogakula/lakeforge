"""
LakeForge Complete End-to-End Pipeline
================================================

This pipeline demonstrates all core LakeForge features:
1. CSV/Excel Ingestion
2. Bronze Layer with Audit Columns
3. Data Quality Validations
4. Schema Drift Detection
5. Trust Engine Validations
6. Trust Score Reporting (JSON + HTML)
7. SCD Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

# Initialize Spark
spark = SparkSession.builder.appName("LakeForge-EndToEnd-Pipeline").getOrCreate()

# ============================================================================
# STEP 1: Ingestion (CSV Example)
# ============================================================================

from lakeforge.ingestion import create_csv_loader
from lakeforge.bronze import create_bronze_writer
from lakeforge.observability import LakeForgeLogger

logger = LakeForgeLogger(name="lakeforge_pipeline")
logger.info("Starting LakeForge End-to-End Pipeline")

# Create sample customer data for demonstration
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sample_data = [
    (1, "john.doe@example.com", "John Doe", "USA"),
    (2, "jane.smith@example.com", "Jane Smith", "UK"),
    (3, "bob.wilson@example.com", "Bob Wilson", "Canada"),
    (4, "alice.brown@example.com", "Alice Brown", "USA"),
    (5, "charlie.davis@example.com", "Charlie Davis", "UK")
]

schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True)
])

df_customers = spark.createDataFrame(sample_data, schema)
logger.info(f"Loaded {df_customers.count()} customer records from sample data")

# ============================================================================
# STEP 2: Schema Drift Detection
# ============================================================================

from lakeforge.metadata import create_schema_drift_detector

drift_detector = create_schema_drift_detector(spark)

# Detect drift against existing bronze table
drift_results = drift_detector.detect_drift(
    source_df=df_customers,
    target_table="customers",
    catalog="bronze",
    schema="raw"
)

if drift_results.get("has_drift"):
    logger.warning(f"Schema drift detected! Drift score: {drift_results['drift_score']}")
    
    # Get evolution recommendations
    recommendations = drift_detector.get_schema_evolution_strategy(drift_results)
    logger.info(f"Evolution recommendations: {recommendations}")
    
    # Optional: Auto-evolve schema
    # evolution_result = drift_detector.auto_evolve_schema(
    #     source_df=df_customers,
    #     target_table="customers",
    #     catalog="bronze",
    #     schema="raw"
    # )
else:
    logger.info("No schema drift detected")

# Log drift results
drift_detector.log_drift(
    drift_results=drift_results,
    catalog="monitoring",
    schema="logs"
)

# ============================================================================
# STEP 3: Write to Bronze with Audit Columns
# ============================================================================

bronze_writer = create_bronze_writer(spark)

# Add audit columns
df_bronze = bronze_writer.add_audit_columns(
    df=df_customers,
    source_system="crm_system"
)

# Write to Bronze
write_metrics = bronze_writer.write_to_bronze(
    df=df_bronze,
    target_table="customers",
    catalog="bronze",
    schema="raw",
    mode="append",
    partition_columns=["_ingestion_date"],
    enable_optimize=True
)

logger.info(f"Bronze write metrics: {write_metrics}")

# Track file ingestion
bronze_writer.track_file_ingestion(
    filename="customers.csv",
    catalog="bronze",
    schema="raw"
)

# ============================================================================
# STEP 4: Data Quality Validations
# ============================================================================

from lakeforge.dq import create_dq_engine

dq_engine = create_dq_engine(spark)

# Read bronze data for DQ validation
df_bronze_read = spark.table("bronze.raw.customers")

# Run DQ validations
dq_results = dq_engine.validate_dataframe(
    df=df_bronze_read,
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
            "threshold": 5.0
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
            "pattern": "^[\w\.-]+@[\w\.-]+\.\w+$"
        }
    ]
)

logger.info(f"DQ Score: {dq_results['rules_passed']}/{dq_results['rules_executed']} passed")

# Write quarantine records if any failures
if dq_results["rules_failed"] > 0:
    quarantine_metrics = dq_engine.write_quarantine_table(
        df=df_bronze_read,
        rule_results=dq_results["rule_results"],
        catalog="bronze",
        schema="quarantine",
        table_name="customers_quarantine"
    )
    logger.warning(f"Quarantine metrics: {quarantine_metrics}")

# Generate DQ scorecard
dq_scorecard = dq_engine.generate_scorecard(dq_results)

# ============================================================================
# STEP 5: Trust Engine Validations
# ============================================================================

from lakeforge.trust_engine import create_trust_engine

trust_engine = create_trust_engine(spark)

# For this example, assume we have source and transformed data
df_source = spark.table("bronze.raw.customers")
df_silver = spark.table("silver.customers")  # After transformations

# Run trust validations
trust_validations = [
    {
        "type": "row_count",
        "params": {
            "source_df": df_source,
            "target_df": df_silver,
            "tolerance_percent": 5.0
        }
    },
    {
        "type": "duplicate_explosion",
        "params": {
            "source_df": df_source,
            "target_df": df_silver,
            "key_columns": ["customer_id"],
            "max_explosion_ratio": 1.2
        }
    },
    {
        "type": "null_spike",
        "params": {
            "df": df_silver,
            "column": "email",
            "historical_null_rate": 2.0,
            "max_spike_percent": 10.0
        }
    }
]

trust_results = trust_engine.run_trust_validations(trust_validations)

logger.info(f"Trust Score: {trust_results['trust_score']}%")
logger.info(f"Passed: {trust_results['passed_count']}/{trust_results['total_validations']}")

# Log trust results
trust_engine.log_trust_results(
    trust_results=trust_results,
    catalog="monitoring",
    schema="logs"
)

# ============================================================================
# STEP 6: Generate Trust Score Reports
# ============================================================================

from lakeforge.reporting import create_report_generator

report_gen = create_report_generator()

# Generate combined trust report
trust_report = report_gen.generate_trust_report(
    dq_results=dq_results,
    trust_results=trust_results,
    schema_drift_results=drift_results,
    pipeline_name="Customer Data Pipeline",
    report_title="Customer Pipeline Trust Report"
)

logger.info(f"Overall Trust Score: {trust_report['overall_trust_score']}%")
logger.info(f"Trust Level: {trust_report['trust_level']}")

# Save JSON report
report_gen.save_json_report(
    report=trust_report,
    output_path="/Volumes/main/default/lakeforge_reports/customer_trust_report.json"
)

# Save HTML report
report_gen.save_html_report(
    report=trust_report,
    output_path="/Volumes/main/default/lakeforge_reports/customer_trust_report.html"
)

logger.info("Trust reports generated successfully!")

# ============================================================================
# STEP 7: SCD Type 2 (Optional)
# ============================================================================

from lakeforge.silver import create_scd_type2_handler

scd_handler = create_scd_type2_handler(spark)

# Add SCD columns
df_with_scd = scd_handler.add_scd_columns(
    df=df_bronze_read,
    business_keys=["customer_id"]
)

# Merge with SCD Type 2 logic
scd_handler.merge_scd_type2(
    source_df=df_with_scd,
    target_table="customers_scd",
    catalog="silver",
    schema="dim",
    business_keys=["customer_id"]
)

logger.info("SCD Type 2 merge completed")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("✅ LAKEFORGE PIPELINE COMPLETED SUCCESSFULLY")
print("="*70)
print(f"Schema Drift Score: {drift_results.get('drift_score', 'N/A')}")
print(f"Data Quality Score: {dq_results['rules_passed']}/{dq_results['rules_executed']}")
print(f"Trust Validation Score: {trust_results['trust_score']}%")
print(f"Overall Trust Score: {trust_report['overall_trust_score']}%")
print(f"Trust Level: {trust_report['trust_level']}")
print("="*70)
