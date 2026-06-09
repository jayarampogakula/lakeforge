# Databricks notebook source
# MAGIC %md
# MAGIC # Test 6: Reporting
# MAGIC 
# MAGIC Tests JSON/HTML report generation with combined trust scores.

# COMMAND ----------

import sys
sys.path.insert(0, '/Workspace/Users/jayarampogakula@gmail.com/lakeforge')

from lakeforge.reporting import create_report_generator
from lakeforge.observability import LakeForgeLogger

logger = LakeForgeLogger.get_logger("test_reporting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Mock Results

# COMMAND ----------

# Mock DQ results
dq_results = {
    "rules_executed": 5,
    "rules_passed": 4,
    "rules_failed": 1,
    "rule_results": [
        {"rule_name": "id_not_null", "passed": True, "message": "No nulls found"},
        {"rule_name": "email_format", "passed": False, "message": "2 invalid emails"}
    ]
}

# Mock Trust results
trust_results = {
    "trust_score": 85.0,
    "total_validations": 3,
    "passed_count": 2,
    "failed_count": 1,
    "validation_results": [
        {"validation": "row_count", "passed": True, "message": "Within tolerance"},
        {"validation": "duplicate_check", "passed": False, "message": "Duplicates found"}
    ]
}

# Mock Drift results
drift_results = {
    "has_drift": False,
    "drift_score": 1.0,
    "total_drifts": 0
}

print("✅ Mock results created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Trust Report

# COMMAND ----------

report_gen = create_report_generator()

trust_report = report_gen.generate_trust_report(
    dq_results=dq_results,
    trust_results=trust_results,
    schema_drift_results=drift_results,
    pipeline_name="Test Pipeline",
    report_title="Test Trust Report"
)

print("\n✅ Trust Report Generated:")
print(f"Overall Trust Score: {trust_report['overall_trust_score']}%")
print(f"Trust Level: {trust_report['trust_level']}")
print(f"Total Checks: {trust_report['summary']['total_checks']}")
print(f"Passed Checks: {trust_report['summary']['passed_checks']}")
print(f"Failed Checks: {trust_report['summary']['failed_checks']}")

# COMMAND ----------

# Save reports
import json

# Save JSON
json_path = "/dbfs/tmp/lakeforge/test_trust_report.json"
with open(json_path, 'w') as f:
    json.dump(trust_report, f, indent=2)
print(f"✅ JSON report saved: {json_path}")

# Save HTML
html_path = "/dbfs/tmp/lakeforge/test_trust_report.html"
report_gen.save_html_report(trust_report, html_path)
print(f"✅ HTML report saved: {html_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70)
print("TEST 6: REPORTING - SUMMARY")
print("="*70)
print(f"✅ Overall Trust Score: {trust_report['overall_trust_score']}%")
print(f"✅ Trust Level: {trust_report['trust_level']}")
print(f"✅ JSON Report: Generated")
print(f"✅ HTML Report: Generated")
print("="*70)

