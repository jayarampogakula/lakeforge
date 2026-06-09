# LakeForge - Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Step 1: Setup Catalogs

```sql
-- Run these SQL commands first
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS monitoring;

CREATE SCHEMA IF NOT EXISTS bronze.raw;
CREATE SCHEMA IF NOT EXISTS bronze.quarantine;
CREATE SCHEMA IF NOT EXISTS silver.dim;
CREATE SCHEMA IF NOT EXISTS monitoring.logs;
```

### Step 2: Install Dependencies

```python
# In a Databricks notebook
%pip install -r /Workspace/Users/<your-email>/lakeforge/requirements.txt
dbutils.library.restartPython()
```

### Step 3: Run Tests

Open and run these notebooks in order:

1. **`notebooks/tests/test_1_ingestion.py`** - Test data ingestion
2. **`notebooks/tests/test_2_bronze_layer.py`** - Test bronze writes
3. **`notebooks/tests/test_3_schema_drift.py`** - Test drift detection
4. **`notebooks/tests/test_4_data_quality.py`** - Test DQ validations
5. **`notebooks/tests/test_5_trust_engine.py`** - Test trust checks
6. **`notebooks/tests/test_6_reporting.py`** - Test reporting
7. **`notebooks/tests/test_7_scd_type2.py`** - Test SCD Type 2

### Step 4: Run End-to-End Pipeline

Open and run: **`notebooks/demo/end_to_end_pipeline.py`**

This orchestrates all components together in a complete pipeline.

---

## 📚 Documentation

- **[README.md](../README.md)** - Project overview and features
- **[TESTING_GUIDE.md](testing/TESTING_GUIDE.md)** - Comprehensive testing guide
- **[PHASE1_GUIDE.md](getting-started/PHASE1_GUIDE.md)** - Phase 1 detailed guide
- **[PHASE1_SUMMARY.txt](../PHASE1_SUMMARY.txt)** - Complete feature list

---

## 🧪 Testing Checklist

After running all tests, verify:

- [ ] All catalogs created (bronze, silver, monitoring)
- [ ] Test data generated successfully
- [ ] Bronze tables written with audit columns
- [ ] Schema drift detected and logged
- [ ] DQ validations passed/failed correctly
- [ ] Trust score calculated
- [ ] HTML report generated
- [ ] SCD history tracked

---

## ⚡ Common Commands

### Import LakeForge

```python
import sys
sys.path.insert(0, '/Workspace/Users/<your-email>/lakeforge')

from lakeforge import *
```

### Quick Ingestion

```python
from lakeforge.ingestion import create_csv_loader

csv_loader = create_csv_loader(spark)
df = csv_loader.load_csv("/path/to/file.csv")
```

### Quick Bronze Write

```python
from lakeforge.bronze import create_bronze_writer

bronze_writer = create_bronze_writer(spark)
df_with_audit = bronze_writer.add_audit_columns(df, source_system="my_system")
bronze_writer.write_to_bronze(
    df=df_with_audit,
    target_table="my_table",
    catalog="bronze",
    schema="raw"
)
```

### Quick DQ Validation

```python
from lakeforge.dq import create_dq_engine

dq_engine = create_dq_engine(spark)
dq_results = dq_engine.validate_dataframe(
    df=df,
    rules=[
        {"rule_name": "id_not_null", "rule_type": "null_check", "column": "id"}
    ]
)
```

### Quick Trust Report

```python
from lakeforge.reporting import create_report_generator

report_gen = create_report_generator()
report = report_gen.generate_trust_report(
    dq_results=dq_results,
    trust_results=trust_results,
    pipeline_name="My Pipeline"
)
print(f"Trust Score: {report['overall_trust_score']}%")
```

---

## 🐛 Troubleshooting

### Import Error

If you see `ImportError: cannot import name ...`:

```python
# Restart Python and add path
dbutils.library.restartPython()

import sys
sys.path.insert(0, '/Workspace/Users/<your-email>/lakeforge')
```

### Catalog Not Found

```sql
-- Create missing catalogs
CREATE CATALOG IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS bronze.raw;
```

### Table Already Exists

```python
# Use overwrite mode
bronze_writer.write_to_bronze(df, mode="overwrite", ...)

# Or drop first
spark.sql("DROP TABLE IF EXISTS bronze.raw.my_table")
```

---

## 📞 Need Help?

1. Check [TESTING_GUIDE.md](testing/TESTING_GUIDE.md) for detailed procedures
2. Review example notebooks in `notebooks/tests/`
3. Check [README.md](../README.md) for architecture overview

---

**Ready to build production pipelines! 🚀**
