# LakeForge Phase 1 - Testing Guide

## 🎯 Overview

This guide provides comprehensive testing procedures for all LakeForge Phase 1 components.

---

## 📁 Testing Structure

```
lakeforge/
├── notebooks/
│   ├── tests/                    # Individual component tests
│   │   ├── test_1_ingestion.py
│   │   ├── test_2_bronze_layer.py
│   │   ├── test_3_schema_drift.py
│   │   ├── test_4_data_quality.py
│   │   ├── test_5_trust_engine.py
│   │   ├── test_6_reporting.py
│   │   └── test_7_scd_type2.py
│   └── demo/
│       └── end_to_end_pipeline.py    # Full pipeline orchestration
```

---

## 🚀 Quick Start

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Catalogs created**:
   - `bronze` - Raw data layer
   - `silver` - Curated data layer
   - `monitoring` - Logs and metrics
3. **Sample data** (or use provided test data generators)

### Installation

```python
# Install LakeForge dependencies
%pip install -r /Workspace/path/to/lakeforge/requirements.txt
dbutils.library.restartPython()
```

---

## 📊 Component Testing (Stage-by-Stage)

### Stage 1: Ingestion Testing

**File**: `notebooks/tests/test_1_ingestion.py`

**What it tests**:
- CSV ingestion with encoding detection
- Excel ingestion with multi-sheet support
- API ingestion with pagination

**How to run**:
1. Open the notebook
2. Set your data paths in the configuration section
3. Run all cells
4. Verify DataFrames are loaded successfully

**Expected outputs**:
- DataFrames with source data
- Metadata columns added (`_ingestion_timestamp`, `_source_file`)
- Sample data displayed

---

### Stage 2: Bronze Layer Testing

**File**: `notebooks/tests/test_2_bronze_layer.py`

**What it tests**:
- Audit column generation
- Delta table writes
- File tracking
- Partitioning
- OPTIMIZE and Z-ORDER

**Prerequisites**:
- Catalog `bronze` and schema `raw` must exist
- Source DataFrame from Stage 1

**How to run**:
1. Run ingestion test first (or load sample data)
2. Run bronze layer test
3. Verify tables created: `bronze.raw.customers`
4. Check file tracker table exists

**Expected outputs**:
- Bronze table with audit columns
- File tracker entries
- Write metrics (rows written, files created)

---

### Stage 3: Schema Drift Detection

**File**: `notebooks/tests/test_3_schema_drift.py`

**What it tests**:
- Datatype change detection
- New/deleted column detection
- Nullable change detection
- Drift scoring
- Auto-evolution strategies

**How to run**:
1. Create initial bronze table (Stage 2)
2. Load modified source data with schema changes
3. Run drift detection
4. Review drift report

**Expected outputs**:
- Drift detection results JSON
- List of schema changes
- Evolution recommendations
- Drift log entries

---

### Stage 4: Data Quality Validation

**File**: `notebooks/tests/test_4_data_quality.py`

**What it tests**:
- Null checks
- Duplicate detection
- Regex validation
- Range validation
- Custom SQL validation
- Quarantine table generation

**How to run**:
1. Load data with known quality issues
2. Configure DQ rules
3. Run validation
4. Review DQ scorecard

**Expected outputs**:
- DQ scorecard (pass/fail counts)
- Quarantine table with failed records
- Rule-by-rule results

---

### Stage 5: Trust Engine Validation

**File**: `notebooks/tests/test_5_trust_engine.py`

**What it tests**:
- Row count validation
- Join integrity
- Duplicate explosion detection
- Anti-join mismatch
- Null spike detection

**How to run**:
1. Prepare source and target DataFrames
2. Configure trust validations
3. Run trust engine
4. Review trust score

**Expected outputs**:
- Trust score (0-100%)
- Validation results per check
- Trust log entries

---

### Stage 6: Reporting

**File**: `notebooks/tests/test_6_reporting.py`

**What it tests**:
- JSON report generation
- HTML report generation
- Combined DQ + Trust + Drift scoring
- Trust level calculation

**How to run**:
1. Run DQ, Trust, and Drift tests first
2. Pass results to report generator
3. Generate reports
4. Open HTML report in browser

**Expected outputs**:
- JSON report file
- HTML report file with styling
- Overall trust score
- Trust level (EXCELLENT/GOOD/etc.)

---

### Stage 7: SCD Type 2

**File**: `notebooks/tests/test_7_scd_type2.py`

**What it tests**:
- SCD column generation
- Hash-based change detection
- History tracking
- Current/expired record management

**How to run**:
1. Load initial dimension data
2. Run SCD Type 2 merge
3. Load changed dimension data
4. Run merge again
5. Verify history tracking

**Expected outputs**:
- SCD table with version history
- `effective_start_date`, `effective_end_date`, `is_current` columns
- Historical records preserved

---

## 🔄 End-to-End Pipeline Testing

**File**: `notebooks/demo/end_to_end_pipeline.py`

**What it does**:
Orchestrates all stages in sequence:
1. Ingestion → 2. Schema Drift → 3. Bronze → 4. DQ → 5. Trust → 6. Reporting → 7. SCD

**How to run**:
1. Configure all parameters at the top
2. Run all cells sequentially
3. Monitor progress in logs
4. Review final trust report

**Expected outputs**:
- Complete pipeline execution
- All intermediate tables created
- Final trust report with overall score

---

## 🧪 Sample Data

### Option 1: Use Provided Test Data Generators

Each test notebook includes a section to generate synthetic test data.

### Option 2: Use Your Own Data

1. Place CSV/Excel files in `/dbfs/lakeforge/data/`
2. Update file paths in configuration sections
3. Run tests

### Option 3: Sample Customer Data

```python
# Generate sample customer data
from pyspark.sql import Row
from datetime import datetime

data = [
    Row(customer_id=1, name="John Doe", email="john@example.com", age=30, country="USA"),
    Row(customer_id=2, name="Jane Smith", email="jane@example.com", age=25, country="UK"),
    Row(customer_id=3, name="Bob Johnson", email="bob@example.com", age=35, country="Canada")
]

df = spark.createDataFrame(data)
```

---

## ✅ Verification Checklist

After running all tests, verify:

- [ ] All bronze tables exist (`bronze.raw.*`)
- [ ] All silver tables exist (`silver.*`)
- [ ] Monitoring tables exist (`monitoring.logs.*`)
- [ ] File tracker entries present
- [ ] DQ scorecard shows results
- [ ] Trust score calculated
- [ ] HTML report generated
- [ ] SCD history tracked

---

## 🐛 Troubleshooting

### Import Errors

```python
# If you see ModuleNotFoundError
import sys
sys.path.insert(0, '/Workspace/Users/<your-email>/lakeforge')

# Verify import works
from lakeforge import *
```

### Catalog Not Found

```sql
-- Create required catalogs
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS monitoring;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze.raw;
CREATE SCHEMA IF NOT EXISTS bronze.quarantine;
CREATE SCHEMA IF NOT EXISTS silver.dim;
CREATE SCHEMA IF NOT EXISTS monitoring.logs;
```

### Table Already Exists

```python
# Option 1: Drop and recreate
spark.sql("DROP TABLE IF EXISTS bronze.raw.customers")

# Option 2: Use overwrite mode
bronze_writer.write_to_bronze(df, mode="overwrite", ...)
```

### Memory Errors

```python
# Limit data for testing
df = df.limit(1000)

# Use sampling
df = df.sample(fraction=0.1)
```

---

## 📈 Performance Testing

For production readiness:

1. **Volume testing**: Test with large datasets (1M+ rows)
2. **Concurrency testing**: Run multiple pipelines simultaneously
3. **Monitoring**: Track execution times and resource usage
4. **Optimization**: Test OPTIMIZE and Z-ORDER impact

---

## 🎓 Next Steps

After completing Phase 1 testing:

1. Review trust scores and identify improvement areas
2. Customize DQ rules for your data
3. Configure alerting for failures
4. Schedule pipelines for production
5. Explore Phase 2 features (streaming, monitoring, etc.)

---

## 📞 Support

For issues or questions:
- Check [README.md](../../README.md)
- Review [PHASE1_GUIDE.md](../getting-started/PHASE1_GUIDE.md)
- Open an issue on GitHub

---

**Happy Testing! 🚀**
