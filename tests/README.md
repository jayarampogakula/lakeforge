# LakeForge Testing Framework

Comprehensive testing structure for validating LakeForge functionality at all levels.

---

## 📁 Testing Structure

```
/tests/
├── /feature-tests/          ✅ End-to-end feature validation notebooks
├── /unit_tests/             ✅ Python unit tests (pytest)
├── /integration_tests/      ✅ Integration tests (component interactions)
├── /test_data/              ✅ Sample datasets for testing
├── Generate_Test_Datasets   ✅ Creates test data
└── CHAOS_ETL_Disasters_Demo ✅ Chaos engineering demo
```

---

## 🎯 Test Types Explained

### 1. **Feature Tests** (`/feature-tests/`)
**Purpose**: Validate complete user-facing features work end-to-end

**Format**: Interactive Databricks notebooks

**Examples**:
* `test_1_ingestion` - CSV/Excel/API loaders work
* `test_4_data_quality` - DQ Engine validates data
* `test_5_trust_engine` - Trust Engine detects issues

**When to run**: 
* After code changes
* Before releases
* Manual verification

**Run with**: Open notebook → Click "Run All"

---

### 2. **Unit Tests** (`/unit_tests/`)
**Purpose**: Test individual functions in isolation

**Format**: Python files using `pytest`

**Examples**:
* `test_dq_rules.py` - Test individual DQ validation functions
* `test_bronze_writer.py` - Test Bronze write logic
* `test_schema_detector.py` - Test schema detection

**When to run**:
* During development (TDD)
* CI/CD pipelines
* After bug fixes

**Run with**: 
```bash
pytest unit_tests/
```

---

### 3. **Integration Tests** (`/integration_tests/`)
**Purpose**: Test multiple components working together

**Format**: Python files using `pytest` (may require Spark)

**Examples**:
* `test_bronze_to_silver.py` - Bronze + DQ Engine integration
* `test_silver_to_gold.py` - Silver + Trust Engine integration
* `test_end_to_end_pipeline.py` - Full pipeline flow

**When to run**:
* Before deployments
* Integration testing phase
* After major refactors

**Run with**:
```bash
pytest integration_tests/
```

---

### 4. **Test Data** (`/test_data/`)
**Purpose**: Sample datasets for all test levels

**Contents**:
* CSV files (customers, transactions, products)
* JSON files (API responses)
* Parquet files (large datasets)
* Excel files (spreadsheet imports)
* Bad data samples (for negative testing)

**Size**: Keep small (<1000 rows per file)

---

## 📊 Testing Matrix

| Component | Unit Tests | Integration Tests | Feature Tests |
|-----------|------------|-------------------|---------------|
| Ingestion Loaders | ✅ test_loaders.py | ✅ test_ingestion_flow.py | ✅ test_1_ingestion |
| Bronze Layer | ✅ test_bronze_writer.py | ✅ test_bronze_to_silver.py | ✅ test_2_bronze_layer |
| DQ Engine | ✅ test_dq_rules.py | ✅ test_bronze_to_silver.py | ✅ test_4_data_quality |
| Trust Engine | ✅ test_trust_validations.py | ✅ test_silver_to_gold.py | ✅ test_5_trust_engine |
| Schema Drift | ✅ test_schema_detector.py | ✅ test_schema_evolution.py | ✅ test_3_schema_drift |
| Reporting | ✅ test_report_generator.py | ✅ test_end_to_end.py | ✅ test_6_reporting |

---

## 🎨 Test Data Catalog

### Sample Datasets in `/test_data/`

#### **Good Data** (for positive tests)
```
/test_data/
├── customers_valid.csv          # 100 valid customer records
├── transactions_valid.csv       # 500 valid transactions
├── products_valid.csv           # 50 valid products
└── orders_valid.json            # 200 valid orders (JSON)
```

#### **Bad Data** (for negative tests)
```
/test_data/
├── customers_nulls.csv          # Has null values in required fields
├── customers_duplicates.csv     # Has duplicate customer_ids
├── customers_bad_email.csv      # Invalid email formats
├── transactions_bad_amounts.csv # Negative amounts, out of range
└── orders_missing_keys.json     # Missing foreign keys
```

#### **Large Datasets** (for performance tests)
```
/test_data/
├── customers_10k.parquet        # 10,000 customers
├── transactions_100k.parquet    # 100,000 transactions
└── events_1m.parquet             # 1M streaming events
```

---

## 🧪 Test Scenarios

### Unit Test Example: `test_dq_rules.py`
```python
import pytest
from lakeforge.dq import DQEngine

def test_null_check_validation():
    """Test null check rule works correctly"""
    dq_engine = DQEngine(spark)
    
    rule = {
        "rule_type": "null_check",
        "column": "customer_id",
        "threshold": 0.0
    }
    
    # Test with no nulls
    df_valid = spark.createDataFrame([(1,), (2,), (3,)], ["customer_id"])
    result = dq_engine.validate_rule(df_valid, rule)
    assert result["passed"] == True
    
    # Test with nulls
    df_invalid = spark.createDataFrame([(1,), (None,), (3,)], ["customer_id"])
    result = dq_engine.validate_rule(df_invalid, rule)
    assert result["passed"] == False
```

### Integration Test Example: `test_bronze_to_silver.py`
```python
import pytest
from lakeforge.bronze import BronzeWriter
from lakeforge.dq import DQEngine

def test_bronze_to_silver_flow():
    """Test complete Bronze → DQ → Silver flow"""
    # Step 1: Write to Bronze
    bronze_writer = BronzeWriter(spark)
    df_raw = spark.read.csv("/test_data/customers_valid.csv")
    bronze_writer.write_table(df_raw, "bronze.raw.customers")
    
    # Step 2: Apply DQ validations
    dq_engine = DQEngine(spark)
    df_bronze = spark.table("bronze.raw.customers")
    dq_results = dq_engine.validate_dataframe(df_bronze, rules)
    
    # Step 3: Verify Silver write
    assert dq_results["rules_passed"] >= 8
    assert spark.table("silver.customers").count() > 0
```

---

## ✅ Testing Checklist

**Before Release:**
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] All 7 feature tests run successfully
- [ ] Test data generated
- [ ] Code coverage > 70%
- [ ] No critical bugs

---

**Happy Testing! 🎉**