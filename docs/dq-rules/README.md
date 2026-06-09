# Data Quality Rules Guide

Complete guide to defining and implementing data quality rules in LakeForge.

---

## 📋 Overview

The Data Quality (DQ) Engine validates data against defined rules before promoting from Bronze to Silver layer.

**Key Concepts**:
* **Rules** - Validation logic (null checks, regex, ranges, etc.)
* **Thresholds** - Acceptable failure rates (e.g., max 5% nulls)
* **Quarantine** - Failed records isolated for review
* **Scorecard** - Summary of validation results

---

## 🎯 Rule Types

### 1. Null Check
Validates that columns contain no (or minimal) null values.

**Use Case**: Required fields must not be null

**Example**:
```python
{
    "rule_name": "customer_id_not_null",
    "rule_type": "null_check",
    "column": "customer_id",
    "threshold": 0.0  # 0% nulls allowed
}
```

**Parameters**:
* `column` (string) - Column to check
* `threshold` (float) - Max null percentage (0.0 to 1.0)

### 2. Duplicate Check
Detects duplicate records based on key columns.

**Use Case**: Ensure unique customer IDs

**Example**:
```python
{
    "rule_name": "customer_id_unique",
    "rule_type": "duplicate_check",
    "columns": ["customer_id"],
    "allow_duplicates": False
}
```

**Parameters**:
* `columns` (list) - Columns defining uniqueness
* `allow_duplicates` (bool) - Whether duplicates are acceptable

### 3. Regex Pattern Check
Validates column values against regex patterns.

**Use Case**: Validate email format, phone numbers, etc.

**Example**:
```python
{
    "rule_name": "email_format",
    "rule_type": "regex_check",
    "column": "email",
    "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$",
    "threshold": 0.95  # 95% must match
}
```

**Common Patterns**:
```python
# Email
"^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"

# Phone (US format)
"^\\d{3}-\\d{3}-\\d{4}$"

# ZIP code
"^\\d{5}(-\\d{4})?$"

# Date (YYYY-MM-DD)
"^\\d{4}-\\d{2}-\\d{2}$"
```

### 4. Range Check
Validates numeric columns fall within acceptable ranges.

**Use Case**: Age between 0-120, prices positive, etc.

**Example**:
```python
{
    "rule_name": "age_valid_range",
    "rule_type": "range_check",
    "column": "age",
    "min_value": 0,
    "max_value": 120,
    "threshold": 1.0  # 100% must be in range
}
```

**Parameters**:
* `column` (string) - Numeric column to check
* `min_value` (float, optional) - Minimum allowed value
* `max_value` (float, optional) - Maximum allowed value
* `threshold` (float) - Min % that must be in range

### 5. Referential Integrity
Validates foreign key relationships between tables.

**Use Case**: All order.customer_id exist in customers table

**Example**:
```python
{
    "rule_name": "customer_exists",
    "rule_type": "referential_integrity",
    "source_table": "orders",
    "source_column": "customer_id",
    "target_table": "customers",
    "target_column": "customer_id",
    "threshold": 0.99  # 99% must match
}
```

### 6. Custom SQL Check
Run custom SQL validation logic.

**Use Case**: Complex business rules

**Example**:
```python
{
    "rule_name": "total_matches_sum",
    "rule_type": "custom_sql",
    "custom_sql": """
        SELECT 
            CASE 
                WHEN SUM(amount) = MAX(total)
                THEN TRUE 
                ELSE FALSE 
            END as passed
        FROM dq_check_view
    """
}
```

---

## 📝 Complete Rule Configuration Example

```python
from lakeforge.dq import create_dq_engine

spark = SparkSession.builder.appName("DQ-Validation").getOrCreate()
dq_engine = create_dq_engine(spark)

# Define comprehensive rule set
rules = [
    # 1. Required fields
    {
        "rule_name": "customer_id_required",
        "rule_type": "null_check",
        "column": "customer_id",
        "threshold": 0.0
    },
    {
        "rule_name": "email_required",
        "rule_type": "null_check",
        "column": "email",
        "threshold": 0.05  # Allow 5% nulls
    },
    
    # 2. Uniqueness
    {
        "rule_name": "customer_id_unique",
        "rule_type": "duplicate_check",
        "columns": ["customer_id"],
        "allow_duplicates": False
    },
    
    # 3. Format validations
    {
        "rule_name": "email_format_valid",
        "rule_type": "regex_check",
        "column": "email",
        "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$",
        "threshold": 0.95
    },
    {
        "rule_name": "phone_format_valid",
        "rule_type": "regex_check",
        "column": "phone",
        "pattern": "^\\d{3}-\\d{3}-\\d{4}$",
        "threshold": 0.90
    },
    
    # 4. Range validations
    {
        "rule_name": "age_reasonable",
        "rule_type": "range_check",
        "column": "age",
        "min_value": 18,
        "max_value": 100,
        "threshold": 0.99
    },
    {
        "rule_name": "order_amount_positive",
        "rule_type": "range_check",
        "column": "order_amount",
        "min_value": 0.01,
        "max_value": 1000000,
        "threshold": 1.0
    },
    
    # 5. Referential integrity
    {
        "rule_name": "valid_country_code",
        "rule_type": "referential_integrity",
        "source_table": "customers",
        "source_column": "country_code",
        "target_table": "ref_countries",
        "target_column": "code",
        "threshold": 0.99
    }
]

# Load data
df = spark.table("bronze.raw.customers")

# Run validation
dq_results = dq_engine.validate_dataframe(
    df=df,
    rules=rules,
    quarantine_failures=True
)

# Check results
print(f"DQ Score: {dq_results['rules_passed']}/{dq_results['rules_executed']}")
print(f"Overall Pass Rate: {dq_results['rules_passed']/dq_results['rules_executed']*100:.2f}%")

# Write quarantine table
if dq_results["rules_failed"] > 0:
    dq_engine.write_quarantine_table(
        df=df,
        rule_results=dq_results["rule_results"],
        catalog="bronze",
        schema="quarantine",
        table_name="customers_failed"
    )
```

---

## 🎯 Best Practices

### 1. Start Simple, Add Complexity
Begin with basic checks:
```python
# Phase 1: Critical checks only
- customer_id not null
- customer_id unique
- email not null

# Phase 2: Add format validations
- email format
- phone format

# Phase 3: Add business rules
- age range
- referential integrity
```

### 2. Set Realistic Thresholds
```python
# Too strict (may fail unnecessarily)
threshold: 1.0  # 100% must pass

# Realistic (accounts for data quality issues)
threshold: 0.95  # 95% must pass

# Too lenient (defeats purpose)
threshold: 0.50  # Only 50% must pass
```

### 3. Monitor and Adjust
Track DQ scores over time:
* Start with lenient thresholds
* Monitor actual pass rates
* Gradually tighten thresholds
* Alert on sudden drops

### 4. Document Business Rules
Every rule should have:
* Clear business justification
* Owner contact
* Threshold rationale
* Remediation process

### 5. Quarantine Strategy
Handle failed records appropriately:
```python
# Option 1: Reject all failures
quarantine_failures=True

# Option 2: Allow with flag
df.withColumn("dq_flag", 
    when(validation_failed, "FAILED").otherwise("PASSED"))
```

---

## 📊 Rule Templates by Industry

### E-Commerce
```python
e_commerce_rules = [
    {"rule_name": "sku_required", "rule_type": "null_check", 
     "column": "sku", "threshold": 0.0},
    {"rule_name": "price_positive", "rule_type": "range_check", 
     "column": "price", "min_value": 0.01, "threshold": 1.0},
    {"rule_name": "quantity_valid", "rule_type": "range_check", 
     "column": "quantity", "min_value": 1, "max_value": 10000, "threshold": 1.0}
]
```

### Financial Services
```python
financial_rules = [
    {"rule_name": "account_number_format", "rule_type": "regex_check",
     "column": "account_number", "pattern": "^\\d{10}$", "threshold": 1.0},
    {"rule_name": "transaction_amount_reasonable", "rule_type": "range_check",
     "column": "amount", "min_value": -1000000, "max_value": 1000000, "threshold": 0.99},
    {"rule_name": "customer_exists", "rule_type": "referential_integrity",
     "source_column": "customer_id", "target_table": "customers", "threshold": 1.0}
]
```

### Healthcare
```python
healthcare_rules = [
    {"rule_name": "patient_id_unique", "rule_type": "duplicate_check",
     "columns": ["patient_id"], "allow_duplicates": False},
    {"rule_name": "age_reasonable", "rule_type": "range_check",
     "column": "age", "min_value": 0, "max_value": 120, "threshold": 1.0},
    {"rule_name": "icd_code_format", "rule_type": "regex_check",
     "column": "icd_code", "pattern": "^[A-Z]\\d{2}(\\.\\d{1,2})?$", "threshold": 0.99}
]
```

---

## 🔄 Continuous Improvement

### 1. Track Metrics
```sql
-- DQ score trends
SELECT 
    date,
    table_name,
    AVG(dq_score) as avg_score,
    MIN(dq_score) as min_score
FROM monitoring.dq_results
GROUP BY date, table_name
ORDER BY date DESC
```

### 2. Root Cause Analysis
When failures occur:
1. Check quarantine table
2. Identify patterns
3. Fix at source if possible
4. Adjust rules if needed

### 3. Feedback Loop
```
Failed Records → Review → Fix Source → Re-validate → Update Rules
```

---

## 📚 Additional Resources

* [DQ Engine API Reference](../../lakeforge/dq/dq_engine.py)
* [Validation Examples](../examples/)
* [Testing Guide](../testing/TESTING_GUIDE.md)

---

**Next Steps**: Apply rules to your Bronze data and monitor the DQ scorecard!
