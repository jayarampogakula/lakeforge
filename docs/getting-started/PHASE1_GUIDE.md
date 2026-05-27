# LakeForge Phase 1 - Complete Implementation Guide

## Overview

Phase 1 of LakeForge implements the core foundational components for a production-ready data engineering framework:

1. **CSV Loader** - Intelligent CSV ingestion with encoding detection
2. **Excel Loader** - Multi-sheet Excel processing with normalization
3. **Bronze Writer** - Delta Lake writer with audit columns and file tracking
4. **DQ Engine** - Comprehensive data quality validation framework
5. **SCD Type 2** - Slowly Changing Dimension Type 2 implementation
6. **YAML Config** - Configuration-driven pipeline execution
7. **Logging** - Structured observability and metrics tracking

---

## Component Details

### 1. CSV Loader (`lakeforge/ingestion/csv_loader.py`)

**Purpose**: Load CSV files into Spark DataFrames with intelligent handling of encoding, delimiters, and schema detection.

**Key Features**:
- Automatic encoding detection using chardet
- Schema inference or explicit schema support
- Multiline field handling
- Corrupt record handling with PERMISSIVE mode
- Source metadata tracking (file path, timestamp)
- Multiple file pattern support
- Structure validation against expected schema

**Usage Example**:
```python
from lakeforge.ingestion.csv_loader import create_csv_loader

csv_loader = create_csv_loader(spark)

df = csv_loader.load_csv_with_metadata(
    file_path="/path/to/sales.csv",
    header=True,
    delimiter=",",
    add_source_metadata=True
)
```

**Why This Matters**:
- CSV files often have inconsistent encodings (UTF-8, Latin-1, etc.)
- Manual schema definition is error-prone
- Source tracking is critical for data lineage and debugging
- Handles real-world messy data with corrupt record detection

---

### 2. Excel Loader (`lakeforge/ingestion/excel_loader.py`)

**Purpose**: Load Excel files with support for multiple sheets, normalization, and data cleaning.

**Key Features**:
- Multi-sheet loading (single, multiple, or all sheets)
- Sheet normalization (union sheets with different schemas)
- Automatic datetime handling
- Empty row/column removal
- String trimming and cleaning
- Sheet name listing and validation

**Usage Example**:
```python
from lakeforge.ingestion.excel_loader import create_excel_loader

excel_loader = create_excel_loader(spark)

# Load single sheet
df = excel_loader.load_excel(
    file_path="/path/to/report.xlsx",
    sheet_name="Sales Data"
)

# Load and normalize all sheets
normalized_df = excel_loader.load_all_sheets(
    file_path="/path/to/report.xlsx",
    normalize=True
)
```

**Why This Matters**:
- Excel files are common in enterprise data pipelines
- Multiple sheets often need to be combined
- Schema differences between sheets need automatic handling
- Excel datetime/formatting needs special treatment

---

### 3. Bronze Writer (`lakeforge/bronze/bronze_writer.py`)

**Purpose**: Write data to Bronze layer Delta tables with comprehensive audit capabilities and file tracking.

**Key Features**:
- Automatic audit column addition:
  - `_ingestion_timestamp`: When data was ingested
  - `_ingestion_date`: Date partition for the ingestion
  - `_source_system`: System identifier
  - `_record_hash`: MD5 hash for deduplication
- Multiple write modes: append, overwrite, merge
- Partitioning support
- Post-write OPTIMIZE and Z-ORDER
- File tracker table for ingestion lineage
- Merge/upsert capability

**Usage Example**:
```python
from lakeforge.bronze.bronze_writer import create_bronze_writer

bronze_writer = create_bronze_writer(spark)

metrics = bronze_writer.write_to_bronze(
    df=df,
    target_table="sales_raw",
    catalog="bronze",
    schema="sales",
    mode="append",
    partition_by=["_ingestion_date"],
    add_audit_columns=True,
    source_system="erp_system",
    optimize_after_write=True
)
```

**Why This Matters**:
- Audit columns enable tracking data lineage and debugging
- Hash columns enable efficient deduplication
- File tracking prevents duplicate processing
- Merge capability supports incremental loads
- OPTIMIZE/Z-ORDER improves query performance

---

### 4. DQ Engine (`lakeforge/dq/dq_engine.py`)

**Purpose**: Execute comprehensive data quality checks and generate scorecards for monitoring.

**Key Features**:
- **Null Check**: Validate columns for null percentages
- **Duplicate Check**: Identify duplicate records on key columns
- **Regex Check**: Validate format patterns (emails, phone numbers, etc.)
- **Range Check**: Ensure numeric values are within bounds
- **Custom SQL Check**: Execute arbitrary SQL validation logic
- Quarantine failed records to separate tables
- Generate DQ scorecards with pass/fail metrics
- Configurable severity levels (error, warning, info)
- Configurable actions (quarantine, fail, warn)

**Usage Example**:
```python
from lakeforge.dq.dq_engine import create_dq_engine

dq_engine = create_dq_engine(spark)

rules = [
    {
        "rule_name": "order_id_not_null",
        "rule_type": "null_check",
        "column": "order_id",
        "threshold": 0.0,
        "action": "quarantine",
        "severity": "error"
    },
    {
        "rule_name": "email_format_valid",
        "rule_type": "regex_check",
        "column": "email",
        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
        "threshold": 0.95,
        "action": "warn",
        "severity": "warning"
    }
]

results = dq_engine.validate_dataframe(
    df=bronze_df,
    rules=rules,
    quarantine_failures=True
)

# Write quarantine records
if results.get("quarantine_df"):
    dq_engine.write_quarantine_table(
        quarantine_df=results["quarantine_df"],
        catalog="bronze",
        schema="sales",
        table_name="sales_quarantine",
        validation_results=results
    )

# Generate scorecard
dq_engine.generate_scorecard(
    validation_results=results,
    catalog="bronze",
    schema="sales",
    table_name="sales_raw"
)
```

**Why This Matters**:
- Data quality issues cause downstream analytics failures
- Early detection prevents bad data from reaching production
- Quarantine enables fixing and reprocessing bad data
- Scorecards provide visibility into data health over time
- Rule-based approach is maintainable and auditable

---

### 5. SCD Type 2 (`lakeforge/silver/scd_type2.py`)

**Purpose**: Implement Slowly Changing Dimension Type 2 logic to maintain full history of changes.

**Key Features**:
- Automatic hash-based change detection
- Effective date tracking (start and end dates)
- Current record flagging
- Support for multiple business keys
- Configurable tracked columns
- Time travel queries (as-of-date)
- Record history retrieval
- Automatic initial load handling

**Usage Example**:
```python
from lakeforge.silver.scd_type2 import create_scd_type2_handler

scd_handler = create_scd_type2_handler(spark)

metrics = scd_handler.merge_scd_type2(
    source_df=customer_df,
    target_table="customer_dimension",
    catalog="silver",
    schema="dimensions",
    key_columns=["customer_id"],
    tracked_columns=["name", "email", "address"],
    create_if_not_exists=True
)

# Get current records
current = scd_handler.get_current_records(
    catalog="silver",
    schema="dimensions",
    table_name="customer_dimension"
)

# Get historical records for a customer
history = scd_handler.get_record_history(
    catalog="silver",
    schema="dimensions",
    table_name="customer_dimension",
    key_values={"customer_id": "C001"}
)

# Time travel - records as of specific date
as_of = scd_handler.get_records_as_of_date(
    catalog="silver",
    schema="dimensions",
    table_name="customer_dimension",
    as_of_date=date(2024, 1, 1)
)
```

**Why This Matters**:
- Maintains complete audit trail of all changes
- Enables historical analysis and time-travel queries
- Critical for regulatory compliance and auditing
- Supports accurate point-in-time reporting
- Hash-based detection is efficient and reliable

---

### 6. YAML Config (`lakeforge/metadata/config_parser.py`)

**Purpose**: Enable configuration-driven pipeline execution with validation.

**Key Features**:
- Ingestion configuration parsing
- DQ rules configuration parsing
- Dataclass-based strongly-typed configs
- Configuration validation with error reporting
- Support for metadata, tags, and descriptions

**Configuration Files**:

**Ingestion Config** (`configs/ingestion/sample_csv_ingestion.yaml`):
```yaml
source_name: "sales_data"
source_type: "csv"
source_path: "/mnt/raw/sales/sales_2024.csv"
target_table: "sales_raw"
target_catalog: "bronze"
target_schema: "sales"
mode: "append"
partition_by: ["order_date"]
add_audit_columns: true
```

**DQ Config** (`configs/dq/sample_dq_rules.yaml`):
```yaml
table_name: "sales_raw"
catalog: "bronze"
schema: "sales"
rules:
  - rule_name: "order_id_not_null"
    rule_type: "null_check"
    column: "order_id"
    threshold: 0.0
```

**Usage Example**:
```python
from lakeforge.metadata.config_parser import load_ingestion_config, load_dq_config

# Load and validate configs
ingestion_config = load_ingestion_config("configs/ingestion/sales.yaml")
dq_config = load_dq_config("configs/dq/sales_rules.yaml")

# Use in pipeline
df = csv_loader.load_csv(ingestion_config.source_path)
results = dq_engine.validate_dataframe(df, dq_config.rules)
```

**Why This Matters**:
- Decouples configuration from code
- Enables non-developers to modify pipelines
- Version control for pipeline configurations
- Validation prevents runtime errors
- Reusability across different datasets

---

### 7. Logging (`lakeforge/observability/logger.py`)

**Purpose**: Provide structured, contextual logging for pipeline observability.

**Key Features**:
- Structured logging with timestamps
- Context tracking for correlation
- Multiple log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Console and file output support
- Operation tracking with metadata
- Exception logging with stack traces
- Singleton pattern for global access

**Usage Example**:
```python
from lakeforge.observability.logger import get_logger

logger = get_logger(name="my_pipeline", level="INFO")

# Basic logging
logger.info("Pipeline started")

# Logging with context
logger.set_context(pipeline="sales", run_id="12345")
logger.info("Processing batch", records=1000, duration_ms=523)

# Error logging
try:
    result = process_data()
except Exception as e:
    logger.error("Processing failed", exception=e, batch_id="B001")

# Operation tracking
logger.log_operation(
    operation="bronze_write",
    status="completed",
    table="bronze.sales.sales_raw",
    rows=1000,
    duration_sec=5.2
)
```

**Why This Matters**:
- Debugging production issues requires detailed logs
- Structured logs enable log aggregation and analysis
- Context tracking enables tracing requests across components
- Operation metrics support performance monitoring
- Consistent logging format improves troubleshooting

---

## Installation & Setup

### 1. Install Dependencies

```bash
pip install pyspark delta-spark pyyaml chardet pandas openpyxl
```

### 2. Import the Package

```python
# Core imports
from lakeforge import get_logger
from lakeforge.ingestion import create_csv_loader, create_excel_loader
from lakeforge.bronze import create_bronze_writer
from lakeforge.dq import create_dq_engine
from lakeforge.silver import create_scd_type2_handler
```

### 3. Configure Unity Catalog

Ensure you have appropriate catalogs and schemas:

```sql
CREATE CATALOG IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS bronze.sales;

CREATE CATALOG IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver.dimensions;
```

---

## Complete End-to-End Example

See: `notebooks/demo/phase1_complete_example.py` for a fully working example that demonstrates:

1. Loading CSV files with metadata
2. Writing to Bronze layer with audit columns
3. Running comprehensive DQ validation
4. Quarantining failed records
5. Generating DQ scorecards
6. Processing SCD Type 2 dimension updates

---

## Next Steps (Future Phases)

- **Phase 2**: API ingestion, JSON loader, SQL Server connector
- **Phase 3**: Gold layer aggregations, business logic transformations
- **Phase 4**: Monitoring dashboard, alerting, lineage visualization
- **Phase 5**: MLOps integration, feature store, model serving

---

## Architecture Benefits

### Why This Design?

1. **Modularity**: Each component is independent and reusable
2. **Testability**: Pure functions and dependency injection enable unit testing
3. **Configurability**: YAML configs enable non-code changes
4. **Observability**: Comprehensive logging and metrics
5. **Scalability**: Built on PySpark for distributed processing
6. **Reliability**: Delta Lake provides ACID transactions
7. **Maintainability**: Clear separation of concerns

### Production-Ready Features

✅ Audit columns for lineage  
✅ File tracking to prevent duplicates  
✅ Quarantine tables for data quality failures  
✅ SCD Type 2 for historical tracking  
✅ Structured logging for debugging  
✅ Configuration validation  
✅ Error handling and recovery  
✅ OPTIMIZE and Z-ORDER for performance  

---

## Support & Contribution

For questions, issues, or contributions, see the main README.md.

---

**LakeForge Phase 1 - Production-Ready Foundation** ✅
