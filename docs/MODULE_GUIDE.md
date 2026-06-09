# LakeForge Modules Guide

This guide describes the core Python modules, utilities, and engines in the LakeForge metadata-driven ETL framework.

---

## 1. Ingestion Package (`lakeforge/ingestion/`)

Handles reading from a wide range of batch and streaming data sources, dynamically converting raw source formats to Spark DataFrames.

### Core Loaders
* **`csv_loader.py`**: Loads CSV files using configured headers, custom delimiters, and encoding options.
* **`excel_loader.py`**: Loads spreadsheets, supporting specific sheet selections and header configurations.
* **`json_loader.py`**: Loads single-line and multi-line nested JSON structures.
* **`parquet_loader.py`**: Reads optimized columnar Parquet datasets.
* **`s3_loader.py` & `gcs_loader.py`**: AWS and Google Cloud Storage loaders optimized for bucket security credentials and folder mounts.
* **`oracle_loader.py`, `postgres_loader.py`, `mysql_loader.py`, `azure_sql_loader.py`**: Relational database loaders executing pushdown queries via JDBC.
* **`bigquery_loader.py`, `redshift_loader.py`, `snowflake_loader.py`**: Enterprise warehouse loaders optimized for query pushdown and stage operations.
* **`kafka_loader.py` & `streaming_loader.py`**: Structured streaming ingress from event streams and directories.
* **`jira_loader.py`, `sharepoint_loader.py`, `google_sheets_loader.py`**: Specialized SaaS API connectors.

### Metadata Extraction
* **`schema_detector.py`**: Infers a Spark `StructType` schema from CSV, JSON, and Parquet files dynamically before executing load pipelines.

---

## 2. Bronze Layer Package (`lakeforge/bronze/`)

Writes data to Bronze layer Delta tables and maintains raw ingestion audit logs.

* **`bronze_writer.py`**: Appends or merges clean DataFrames into raw Bronze Delta tables. Automatically appends audit tracking metadata:
  - `_ingestion_timestamp`: Ingestion timestamp
  - `_ingestion_date`: Partition-friendly date
  - `_source_system`: Source identifier
  - `_record_hash`: MD5 hash of row values for duplicate checks
* **`file_tracker.py`**: Skeletons for logging ingested files and their sizes/mod-times.

---

## 3. Data Quality Package (`lakeforge/dq/`)

Executes configuration-driven validation gates, quarantining failures, and scoring datasets.

* **`dq_engine.py`**: The central DQ executor. It evaluates:
  - `null_check` (not null): verifies column contains no null values.
  - `duplicate_check` (unique): asserts uniqueness of keys.
  - `regex_check`: verifies string patterns.
  - `range_check`: restricts numerical fields within bounds.
  - `datatype_check`: verifies correct Spark column data types.
  - `allowed_values`: checks if string belongs to an allowed set.
  - `referential_integrity`: left anti-joins against a parent table to find orphaned keys.
  - `row_count_threshold`: checks that row counts match expectations.
  - `null_rate_threshold`: alerts on null volume spikes.
  - `custom_sql`: executes arbitrary Spark SQL validation queries.
* **`validators/datatype_check.py`**: Helper to validate schema types against string descriptors (e.g. `decimal(10,2)`).

---

## 4. Silver Layer Package (`lakeforge/silver/`)

Transforms raw Bronze records, deduplicates, and manages history tracking.

* **`transformer.py`**: Applies transformation chains dynamically:
  - `cast`: casts column to different datatypes.
  - `filter`: filters using Spark expression syntax.
  - `standardize`: lowercase, uppercase, or non-numeric character removal.
  - `derived_column`: executes arbitrary Spark SQL expressions to create new columns.
  - `deduplicate`: groups records by keys and keeps the latest version.
  - `drop`: drops specified columns from the DataFrame.
  - `rename`: renames one or more columns using simple mapping or target naming.
  - `join`: performs Spark joins against other catalog tables using keys.
  - `custom_function`: dynamically loads and runs an external Python transform function.
* **`deduplication.py`**: Implements window-partition deduplication strategies.
* **`merge_engine.py`**: Handles idempotent merge operations for standard upserts and append writes on Silver tables.
* **`scd_type2.py`**: Implements slowly changing dimension Type 2 tracking, updating effective end dates and current flags on matching key updates.

---

## 5. Gold Layer Package (`lakeforge/gold/`)

Aggregates cleansed Silver tables to form final business marts.

* **`aggregations.py`**: Generates and executes Spark SQL queries dynamically. Integrates joins, filters, grouping keys, and aggregate metrics (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`) using declarations defined entirely in JSON configurations.

---

## 6. Trust Engine Package (`lakeforge/trust_engine/`)

The framework's gatekeeper, validating and auditing pipeline health.

* **`trust_engine.py`**: Computes an overall score representing data quality and ingestion integrity:
  - Integrates DQ validation pass rates and schema drift logs.
  - Measures row count drift and duplicate explosion ratios between layers.
  - Logs results to `trust_validation_log` Delta tables.

---

## 7. Utilities Package (`lakeforge/utilities/`)

* **`dynamic_runner.py`**: Dynamically imports Python modules and resolves attributes at runtime using python's `importlib`, enabling flexible custom transformations from metadata configurations.
