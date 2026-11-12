# ⚙️ LakeForge Configuration Reference & Templates

Welcome to the **LakeForge Configuration Reference Directory**. LakeForge uses a declarative, metadata-driven architecture to power the Medallion Data Lakehouse (**Bronze ➔ Silver ➔ Gold ➔ Views ➔ Governance**).

To accelerate pipeline development and establish team best practices, each configuration directory includes a **`template_all_options.json`** file documenting every available property, rule type, and strategy.

---

## 📁 Configuration Directory Structure

```
configs/
├── ingestion/                  # Bronze Ingestion Connectors (Batch, Stream & SaaS)
│   ├── template_all_options.json
│   ├── bad_duplicate_keys.json
│   ├── sample_csv_ingestion.json
│   └── client_alpha/           # Tenant-specific ingestion overrides
├── dq/                         # Data Quality Gate & Quarantine Rules
│   ├── template_all_options.json
│   ├── customers.json
│   └── client_alpha/           # Tenant-specific quality thresholds
├── transformations/            # Silver Cleansing, Deduplication & SCD-2
│   ├── template_all_options.json
│   ├── customers_clean.json
│   └── client_alpha/           # Tenant-specific business logic
├── aggregations/               # Gold Star/Snowflake Dimensional Marts
│   ├── template_all_options.json
│   ├── customer_summary.json
│   └── daily_revenue.json
├── views/                      # Persistent & Temporary Analytical Views
│   ├── template_all_options.json
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── governance/                 # Unity Catalog RBAC, Column PII Masks & Row Filters
│   ├── template_all_options.json
│   ├── access_control.json
│   └── data_protection.json
└── environments/               # Stage Environments (Catalog & Schema Mapping)
    └── env_settings.json
```

---

## 1. 📥 Ingestion Template (`configs/ingestion/template_all_options.json`)

Controls how raw data is acquired from cloud storage, relational databases, data warehouses, streaming systems, or SaaS endpoints into Bronze Delta tables.

| Option | Type | Description |
| :--- | :--- | :--- |
| `source_name` | `string` | Unique identifier for the source system/entity. |
| `source_type` | `string` | Connector format: `csv`, `excel`, `json`, `parquet`, `avro`, `orc`, `delta`, `postgres`, `mysql`, `oracle`, `azure_sql`, `snowflake`, `bigquery`, `redshift`, `mongodb`, `api`, `jira`, `sharepoint`, `google_sheets`. |
| `source_path` | `string` | Source URI, Databricks Volume path, or cloud storage bucket (`/Volumes/...`, `s3://...`, `abfss://...`). |
| `target_catalog` | `string` | Unity Catalog name or stage token `{catalog}` / `{client_catalog}`. |
| `target_schema` | `string` | Target schema (defaults to `bronze`). |
| `target_table` | `string` | Bronze Delta table name. |
| `load_type` | `string` | Ingestion mode: `batch`, `incremental`, `stream`, `full`. |
| `merge_strategy`| `string` | Bronze write mode: `append`, `overwrite`, `upsert`. |
| `business_key` | `list[str]` | Unique composite primary keys for deduplication and upserts. |
| `incremental` | `bool` | Auto-Loader-like incremental file inventory tracking. |
| `incremental_log_table` | `string` | Custom file-inventory ledger table (e.g. `{catalog}.monitoring.ingested_files`). |
| `watermark_column` | `string` | Timestamp column used for incremental watermark queries. |
| `header` | `bool` | Whether files contain column headers (CSV/Excel). |
| `header_row` | `int` | 1-based row number holding column names (skips title/preamble lines). |
| `delimiter` | `string` | Delimiter character (e.g. `,`, `\t`, `\|`). |
| `encoding` | `string` | Character encoding (default: `utf-8`). |
| `sheet_name` | `string` | Target sheet name for Excel (`.xlsx`, `.xls`) ingestion. |
| `sanitize_column_names` | `bool` | Replaces spaces and special characters with `_` for Delta safety. |
| `infer_schema` | `bool` | Whether PySpark infers schema automatically. |
| `schema` | `string` | Explicit Spark SQL DDL schema string. |
| `schema_evolution` | `string` | Schema drift guardrail: `allow` (mergeSchema), `strict` (fail on drift), `quarantine` (isolate drifted columns). |
| `add_audit_columns` | `bool` | Appends `_ingestion_timestamp`, `_source_system`, `_batch_id`, `_record_hash`. |
| `partition_by` | `list[str]` | Physical partition columns for Delta storage. |
| `options` | `dict` | Connector-specific Spark reader options (`multiLine`, `quote`, `escape`, etc.). |
| `client_id` | `string` | Tenant/client scope identifier for multi-tenant isolation. |
| `tags` | `dict` | Metadata tags for governance, classification, and SLAs. |

---

## 2. 🛡 Data Quality Template (`configs/dq/template_all_options.json`)

Validates incoming data before Silver layer promotion, quarantining invalid records and publishing audit scorecards.

| Option | Type | Description |
| :--- | :--- | :--- |
| `table_name` | `string` | Target table being audited. |
| `catalog` | `string` | Catalog name or `{catalog}` stage token. |
| `schema` | `string` | Schema name or `{silver_schema}` stage token. |
| `quarantine_table`| `string` | Destination Delta table for invalid/quarantined rows. |
| `enable_scorecard`| `bool` | Generates execution quality metrics ledger. |
| `action_on_failure`| `string` | Default table-level action: `quarantine`, `fail`, `warn`. |
| `rules` | `list[dict]` | Array of individual quality check definitions (see below). |

### Supported Rule Types
1. **`null_check` / `not_null`**: Asserts specified column is not null.
2. **`duplicate_check` / `unique`**: Asserts composite key uniqueness across specified columns.
3. **`range_check` / `range`**: Numeric/date boundary check (`min_value`, `max_value`).
4. **`regex_check` / `regex`**: Regular expression format validation (`pattern`).
5. **`allowed_values`**: Set membership check (`allowed_values: ["A", "B", "C"]`).
6. **`datatype_check`**: Validates target column datatype conformance (`expected_type`).
7. **`custom_sql`**: Freeform Spark SQL boolean condition (`custom_sql: "end_date >= start_date"`).
8. **`referential_integrity`**: Foreign key integrity check against parent table (`source_column`, `reference_table`, `reference_column`).
9. **`null_rate_threshold`**: Tolerates a null percentage below a threshold (`threshold: 0.10`).
10. **`row_count_threshold`**: Asserts minimum batch volume against baseline (`baseline_count`, `threshold_percent`).

---

## 3. 🔄 Silver Transformations Template (`configs/transformations/template_all_options.json`)

Standardizes, cleanses, enriches, deduplicates, and manages SCD Type 1 & 2 history using generic built-in operations or custom Python/PySpark functions.

> [!TIP]
> **Modular Python Functions**: Instead of massive inline SQL or config bloat, define custom column or DataFrame transformation logic in Python/PySpark files (e.g. `lakeforge/transformations/custom.py` or your own workspace module) and call them with `"custom_module"` and `"custom_function"`.

| Option | Type | Description |
| :--- | :--- | :--- |
| `source_table` | `string` | Source Bronze table name. |
| `target_table` | `string` | Target Silver table name. |
| `custom_module` | `string` | Python module containing custom DataFrame transformation function (e.g. `lakeforge.transformations.custom`). |
| `custom_function`| `string` | Custom Python function name to execute on DataFrame (e.g. `clean_customer_dataframe`). |
| `params` | `dict` | Keyword parameters passed directly to custom Python function. |
| `custom_columns` | `list[dict]` | Column-level custom Python functions (`column`, `target_column`, `module`, `function`). |
| `scd_type` | `int` | `1` (in-place overwrite) or `2` (historical tracking with `is_current`, `valid_from`, `valid_to`). |
| `business_keys` | `list[str]` | Primary composite keys for deduplication and SCD merges. |
| `timestamp_column` | `string` | Timestamp used for ordering historical record versions. |
| `schema` | `string/dict` | Strict schema enforcement (DDL string or dict of column -> type). |
| `deduplication` | `dict` | Deduplication settings (`key_columns`, `order_column`, `order_desc`, `strategy`). |
| `columns` | `dict` | Column expression mappings (e.g. `{"id": "trim(id)}"`, `{"region": "upper(region)}"`) |
| `transformations` | `list[dict]` | Ordered pipeline of transformation steps (see below). |

### Supported Generic & Custom Transformation Step Types
- `filter` / `filter_rows`: Filters rows matching Spark SQL expression (`condition`).
- `drop` / `drop_columns`: Removes obsolete columns (`columns: [...]`).
- `deduplicate` / `drop_duplicates`: Windowed deduplication (`columns`, `strategy`, `order_by`).
- `join` / `join_tables`: Joins with lookup table (`right_table`, `join_keys`, `join_type: "inner" | "left" | "right" | "full"`).
- `rename` / `rename_columns`: Renames columns (`mapping: {"old": "new"}`).
- `cast` / `cast_columns`: Casts column(s) to target types (`column`, `target_type` or `columns` dict).
- `standardize`: String standardization (`column`, `operation: "lowercase" | "uppercase" | "trim" | "initcap" | "remove_non_numeric"`).
- `derived_column`: New calculated column via Spark SQL expression (`name`, `expression`).
- `select` / `select_columns`: Column projection (`columns: [...]`).
- `select_and_cast`: Projects and casts in a single operation (`columns: {"col": "type"}`).
- `enforce_schema`: Schema validation and conformance (`schema`).
- `custom_column`: Custom Python function on a single column (`column`, `target_column`, `module`, `function`).
- `custom_dataframe` / `custom_function`: Custom Python function on the entire DataFrame (`module`, `function`, `params`).

---

## 4. 🥇 Gold Aggregations Template (`configs/aggregations/template_all_options.json`)

Computes dimensional marts, KPI rollups, and star/snowflake analytical tables using generic groupBy/join definitions or custom Python/PySpark aggregation functions.

> [!TIP]
> **Custom Python Aggregation Functions**: For advanced Gold marts, write your aggregation in a Python module (e.g. `lakeforge/aggregations/custom.py` or your own workspace file) and reference it with `"custom_module"` and `"custom_function"`. The function receives `source_dfs: Dict[str, DataFrame]` and returns the aggregated DataFrame!

| Option | Type | Description |
| :--- | :--- | :--- |
| `target_table` | `string` | Target Gold table name. |
| `source_tables` | `dict` | Mapping of alias to Silver source table names. |
| `custom_module` | `string` | Python module containing custom mart aggregation function (e.g. `lakeforge.aggregations.custom`). |
| `custom_function`| `string` | Custom Python function name to execute (e.g. `compute_customer_sales_mart`). |
| `params` | `dict` | Parameters passed to custom aggregation function. |
| `merge_strategy` | `string` | Gold write mode: `upsert`, `overwrite`, `append`. |
| `business_key` | `list[str]` | Composite business key for upserting into the Gold mart. |
| `group_by` | `list[str]` | Multidimensional group-by dimensions. |
| `aggregations` | `list[dict]` | Array of metric definitions (`name`, `expression`). |
| `join_logic` | `string` | Custom SQL join syntax between aliased source tables. |
| `filter_condition` | `string` | SQL `WHERE` clause applied prior to aggregation. |
| `having_condition` | `string` | SQL `HAVING` clause applied post-aggregation. |
| `order_by` | `list[str]` | Output sorting specifications. |
| `aggregations` | `list[dict]` | Array of metric definitions (`name`, `expression`). |
| `join_logic` | `string` | Custom SQL join syntax between aliased source tables. |
| `filter_condition` | `string` | SQL `WHERE` clause applied prior to aggregation. |
| `having_condition` | `string` | SQL `HAVING` clause applied post-aggregation. |
| `order_by` | `list[str]` | Output sorting specifications. |

---

## 5. 👁 Views Template (`configs/views/template_all_options.json`)

Generates on-demand persistent Delta views or temporary session views for Power BI, Tableau, and ad-hoc analytics.

| Option | Type | Description |
| :--- | :--- | :--- |
| `view_name` | `string` | Name of the Spark SQL view. |
| `definition_type`| `string` | `table` (projection/filter on a single table) or `query` (freeform SQL). |
| `schema_type` | `string` | Schema tier: `bronze`, `silver`, `gold`. |
| `view_type` | `string` | `persistent` (persisted in catalog/schema) or `temp` (session-scoped). |
| `source_table` | `string` | Source table (required if `definition_type` is `table`). |
| `select_columns` | `list[str]` | Column projection (for `definition_type: "table"`). |
| `filter_condition`| `string` | Filter predicate (for `definition_type: "table"`). |
| `query` | `string` | Complete SQL statement (for `definition_type: "query"`). |

---

## 6. 🔐 Governance Template (`configs/governance/template_all_options.json`)

Automates Unity Catalog role-based access control (RBAC), fine-grained column PII masking, and row-level security filters.

| Section | Description |
| :--- | :--- |
| `roles` | Group and service principal permissions across Catalogs (`USE CATALOG`), Schemas (`USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE`), Tables, and Views. |
| `clients` | Hierarchical client-specific role overrides. |
| `masking_functions` | SQL UDFs for column masking (e.g. SHA-256 hashes, email masking, phone redacting). |
| `column_masks` | Associates sensitive table columns with masking functions. |
| `row_filters` | Restricts row visibility based on group membership (`is_account_group_member`) or tenant context. |

---

## 🛠 How to Use These Templates for a New Use Case

1. **Copy the template** into the respective directory with your use case name:
   ```bash
   cp configs/ingestion/template_all_options.json configs/ingestion/my_source.json
   cp configs/dq/template_all_options.json configs/dq/my_source.json
   cp configs/transformations/template_all_options.json configs/transformations/my_source_clean.json
   ```
2. **Remove the options you don't need** and update your entity names and business logic.
3. **Validate your configuration** across all environments with the LakeForge CLI:
   ```bash
   lakeforge validate configs --env dev
   ```
