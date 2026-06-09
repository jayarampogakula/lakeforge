# LakeForge Architecture Documentation

## Overview

LakeForge implements a modern **Medallion Architecture** for data lakehouse patterns, combining the best of data lakes and data warehouses.

---

## 🏗️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
│  Databases • Cloud Storage • APIs • Streaming • SaaS Apps       │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │   INGESTION LAYER            │
        │  - 16 Pre-built Connectors   │
        │  - Auto Loader Support       │
        │  - Schema Detection          │
        └──────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                        │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  - Raw data as-is from sources                             │  │
│  │  - Audit columns added (_ingestion_timestamp, etc.)        │  │
│  │  - Minimal validation                                      │  │
│  │  - Append-only or full snapshots                          │  │
│  │  - File tracking & deduplication                          │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  Tables: bronze.raw.customers, bronze.raw.transactions            │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
                       ▼ DQ Engine validates
┌──────────────────────────────────────────────────────────────────┐
│                 SILVER LAYER (Cleansed Data)                      │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  - Data quality rules enforced                             │  │
│  │  - Type casting & standardization                         │  │
│  │  - Deduplication logic                                     │  │
│  │  - Null handling & cleansing                              │  │
│  │  - Business rules applied                                  │  │
│  │  - Quarantine for bad records                             │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  Tables: silver.customers, silver.transactions                    │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
                       ▼ Trust Engine validates
┌──────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Business Metrics)                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  - Aggregated business metrics                             │  │
│  │  - Star/snowflake schemas                                  │  │
│  │  - SCD Type 2 for history tracking                        │  │
│  │  - Optimized for BI queries                               │  │
│  │  - Trust score reporting                                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  Tables: gold.customer_metrics, gold.daily_sales                  │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │    CONSUMPTION LAYER         │
        │  - Dashboards (Lakeview)     │
        │  - BI Tools (Tableau, etc.)  │
        │  - ML Models (MLflow)        │
        │  - REST APIs                 │
        └──────────────────────────────┘
```

---

## 🎯 Core Components

### 1. Ingestion Layer
**Location**: `lakeforge/ingestion/`

**Purpose**: Load data from various sources into Bronze layer

**Components**:
* 16 pre-built loaders (Kafka, Oracle, S3, BigQuery, etc.)
* Auto Loader for cloud storage
* Schema detection and evolution
* Metadata tracking

### 2. Bronze Layer (Raw Zone)
**Location**: `lakeforge/bronze/`

**Purpose**: Store raw data with minimal transformation

**Key Features**:
* Preserves original data exactly as received
* Adds audit columns (timestamp, source, user)
* File tracking to prevent duplicates
* Partition by ingestion date
* Delta Lake format for ACID transactions

**Tables Example**:
```
bronze.raw.customers
  ├── customer_id
  ├── name
  ├── email
  ├── _source_file
  ├── _ingestion_timestamp
  ├── _ingestion_date (partition)
  └── _source_system
```

### 3. Data Quality Engine
**Location**: `lakeforge/dq/`

**Purpose**: Validate data quality before moving to Silver

**Validations**:
* Null checks
* Duplicate detection
* Regex pattern matching
* Range validations
* Referential integrity
* Custom SQL rules

**Output**:
* DQ scorecard
* Quarantine tables for failed records
* Validation metrics

### 4. Silver Layer (Cleansed Zone)
**Location**: `lakeforge/silver/`

**Purpose**: Store validated and cleansed data

**Key Features**:
* Type casting and standardization
* Deduplication
* Data cleansing (trim, uppercase, etc.)
* Business rule enforcement
* Incremental merge (MERGE INTO)
* Schema enforcement

**Tables Example**:
```
silver.customers
  ├── customer_id (INT, NOT NULL)
  ├── name (STRING, standardized)
  ├── email (STRING, validated format)
  ├── country_code (STRING, ISO format)
  ├── effective_date
  └── is_current
```

### 5. Trust Engine
**Location**: `lakeforge/trust_engine/`

**Purpose**: Validate transformation integrity

**Validations**:
* Row count matching
* Join integrity checks
* Duplicate explosion detection
* Anti-join validation (orphaned records)
* Null spike detection
* Aggregation validation

**Output**:
* Trust score (0-100%)
* Validation results
* Anomaly detection

### 6. Gold Layer (Curated Zone)
**Location**: `lakeforge/gold/`

**Purpose**: Business-ready aggregated data

**Key Features**:
* Pre-aggregated metrics
* Star/snowflake schemas
* SCD Type 2 for history
* Optimized for query performance
* Trust score reporting

**Tables Example**:
```
gold.customer_metrics
  ├── customer_id
  ├── total_purchases
  ├── avg_order_value
  ├── lifetime_value
  ├── churn_risk_score
  └── last_purchase_date

gold.daily_sales_summary
  ├── date (partition)
  ├── total_revenue
  ├── order_count
  ├── avg_order_value
  └── unique_customers
```

### 7. Metadata & Observability
**Location**: `lakeforge/metadata/`, `lakeforge/observability/`

**Purpose**: Track lineage, metrics, and logs

**Components**:
* Schema drift detection
* Data lineage tracking
* Pipeline metrics
* Logging framework
* Monitoring dashboards

### 8. Reporting Engine
**Location**: `lakeforge/reporting/`

**Purpose**: Generate trust and quality reports

**Outputs**:
* JSON reports (machine-readable)
* HTML reports (human-readable)
* Trust score summaries
* Data quality dashboards

---

## 🔄 Data Flow Patterns

### Pattern 1: Batch Processing (Daily Loads)
```
Source Database
  → Bronze (Full/Incremental)
  → DQ Validation
  → Silver (MERGE INTO)
  → Trust Validation
  → Gold (Aggregations)
```

### Pattern 2: Streaming Processing
```
Kafka Topic
  → Auto Loader (Bronze stream)
  → Real-time DQ checks
  → Silver (Streaming MERGE)
  → Micro-batch aggregations (Gold)
```

### Pattern 3: CDC (Change Data Capture)
```
Oracle CDC Feed
  → Bronze (Append CDC events)
  → Silver (Apply CDC: INSERT/UPDATE/DELETE)
  → Gold (Refresh aggregations)
```

---

## 🛠️ Technology Stack

### Core Technologies
* **Databricks** - Unified analytics platform
* **Apache Spark** - Distributed processing
* **Delta Lake** - ACID transactions & time travel
* **Unity Catalog** - Unified governance

### Storage Formats
* **Delta** - Primary format (Bronze, Silver, Gold)
* **Parquet** - Archived data
* **JSON/CSV** - Raw source formats

### Programming Languages
* **Python** - Primary language
* **SQL** - Queries and transformations
* **Bash** - Automation scripts

---

## 📊 Design Principles

### 1. **Separation of Concerns**
Each layer has a clear purpose:
* Bronze = Raw storage
* Silver = Quality & cleansing
* Gold = Business logic

### 2. **Idempotency**
Pipelines can be re-run safely:
* MERGE operations instead of INSERT
* Deduplication logic
* Watermark tracking

### 3. **Schema Evolution**
Support changing data structures:
* Schema drift detection
* Auto schema merge
* Backward compatibility

### 4. **Data Quality First**
Quality checks at every stage:
* DQ Engine at Bronze → Silver
* Trust Engine at Silver → Gold
* Quarantine for failures

### 5. **Observability**
Track everything:
* Audit columns on all tables
* Pipeline metrics
* Data lineage
* Error logging

---

## 🔐 Security & Governance

### Unity Catalog Integration
* Catalog-based organization
* Schema-level access control
* Column-level masking
* Row-level filtering

### Audit Trail
* Who ingested the data
* When it was loaded
* Source system tracking
* Transformation lineage

### Data Quality Gates
* Automated validation
* Quarantine mechanism
* Alert on threshold breaches
* Trust score reporting

---

## 📈 Scalability

### Horizontal Scaling
* Spark distributed processing
* Auto-scaling clusters
* Partition pruning

### Vertical Optimization
* Delta Lake optimizations
* Z-ordering for queries
* Liquid clustering
* Photon engine

### Storage Optimization
* Data compaction
* Vacuum old files
* Partition management

---

## 🚀 Deployment

### Development
```
Catalog: lakeforge_dev
Schemas: bronze, silver, gold
Compute: Shared dev cluster
```

### Staging
```
Catalog: lakeforge_staging
Schemas: bronze, silver, gold
Compute: Dedicated staging cluster
```

### Production
```
Catalog: lakeforge_prod
Schemas: bronze, silver, gold
Compute: Auto-scaling prod cluster
Schedule: Databricks Workflows
```

---

## 📚 Related Documentation

* [Ingestion Guide](../ingestion/README.md)
* [Data Quality Rules](../dq-rules/README.md)
* [Testing Strategy](../testing/TESTING_GUIDE.md)
* [Getting Started](../getting-started/PHASE1_GUIDE.md)
* [Examples](../examples/)

---

**For detailed implementation, see the `/pipelines` folder and example notebooks.**
