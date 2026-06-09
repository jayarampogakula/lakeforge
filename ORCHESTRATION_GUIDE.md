# LakeForge Orchestration Guide

## 📁 Current Folder Structure Analysis

### `/pipelines` Folder (Production Pipelines)
Contains the main ETL pipeline notebooks for Bronze → Silver → Gold:

* ✅ **BRONZE_Ingestion_Pipeline** - COMPLETE (19 cells with full implementation)
* ⚠️  **SILVER_Transformation_Pipeline** - EMPTY (needs implementation)
* ⚠️  **GOLD_Aggregation_Pipeline** - EMPTY (needs implementation)
* ✅ **BRONZE_Ingestion_Pipeline_COMPLETE** - Reference/backup version
* ✅ **01_bronze_ingestion_pipeline.py** - Python script version

### `/notebooks` Folder (Examples & Demos)
Contains educational and testing notebooks:

* `/notebooks/demo/` - Example pipelines and guides
  * `end_to_end_pipeline.py` - Complete working example
  * `simple_pipeline_guide.py` - Beginner guide
* `/notebooks/onboarding/` - Onboarding materials
* `/notebooks/troubleshooting/` - Debug notebooks
* `/notebooks/tests/` - Test scenarios

---

## 🚨 What's Missing

### 1. Silver Transformation Pipeline
**Status**: Notebook exists but is **EMPTY**

**Should Include**:
* Data cleansing and standardization
* DQ validation enforcement (reject/quarantine bad records)
* Business logic transformations
* Type casting and formatting
* Deduplication
* Trust Engine validations (join integrity, row count checks)
* Incremental merge logic

### 2. Gold Aggregation Pipeline
**Status**: Notebook exists but is **EMPTY**

**Should Include**:
* Business metric calculations
* Aggregations and roll-ups
* Star/snowflake schema creation
* Slowly Changing Dimensions (SCD Type 2)
* Final Trust Score reporting
* Optimized query structures

### 3. Orchestration Job
**Status**: Does **NOT** exist

**Should Include**:
* Databricks Workflow/Job definition
* Task dependencies (Bronze → Silver → Gold)
* Error handling and retries
* Parameter passing between notebooks
* Schedule configuration
* Alerting on failures

---

## 🎯 Recommended Architecture

### Option 1: Databricks Workflows (Jobs)
**Best for**: Production environments with scheduling

```
Job: "LakeForge_ETL_Pipeline"
├── Task 1: Bronze Ingestion
│   ├── Notebook: BRONZE_Ingestion_Pipeline
│   ├── Timeout: 30 minutes
│   └── Retries: 2
├── Task 2: Silver Transformation (depends on Task 1)
│   ├── Notebook: SILVER_Transformation_Pipeline
│   ├── Timeout: 45 minutes
│   └── Retries: 2
└── Task 3: Gold Aggregation (depends on Task 2)
    ├── Notebook: GOLD_Aggregation_Pipeline
    ├── Timeout: 30 minutes
    └── Retries: 2

Schedule: Daily at 2:00 AM UTC
Notifications: Email on failure
```

### Option 2: Delta Live Tables (DLT)
**Best for**: Streaming/continuous pipelines

```python
# In a single DLT pipeline notebook
import dlt

@dlt.table(name="bronze_customers")
def bronze_customers():
    return spark.read.csv("/data/customers.csv")

@dlt.table(name="silver_customers")
def silver_customers():
    return dlt.read("bronze_customers").where(...)

@dlt.table(name="gold_customer_metrics")
def gold_customer_metrics():
    return dlt.read("silver_customers").groupBy(...)
```

### Option 3: Orchestrator Notebook
**Best for**: Simple workflows or testing

```python
# Master notebook that calls all three
dbutils.notebook.run("BRONZE_Ingestion_Pipeline", timeout_seconds=3600)
dbutils.notebook.run("SILVER_Transformation_Pipeline", timeout_seconds=3600)
dbutils.notebook.run("GOLD_Aggregation_Pipeline", timeout_seconds=3600)
```

---

## 📋 How to Create a Databricks Job

### Method 1: Using Databricks UI

1. **Navigate to Workflows**
   * Go to Databricks workspace
   * Click "Workflows" in left sidebar
   * Click "Create Job"

2. **Configure Job**
   * **Job Name**: `LakeForge_ETL_Pipeline`
   * **Description**: "Bronze → Silver → Gold ETL pipeline"

3. **Add Task 1 (Bronze)**
   * **Task name**: `01_bronze_ingestion`
   * **Type**: Notebook
   * **Source**: Workspace
   * **Path**: `/Users/jayarampogakula@gmail.com/lakeforge/pipelines/BRONZE_Ingestion_Pipeline`
   * **Cluster**: Existing cluster or create new job cluster
   * **Parameters** (optional):
     ```json
     {
       "catalog": "lakeforge_dev",
       "schema": "bronze",
       "data_path": "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/data"
     }
     ```
   * **Timeout**: 30 minutes
   * **Retries**: 2

4. **Add Task 2 (Silver)**
   * **Task name**: `02_silver_transformation`
   * **Type**: Notebook
   * **Source**: Workspace
   * **Path**: `/Users/jayarampogakula@gmail.com/lakeforge/pipelines/SILVER_Transformation_Pipeline`
   * **Depends on**: `01_bronze_ingestion` ✅
   * **Cluster**: Same as Bronze (recommended for efficiency)
   * **Parameters**:
     ```json
     {
       "source_catalog": "lakeforge_dev",
       "source_schema": "bronze",
       "target_catalog": "lakeforge_dev",
       "target_schema": "silver"
     }
     ```
   * **Timeout**: 45 minutes
   * **Retries**: 2

5. **Add Task 3 (Gold)**
   * **Task name**: `03_gold_aggregation`
   * **Type**: Notebook
   * **Source**: Workspace
   * **Path**: `/Users/jayarampogakula@gmail.com/lakeforge/pipelines/GOLD_Aggregation_Pipeline`
   * **Depends on**: `02_silver_transformation` ✅
   * **Cluster**: Same cluster
   * **Parameters**:
     ```json
     {
       "source_catalog": "lakeforge_dev",
       "source_schema": "silver",
       "target_catalog": "lakeforge_dev",
       "target_schema": "gold"
     }
     ```
   * **Timeout**: 30 minutes
   * **Retries**: 2

6. **Configure Schedule** (optional)
   * **Trigger type**: Scheduled
   * **Schedule**: Cron expression
     * Daily: `0 0 2 * * ?` (2 AM daily)
     * Hourly: `0 0 * * * ?`
     * Every 6 hours: `0 0 */6 * * ?`
   * **Timezone**: Your timezone

7. **Configure Notifications**
   * **On failure**: Send email to team
   * **On success**: Optional
   * **Email addresses**: your-team@company.com

8. **Save and Run**
   * Click "Create"
   * Test with "Run now"

---

### Method 2: Using Databricks CLI

Create a job configuration file:

```bash
cat > lakeforge_job.json << 'JSON'
{
  "name": "LakeForge_ETL_Pipeline",
  "email_notifications": {
    "on_failure": ["your-email@company.com"]
  },
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "01_bronze_ingestion",
      "notebook_task": {
        "notebook_path": "/Users/jayarampogakula@gmail.com/lakeforge/pipelines/BRONZE_Ingestion_Pipeline",
        "base_parameters": {
          "catalog": "lakeforge_dev",
          "schema": "bronze"
        }
      },
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      },
      "timeout_seconds": 1800,
      "max_retries": 2
    },
    {
      "task_key": "02_silver_transformation",
      "depends_on": [
        {
          "task_key": "01_bronze_ingestion"
        }
      ],
      "notebook_task": {
        "notebook_path": "/Users/jayarampogakula@gmail.com/lakeforge/pipelines/SILVER_Transformation_Pipeline",
        "base_parameters": {
          "source_catalog": "lakeforge_dev",
          "source_schema": "bronze",
          "target_catalog": "lakeforge_dev",
          "target_schema": "silver"
        }
      },
      "job_cluster_key": "shared_cluster",
      "timeout_seconds": 2700,
      "max_retries": 2
    },
    {
      "task_key": "03_gold_aggregation",
      "depends_on": [
        {
          "task_key": "02_silver_transformation"
        }
      ],
      "notebook_task": {
        "notebook_path": "/Users/jayarampogakula@gmail.com/lakeforge/pipelines/GOLD_Aggregation_Pipeline",
        "base_parameters": {
          "source_catalog": "lakeforge_dev",
          "source_schema": "silver",
          "target_catalog": "lakeforge_dev",
          "target_schema": "gold"
        }
      },
      "job_cluster_key": "shared_cluster",
      "timeout_seconds": 1800,
      "max_retries": 2
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/New_York",
    "pause_status": "UNPAUSED"
  }
}
JSON

# Create the job
databricks jobs create --json-file lakeforge_job.json
```

---

### Method 3: Using Python API

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create job
job = w.jobs.create(
    name="LakeForge_ETL_Pipeline",
    tasks=[
        jobs.Task(
            task_key="01_bronze_ingestion",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/jayarampogakula@gmail.com/lakeforge/pipelines/BRONZE_Ingestion_Pipeline",
                base_parameters={"catalog": "lakeforge_dev"}
            ),
            new_cluster=jobs.ClusterSpec(
                spark_version="14.3.x-scala2.12",
                node_type_id="i3.xlarge",
                num_workers=2
            )
        ),
        jobs.Task(
            task_key="02_silver_transformation",
            depends_on=[jobs.TaskDependency(task_key="01_bronze_ingestion")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/jayarampogakula@gmail.com/lakeforge/pipelines/SILVER_Transformation_Pipeline"
            ),
            job_cluster_key="shared_cluster"
        ),
        jobs.Task(
            task_key="03_gold_aggregation",
            depends_on=[jobs.TaskDependency(task_key="02_silver_transformation")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Users/jayarampogakula@gmail.com/lakeforge/pipelines/GOLD_Aggregation_Pipeline"
            ),
            job_cluster_key="shared_cluster"
        )
    ],
    email_notifications=jobs.JobEmailNotifications(
        on_failure=["your-email@company.com"]
    )
)

print(f"Created job: {job.job_id}")
```

---

## 🔄 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│  CSV Files, Databases, APIs, Kafka, Cloud Storage           │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Data + Audit Tracking)                   │
│  ├── BRONZE_Ingestion_Pipeline                             │
│  ├── Schema validation                                      │
│  ├── Initial DQ checks                                      │
│  └── Quarantine bad records                                 │
│                                                              │
│  Tables: bronze.customers, bronze.transactions              │
└──────────────────────┬───────────────────────────────────────┘
                       │ DQ Engine validates data quality
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleansed & Validated)                        │
│  ├── SILVER_Transformation_Pipeline                        │
│  ├── Data cleansing                                         │
│  ├── Type casting & standardization                         │
│  ├── Deduplication                                          │
│  └── Business rules enforcement                             │
│                                                              │
│  Tables: silver.customers, silver.transactions              │
└──────────────────────┬───────────────────────────────────────┘
                       │ Trust Engine validates transformations
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Business Metrics & Analytics-Ready)            │
│  ├── GOLD_Aggregation_Pipeline                             │
│  ├── Aggregations & metrics                                 │
│  ├── Star schema / Data marts                               │
│  ├── SCD Type 2                                             │
│  └── Trust Score reporting                                  │
│                                                              │
│  Tables: gold.customer_metrics, gold.daily_transactions     │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            CONSUMPTION LAYER                                 │
│  Dashboards, BI Tools, ML Models, APIs                      │
└─────────────────────────────────────────────────────────────┘
```

---

## ✅ Next Steps

1. **Complete SILVER pipeline** - I'll create it for you
2. **Complete GOLD pipeline** - I'll create it for you
3. **Create Databricks Job** - Follow the guide above
4. **Test end-to-end flow** - Run manually first
5. **Schedule for production** - Set up daily runs
6. **Add monitoring** - Configure alerts

---

## 📊 Parameter Passing Between Notebooks

Use widgets to accept parameters:

```python
# In each notebook, add at top:
dbutils.widgets.text("catalog", "lakeforge_dev", "Catalog Name")
dbutils.widgets.text("schema", "bronze", "Schema Name")

# Get values:
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
```

Then pass from job configuration or parent notebook:

```python
# In orchestrator notebook:
result = dbutils.notebook.run(
    "BRONZE_Ingestion_Pipeline",
    timeout_seconds=3600,
    arguments={"catalog": "lakeforge_dev", "schema": "bronze"}
)
```

---

**Ready to implement?** Let me know and I'll create the complete SILVER and GOLD pipelines!
