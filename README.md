# LakeForge

Enterprise-grade Data Lakehouse framework for ingesting, transforming, and managing data across multi-cloud environments.

## Overview

LakeForge is a comprehensive framework for building production-ready data pipelines on Databricks, following the Medallion Architecture (Bronze, Silver, Gold layers).

## Features

* **Multi-Source Ingestion**: 15+ pre-built connectors for databases, cloud storage, SaaS apps, and streaming platforms
* **Medallion Architecture**: Bronze → Silver → Gold data layers with quality gates
* **Schema Management**: Automatic schema detection, evolution, and validation
* **Data Quality**: Built-in validation, profiling, and monitoring
* **Incremental Processing**: CDC, watermark tracking, and efficient updates
* **Cloud Native**: Supports AWS, Azure, and GCP

## Supported Data Sources

### Cloud Data Warehouses
* **Snowflake** - Enterprise data warehouse with query pushdown
* **BigQuery** - Google Cloud's serverless data warehouse
* **Redshift** - AWS data warehouse with optimized UNLOAD

### Relational Databases
* **Oracle** - On-premise and cloud Oracle databases
* **PostgreSQL** - Open-source relational database
* **MySQL/MariaDB** - Popular open-source databases
* **Azure SQL** - Microsoft's cloud database service

### NoSQL & Document Stores
* **MongoDB** - Document database with aggregation pipelines

### Cloud Storage
* **AWS S3** - Amazon object storage with Auto Loader support
* **Google Cloud Storage** - GCP object storage
* **Parquet Files** - Columnar format with schema evolution
* **Filesystem** - DBFS, Volumes, mounted storage

### Streaming & Messaging
* **Apache Kafka** - Real-time event streaming platform

### SaaS & Collaboration
* **Jira** - Atlassian issue tracking and project management
* **SharePoint** - Microsoft document management
* **Google Sheets** - Cloud spreadsheets

## Quick Start

### Installation

```bash
git clone https://github.com/yourorg/lakeforge.git
cd lakeforge
pip install -e .
```

### Basic Usage

```python
from lakeforge.ingestion import OracleLoader
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("LakeForge").getOrCreate()

# Load from Oracle
loader = OracleLoader(
    spark=spark,
    host="oracle.company.com",
    port=1521,
    service_name="PROD",
    user="etl_user",
    password=dbutils.secrets.get("prod", "oracle-password")
)

# Ingest to Bronze layer
df = loader.load_table("SALES.ORDERS")
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .save("/mnt/bronze/orders")
```

### Streaming Example

```python
from lakeforge.ingestion import KafkaLoader

loader = KafkaLoader(spark=spark)

# Stream from Kafka
stream_df = loader.load_stream(
    kafka_bootstrap_servers="kafka:9092",
    topic="orders",
    value_format="json"
)

# Write to Bronze with checkpointing
query = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/orders") \
    .start("/mnt/bronze/streaming_orders")
```

## Project Structure

```
lakeforge/
├── lakeforge/                 # Core Python package
│   ├── ingestion/            # Data ingestion modules
│   │   ├── kafka_loader.py
│   │   ├── oracle_loader.py
│   │   ├── s3_loader.py
│   │   └── ...
│   ├── transformation/       # Data transformation logic
│   ├── validation/          # Data quality validation
│   └── utils/              # Utility functions
├── docs/                    # Documentation
│   ├── ingestion/          # Ingestion guides per source
│   │   ├── README.md       # Ingestion overview
│   │   ├── kafka.md
│   │   ├── oracle.md
│   │   └── ...
│   └── architecture/       # Architecture documentation
├── examples/               # Example notebooks and pipelines
├── tests/                 # Unit and integration tests
└── configs/              # Configuration templates

```

## Documentation

Comprehensive documentation is available in the `docs/` directory:

* **[Ingestion Guide](docs/ingestion/README.md)** - Complete guide for all data sources
* **Architecture Guide** - Medallion architecture patterns
* **Best Practices** - Production deployment guidelines
* **API Reference** - Complete API documentation

### Source-Specific Guides

Detailed configuration and examples for each data source:

* [Kafka Streaming](docs/ingestion/kafka.md)
* [Oracle Database](docs/ingestion/oracle.md)
* [AWS Redshift](docs/ingestion/redshift.md)
* [Google BigQuery](docs/ingestion/bigquery.md)
* [Snowflake](docs/ingestion/snowflake.md)
* [AWS S3](docs/ingestion/s3.md)
* [Jira](docs/ingestion/jira.md)
* [SharePoint](docs/ingestion/sharepoint.md)
* [And more...](docs/ingestion/)

## Medallion Architecture

LakeForge implements the industry-standard Medallion Architecture:

### Bronze Layer (Raw)
* Raw data ingestion from sources
* Minimal transformation
* Preserves original data lineage
* Append-only or full snapshots

### Silver Layer (Cleansed)
* Validated and cleansed data
* Schema enforcement
* Deduplicated records
* Type casting and normalization

### Gold Layer (Curated)
* Business-level aggregations
* Star/snowflake schemas
* Ready for analytics and reporting
* Optimized for query performance

## Configuration Management

Store credentials securely using Databricks Secrets:

```python
# Create secret scope
databricks secrets create-scope --scope production

# Add secrets
databricks secrets put --scope production --key oracle-password

# Use in code
password = dbutils.secrets.get(scope="production", key="oracle-password")
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## Support

* **Documentation**: [docs/](docs/)
* **Issues**: GitHub Issues
* **Discussions**: GitHub Discussions

## Roadmap

* [ ] Additional connectors (Salesforce, ServiceNow, SAP)
* [ ] Data catalog integration
* [ ] Enhanced monitoring and alerting
* [ ] Auto-scaling recommendations
* [ ] Cost optimization tools

---

Built with ❤️ for the Data Engineering community
