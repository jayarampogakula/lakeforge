"""
LakeForge Ingestion Module
Data loaders for various file formats
"""

from lakeforge.ingestion.files.csv_loader import CSVLoader, create_csv_loader
from lakeforge.ingestion.files.excel_loader import ExcelLoader, create_excel_loader
from lakeforge.ingestion.files.json_loader import JSONLoader, create_json_loader
from lakeforge.ingestion.utilities.schema_detector import SchemaDetector, create_schema_detector

from lakeforge.ingestion.apis.api_loader import APILoader, create_api_loader
from lakeforge.ingestion.databases.azure_sql_loader import AzureSQLLoader
from lakeforge.ingestion.databases.bigquery_loader import BigQueryLoader
from lakeforge.ingestion.files.filesystem_loader import FilesystemLoader
from lakeforge.ingestion.cloud.gcs_loader import GCSLoader
from lakeforge.ingestion.apis.google_sheets_loader import GoogleSheetsLoader
from lakeforge.ingestion.apis.jira_loader import JiraLoader
from lakeforge.ingestion.streaming.kafka_loader import KafkaLoader
from lakeforge.ingestion.databases.mongodb_loader import MongoDBLoader
from lakeforge.ingestion.databases.mysql_loader import MySQLLoader
from lakeforge.ingestion.databases.oracle_loader import OracleLoader
from lakeforge.ingestion.files.parquet_loader import ParquetLoader
from lakeforge.ingestion.databases.postgres_loader import PostgresLoader
from lakeforge.ingestion.databases.redshift_loader import RedshiftLoader
from lakeforge.ingestion.cloud.s3_loader import S3Loader
from lakeforge.ingestion.apis.sharepoint_loader import SharePointLoader
from lakeforge.ingestion.databases.snowflake_loader import SnowflakeLoader
from lakeforge.ingestion.streaming.streaming_loader import StreamingLoader, create_streaming_loader


__all__ = [
    'CSVLoader',
    'create_csv_loader',
    'ExcelLoader',
    'create_excel_loader',
    'JSONLoader',
    'create_json_loader',
    'SchemaDetector',
    'create_schema_detector',
    'APILoader',
    'create_api_loader',
    'AzureSQLLoader',
    'BigQueryLoader',
    'FilesystemLoader',
    'GCSLoader',
    'GoogleSheetsLoader',
    'JiraLoader',
    'KafkaLoader',
    'MongoDBLoader',
    'MySQLLoader',
    'OracleLoader',
    'ParquetLoader',
    'PostgresLoader',
    'RedshiftLoader',
    'S3Loader',
    'SharePointLoader',
    'SnowflakeLoader',
    'StreamingLoader',
    'create_streaming_loader'
]

