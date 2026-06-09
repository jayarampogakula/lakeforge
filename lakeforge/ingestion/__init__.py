"""
LakeForge Ingestion Module
Data loaders for various file formats
"""

from lakeforge.ingestion.csv_loader import CSVLoader, create_csv_loader
from lakeforge.ingestion.excel_loader import ExcelLoader, create_excel_loader
from lakeforge.ingestion.json_loader import JSONLoader, create_json_loader
from lakeforge.ingestion.schema_detector import SchemaDetector, create_schema_detector

from lakeforge.ingestion.api_loader import APILoader, create_api_loader
from lakeforge.ingestion.azure_sql_loader import AzureSQLLoader
from lakeforge.ingestion.bigquery_loader import BigQueryLoader
from lakeforge.ingestion.filesystem_loader import FilesystemLoader
from lakeforge.ingestion.gcs_loader import GCSLoader
from lakeforge.ingestion.google_sheets_loader import GoogleSheetsLoader
from lakeforge.ingestion.jira_loader import JiraLoader
from lakeforge.ingestion.kafka_loader import KafkaLoader
from lakeforge.ingestion.mongodb_loader import MongoDBLoader
from lakeforge.ingestion.mysql_loader import MySQLLoader
from lakeforge.ingestion.oracle_loader import OracleLoader
from lakeforge.ingestion.parquet_loader import ParquetLoader
from lakeforge.ingestion.postgres_loader import PostgresLoader
from lakeforge.ingestion.redshift_loader import RedshiftLoader
from lakeforge.ingestion.s3_loader import S3Loader
from lakeforge.ingestion.sharepoint_loader import SharePointLoader
from lakeforge.ingestion.snowflake_loader import SnowflakeLoader
from lakeforge.ingestion.streaming_loader import StreamingLoader, create_streaming_loader

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

