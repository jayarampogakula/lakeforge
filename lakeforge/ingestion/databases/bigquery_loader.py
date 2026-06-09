"""
Google BigQuery Loader for LakeForge
Load data from BigQuery using Spark BigQuery connector
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class BigQueryLoader:
    """Load data from Google BigQuery"""
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        credentials_path: Optional[str] = None,
        temp_gcs_bucket: Optional[str] = None
    ):
        """
        Initialize BigQuery loader
        
        Args:
            spark: SparkSession instance
            project_id: GCP project ID
            credentials_path: Path to service account JSON key
            temp_gcs_bucket: GCS bucket for temporary data
        """
        self.spark = spark
        self.project_id = project_id
        self.credentials_path = credentials_path
        self.temp_gcs_bucket = temp_gcs_bucket
        self.logger = logging.getLogger(__name__)
        
        # Set GCP credentials if provided
        if credentials_path:
            self.spark.conf.set("google.cloud.auth.service.account.json.keyfile", credentials_path)
    
    def load_table(
        self,
        table: str,
        dataset: Optional[str] = None,
        filter_clause: Optional[str] = None
    ) -> DataFrame:
        """
        Load table from BigQuery
        
        Args:
            table: Table name or fully qualified (project.dataset.table)
            dataset: Dataset name (if not in table name)
            filter_clause: SQL WHERE clause for server-side filtering
        
        Returns:
            DataFrame
        """
        # Build fully qualified table name
        if '.' not in table and dataset:
            full_table = f"{self.project_id}.{dataset}.{table}"
        elif table.count('.') == 1:
            full_table = f"{self.project_id}.{table}"
        else:
            full_table = table
        
        self.logger.info(f"Loading BigQuery table: {full_table}")
        
        reader = self.spark.read.format("bigquery")
        reader = reader.option("table", full_table)
        
        if self.temp_gcs_bucket:
            reader = reader.option("temporaryGcsBucket", self.temp_gcs_bucket)
        
        if filter_clause:
            reader = reader.option("filter", filter_clause)
        
        return reader.load()
    
    def load_query(self, query: str) -> DataFrame:
        """
        Load data using SQL query
        
        Args:
            query: BigQuery SQL query
        
        Returns:
            DataFrame
        """
        self.logger.info("Executing BigQuery query")
        
        reader = self.spark.read.format("bigquery")
        reader = reader.option("query", query)
        reader = reader.option("project", self.project_id)
        
        if self.temp_gcs_bucket:
            reader = reader.option("temporaryGcsBucket", self.temp_gcs_bucket)
        
        return reader.load()
    
    def load_partitioned_table(
        self,
        table: str,
        partition_field: str,
        partition_filter: str
    ) -> DataFrame:
        """
        Load partitioned BigQuery table with filter
        
        Args:
            table: Table name
            partition_field: Partition field name
            partition_filter: Partition filter (e.g., "2024-01-01")
        
        Returns:
            DataFrame
        """
        filter_clause = f"{partition_field} = '{partition_filter}'"
        return self.load_table(table, filter_clause=filter_clause)
