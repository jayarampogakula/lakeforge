"""
Google Cloud Storage Loader for LakeForge
Load files from GCS buckets
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class GCSLoader:
    """Load files from Google Cloud Storage"""
    
    def __init__(
        self,
        spark: SparkSession,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize GCS loader
        
        Args:
            spark: SparkSession instance
            credentials_path: Path to service account JSON key
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        
        if credentials_path:
            self.spark.conf.set("google.cloud.auth.service.account.json.keyfile", credentials_path)
    
    def load(
        self,
        path: str,
        file_format: str = "parquet",
        **options
    ) -> DataFrame:
        """
        Load files from GCS
        
        Args:
            path: GCS path (gs://bucket/path/)
            file_format: File format (parquet, csv, json, avro, orc)
            **options: Format-specific options
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading {file_format} from GCS: {path}")
        
        reader = self.spark.read.format(file_format)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)
    
    def load_csv(
        self,
        path: str,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        **options
    ) -> DataFrame:
        """Load CSV files from GCS"""
        return self.load(
            path,
            file_format="csv",
            header=header,
            inferSchema=infer_schema,
            delimiter=delimiter,
            **options
        )
    
    def load_json(
        self,
        path: str,
        multiline: bool = False,
        **options
    ) -> DataFrame:
        """Load JSON files from GCS"""
        return self.load(
            path,
            file_format="json",
            multiLine=multiline,
            **options
        )
