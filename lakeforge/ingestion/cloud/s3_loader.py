"""
AWS S3 Loader for LakeForge
Load files from Amazon S3 buckets
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class S3Loader:
    """Load files from AWS S3"""
    
    def __init__(
        self,
        spark: SparkSession,
        aws_access_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        aws_session_token: Optional[str] = None
    ):
        """
        Initialize S3 loader
        
        Args:
            spark: SparkSession instance
            aws_access_key: AWS access key ID (optional if using IAM roles)
            aws_secret_key: AWS secret access key (optional if using IAM roles)
            aws_session_token: AWS session token for temporary credentials
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        
        # Configure AWS credentials if provided
        if aws_access_key and aws_secret_key:
            self.spark.conf.set("fs.s3a.access.key", aws_access_key)
            self.spark.conf.set("fs.s3a.secret.key", aws_secret_key)
            
            if aws_session_token:
                self.spark.conf.set("fs.s3a.session.token", aws_session_token)
    
    def load(
        self,
        path: str,
        file_format: str = "parquet",
        **options
    ) -> DataFrame:
        """
        Load files from S3
        
        Args:
            path: S3 path (s3://bucket/path/ or s3a://bucket/path/)
            file_format: File format (parquet, csv, json, avro, orc, delta)
            **options: Format-specific options
        
        Returns:
            DataFrame
        """
        # Ensure s3a:// protocol
        if path.startswith("s3://"):
            path = path.replace("s3://", "s3a://")
        
        self.logger.info(f"Loading {file_format} from S3: {path}")
        
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
        """Load CSV files from S3"""
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
        """Load JSON files from S3"""
        return self.load(
            path,
            file_format="json",
            multiLine=multiline,
            **options
        )
    
    def load_with_autoloader(
        self,
        source_path: str,
        file_format: str = "json",
        schema_location: str = None,
        **options
    ) -> DataFrame:
        """
        Load files using Auto Loader for incremental ingestion
        
        Args:
            source_path: S3 source path
            file_format: File format
            schema_location: Path to store inferred schema
            **options: Auto Loader options
        
        Returns:
            Streaming DataFrame
        """
        if source_path.startswith("s3://"):
            source_path = source_path.replace("s3://", "s3a://")
        
        self.logger.info(f"Setting up Auto Loader for S3: {source_path}")
        
        reader = (self.spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", file_format)
                  .option("cloudFiles.useNotifications", "false"))
        
        if schema_location:
            reader = reader.option("cloudFiles.schemaLocation", schema_location)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(source_path)
