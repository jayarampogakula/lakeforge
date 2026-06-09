"""
Filesystem Loader for LakeForge
Supports loading files from local filesystem, DBFS, S3, Azure, GCS
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, List
import logging

class FilesystemLoader:
    """Load files from various filesystem sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def load_directory(
        self,
        path: str,
        file_format: str = "parquet",
        recursive: bool = False,
        **options
    ) -> DataFrame:
        """
        Load all files from a directory
        
        Args:
            path: Directory path (supports s3://, wasbs://, gs://, dbfs:/, /Volumes/)
            file_format: File format (parquet, csv, json, avro, orc, delta)
            recursive: Whether to scan subdirectories recursively
            **options: Format-specific options
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading {file_format} files from: {path}")
        
        reader = self.spark.read.format(file_format)
        
        if recursive:
            reader = reader.option("recursiveFileLookup", "true")
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)
    
    def load_with_autoloader(
        self,
        source_path: str,
        file_format: str = "json",
        schema_location: Optional[str] = None,
        **options
    ) -> DataFrame:
        """
        Load files using Auto Loader for incremental ingestion
        
        Args:
            source_path: Source directory path
            file_format: File format
            schema_location: Path to store inferred schema
            **options: Auto Loader options
        
        Returns:
            Streaming DataFrame
        """
        self.logger.info(f"Setting up Auto Loader for: {source_path}")
        
        reader = (self.spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", file_format))
        
        if schema_location:
            reader = reader.option("cloudFiles.schemaLocation", schema_location)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(source_path)
    
    def list_files(self, path: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in a directory
        
        Args:
            path: Directory path
            pattern: File pattern filter (e.g., "*.parquet")
        
        Returns:
            List of file paths
        """
        try:
            files = dbutils.fs.ls(path)
            file_list = [f.path for f in files if not f.isDir()]
            
            if pattern:
                import fnmatch
                file_list = [f for f in file_list if fnmatch.fnmatch(f, pattern)]
            
            return file_list
        except Exception as e:
            self.logger.error(f"Error listing files: {e}")
            return []
