"""
Parquet File Loader for LakeForge
Supports loading Parquet files from various storage locations
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class ParquetLoader:
    """Load Parquet files with schema evolution support"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def load(
        self,
        path: str,
        merge_schema: bool = True,
        partition_columns: Optional[list] = None,
        **options
    ) -> DataFrame:
        """
        Load Parquet files
        
        Args:
            path: Path to parquet file(s) - supports wildcards
            merge_schema: Whether to merge schemas across files
            partition_columns: Partition columns if data is partitioned
            **options: Additional Spark read options
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading Parquet from: {path}")
        
        reader = self.spark.read.format("parquet")
        
        if merge_schema:
            reader = reader.option("mergeSchema", "true")
        
        # Apply additional options
        for key, value in options.items():
            reader = reader.option(key, value)
        
        df = reader.load(path)
        
        self.logger.info(f"Loaded {df.count()} rows from Parquet")
        return df
    
    def load_partitioned(
        self,
        base_path: str,
        partition_filter: Optional[str] = None
    ) -> DataFrame:
        """
        Load partitioned Parquet data with optional filter
        
        Args:
            base_path: Base path to partitioned data
            partition_filter: Partition filter (e.g., "year=2024/month=01")
        
        Returns:
            DataFrame
        """
        if partition_filter:
            path = f"{base_path}/{partition_filter}"
        else:
            path = base_path
        
        return self.load(path, merge_schema=True)
