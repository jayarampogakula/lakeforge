"""
LakeForge Schema Detector Module
Detects schemas from raw files using Spark schema inference.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Optional

class SchemaDetector:
    """
    Utility class to dynamically detect and infer schema from raw files.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Schema Detector.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        
    def detect_csv_schema(
        self,
        path: str,
        header: bool = True,
        delimiter: str = ","
    ) -> StructType:
        """Detect schema from CSV file."""
        return self.spark.read.format("csv") \
            .option("header", str(header).lower()) \
            .option("delimiter", delimiter) \
            .option("inferSchema", "true") \
            .load(path).schema
            
    def detect_json_schema(
        self,
        path: str,
        multi_line: bool = True
    ) -> StructType:
        """Detect schema from JSON file."""
        return self.spark.read.format("json") \
            .option("multiLine", str(multi_line).lower()) \
            .load(path).schema
            
    def detect_parquet_schema(self, path: str) -> StructType:
        """Detect schema from Parquet file."""
        return self.spark.read.format("parquet").load(path).schema


def create_schema_detector(spark: Optional[SparkSession] = None) -> SchemaDetector:
    """
    Factory function for SchemaDetector.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Schema-Detector").getOrCreate()
    return SchemaDetector(spark)
