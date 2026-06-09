"""
LakeForge JSON Loader Module
Loads JSON files into Spark DataFrames.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any

class JSONLoader:
    """
    Loader for JSON data sources.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize JSON Loader.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        
    def load(
        self,
        path: str,
        multi_line: bool = True,
        schema: Optional[Any] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Load JSON file into Spark DataFrame.
        
        Args:
            path: Path to JSON file
            multi_line: Whether JSON is multi-line (one object per file or multiple lines)
            schema: Optional schema to apply
            options: Additional read options
            
        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("json")
        
        # Apply multiLine option
        reader = reader.option("multiLine", str(multi_line).lower())
        
        if schema:
            reader = reader.schema(schema)
            
        if options:
            reader = reader.options(**options)
            
        return reader.load(path)


def create_json_loader(spark: Optional[SparkSession] = None) -> JSONLoader:
    """
    Factory function for JSONLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-JSON-Loader").getOrCreate()
    return JSONLoader(spark)
