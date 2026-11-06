"""
LakeForge ORC Loader Module
Handles ORC file ingestion using Spark's built-in ORC data source.
"""
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame


class ORCLoader:
    """
    ORC file loader (columnar format, optimized for heavy scans).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(
        self,
        file_path: str,
        schema: Optional[Any] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """
        Load ORC file into a Spark DataFrame.

        Args:
            file_path: Path to ORC file or directory
            schema: Optional explicit schema to apply
            options: Additional ORC reader options

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("orc")

        if schema:
            reader = reader.schema(schema)

        if options:
            reader = reader.options(**options)

        return reader.load(file_path)

    def get_sample_data(self, file_path: str, num_rows: int = 100, **load_options) -> DataFrame:
        """
        Load a sample of ORC data for preview/validation.
        """
        return self.load(file_path, **load_options).limit(num_rows)


def create_orc_loader(spark: Optional[SparkSession] = None) -> ORCLoader:
    """
    Factory function for ORCLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-ORC-Loader").getOrCreate()
    return ORCLoader(spark)
