"""
LakeForge Avro Loader Module
Handles Avro file ingestion using Spark's built-in Avro data source.
"""
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame


class AvroLoader:
    """
    Avro file loader (reads .avro container files with embedded schemas).
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
        Load Avro file into a Spark DataFrame.

        Args:
            file_path: Path to Avro file or directory
            schema: Optional explicit schema to apply
            options: Additional Avro reader options (e.g. avroSchema)

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("avro")

        if schema:
            reader = reader.schema(schema)

        if options:
            reader = reader.options(**options)

        return reader.load(file_path)

    def get_sample_data(self, file_path: str, num_rows: int = 100, **load_options) -> DataFrame:
        """
        Load a sample of Avro data for preview/validation.
        """
        return self.load(file_path, **load_options).limit(num_rows)


def create_avro_loader(spark: Optional[SparkSession] = None) -> AvroLoader:
    """
    Factory function for AvroLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Avro-Loader").getOrCreate()
    return AvroLoader(spark)
