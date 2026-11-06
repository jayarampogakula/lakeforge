"""
LakeForge Delta Loader Module
Handles Delta Lake table/file ingestion with time-travel support.
"""
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame


class DeltaLoader:
    """
    Delta Lake loader supporting table paths, Delta directories, and time travel.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(
        self,
        file_path: str,
        version_as_of: Optional[int] = None,
        timestamp_as_of: Optional[str] = None,
        schema: Optional[Any] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """
        Load Delta table into a Spark DataFrame.

        Args:
            file_path: Delta table path or UC table location
            version_as_of: Time travel - load table state at this version
            timestamp_as_of: Time travel - load table state at this timestamp
            schema: Optional explicit schema to apply
            options: Additional Delta reader options

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("delta")

        if version_as_of is not None:
            reader = reader.option("versionAsOf", str(version_as_of))
        if timestamp_as_of is not None:
            reader = reader.option("timestampAsOf", timestamp_as_of)

        if schema:
            reader = reader.schema(schema)

        if options:
            reader = reader.options(**options)

        return reader.load(file_path)

    def load_table(self, table_name: str) -> DataFrame:
        """
        Load a registered Delta (or UC) table by name.

        Args:
            table_name: Fully qualified table name (catalog.schema.table)

        Returns:
            Spark DataFrame
        """
        return self.spark.table(table_name)

    def get_sample_data(self, file_path: str, num_rows: int = 100, **load_options) -> DataFrame:
        """
        Load a sample of Delta data for preview/validation.
        """
        return self.load(file_path, **load_options).limit(num_rows)


def create_delta_loader(spark: Optional[SparkSession] = None) -> DeltaLoader:
    """
    Factory function for DeltaLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Delta-Loader").getOrCreate()
    return DeltaLoader(spark)
