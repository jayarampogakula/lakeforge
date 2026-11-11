"""
LakeForge Incremental File Loading
File-inventory based incremental ingestion: every successful bronze write
records the files it loaded in a Delta inventory table; the next run of the
same source loads ONLY files not yet seen. This gives Auto-Loader-like
"new files only" behaviour for sources Spark's cloudFiles source cannot
handle directly (preamble rows before the header, legacy .xls files that
must be read with pandas).

Inventory table schema (auto-created on first use):
    source      STRING   - ingestion source name
    file_path   STRING   - full path of the ingested file
    ingested_at TIMESTAMP
"""
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class FileInventoryManager:
    """Tracks which files a source has already ingested."""

    _CANDIDATES_VIEW = "_lf_incremental_candidates"

    def __init__(self, spark: SparkSession, table: str):
        """
        Args:
            spark: Active SparkSession
            table: Fully qualified inventory table
                   (e.g. catalog.monitoring.ingested_files)
        """
        self.spark = spark
        self.table = table

    def ensure_table(self) -> "FileInventoryManager":
        """Create the inventory table if it does not exist yet."""
        if not self.spark.catalog.tableExists(self.table):
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    source STRING,
                    file_path STRING,
                    ingested_at TIMESTAMP
                )
                """
            )
        return self

    def list_new_files(self, source: str, files: List[str]) -> List[str]:
        """
        Return the subset of `files` that `source` has not ingested yet.

        Args:
            source: Ingestion source name
            files: Candidate file paths found under the source's path

        Returns:
            File paths not present in the inventory for this source
        """
        if not files:
            return []
        self.ensure_table()
        candidates = self.spark.createDataFrame(
            [(f,) for f in files], "file_path STRING"
        )
        candidates.createOrReplaceTempView(self._CANDIDATES_VIEW)
        try:
            new_files = self.spark.sql(
                f"""
                SELECT c.file_path
                FROM {self._CANDIDATES_VIEW} c
                LEFT ANTI JOIN {self.table} inv
                  ON inv.source = '{source}' AND inv.file_path = c.file_path
                ORDER BY c.file_path
                """
            )
            return [row.file_path for row in new_files.collect()]
        finally:
            self.spark.catalog.dropTempView(self._CANDIDATES_VIEW)

    def record_files(self, source: str, files: List[str]) -> None:
        """
        Mark `files` as ingested for `source`.
        Called only AFTER a successful bronze write, so a failed run
        re-processes its files on the next attempt.
        """
        if not files:
            return
        self.ensure_table()
        rows = self.spark.createDataFrame(
            [(source, f) for f in files], "source STRING, file_path STRING"
        ).withColumn("ingested_at", F.current_timestamp())
        rows.write.mode("append").saveAsTable(self.table)
