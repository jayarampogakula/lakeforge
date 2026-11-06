"""
LakeForge Text Loader Module
Handles plain-text and fixed-width file ingestion.
"""
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Union

from pyspark.sql import SparkSession, DataFrame


class TextLoader:
    """
    Loader for plain text (line-per-row) and fixed-width (positional) files.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_lines(
        self,
        file_path: str,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """
        Load a plain text file where each line becomes one row in a `value` column.

        Args:
            file_path: Path to text file or directory
            options: Additional text reader options (e.g. wholetext)

        Returns:
            Spark DataFrame with a single `value` column
        """
        reader = self.spark.read.format("text")
        if options:
            reader = reader.options(**options)
        return reader.load(file_path)

    def load_fixed_width(
        self,
        file_path: str,
        column_specs: Optional[Dict[str, Tuple[int, int]]] = None,
        header: bool = True,
        encoding: str = "utf-8",
        skip_rows: int = 0,
    ) -> DataFrame:
        """
        Load a fixed-width (positional) text file, e.g. legacy mainframe extracts.

        Args:
            file_path: Path to the fixed-width file (must be locally readable:
                       /Volumes, /Workspace, or driver-local paths)
            column_specs: Mapping of column name -> (start, end) character positions.
                          If None, pandas infers column widths automatically.
            header: Whether the first row contains column names
            encoding: File encoding
            skip_rows: Rows to skip at the start of the file

        Returns:
            Spark DataFrame
        """
        import pandas as pd

        p = Path(file_path)
        if not p.is_file():
            raise FileNotFoundError(
                f"Fixed-width loading requires a locally readable single file, "
                f"got: {file_path}"
            )

        if column_specs:
            # pandas expects colspecs as (start, end) list parallel to names
            colspecs = [column_specs[name] for name in column_specs]
            names = list(column_specs.keys())
            pandas_df = pd.read_fwf(
                p,
                colspecs=colspecs,
                names=names,
                header=0 if header else None,
                skiprows=skip_rows,
                encoding=encoding,
                dtype=str,
            )
        else:
            pandas_df = pd.read_fwf(
                p,
                header=0 if header else None,
                skiprows=skip_rows,
                encoding=encoding,
                dtype=str,
            )

        pandas_df = pandas_df.where(pandas_df.notna(), None)
        return self.spark.createDataFrame(pandas_df)

    def load_delimited_with_positions(
        self,
        file_path: str,
        delimiter: str,
        positions: Optional[List[int]] = None,
        quote_char: str = '"',
        encoding: str = "utf-8",
    ) -> DataFrame:
        """
        Load a delimited file by explicitly specifying character quote/escape handling.
        Fallback for quirky delimited files that the CSV parser rejects.

        Args:
            file_path: Path to the delimited file
            delimiter: Field delimiter
            positions: Optional 0-based field positions to keep
            quote_char: Quote character
            encoding: File encoding

        Returns:
            Spark DataFrame
        """
        reader = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", delimiter)
            .option("quote", quote_char)
            .option("encoding", encoding)
            .option("mode", "PERMISSIVE")
        )
        df = reader.load(file_path)
        if positions is not None:
            df = df.select([df.columns[i] for i in positions])
        return df

    def get_sample_data(self, file_path: str, num_rows: int = 100, **load_options) -> DataFrame:
        """
        Load a sample of text data for preview/validation.
        """
        return self.load_lines(file_path).limit(num_rows)


def create_text_loader(spark: Optional[SparkSession] = None) -> TextLoader:
    """
    Factory function for TextLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Text-Loader").getOrCreate()
    return TextLoader(spark)
