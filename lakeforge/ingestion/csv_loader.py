"""
LakeForge CSV Loader Module
Handles CSV file ingestion with schema detection and encoding handling.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from typing import Optional, Dict, Any, List
import chardet
from pathlib import Path


class CSVLoader:
    """
    CSV file loader with automatic schema detection and encoding handling.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize CSV Loader.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def detect_encoding(self, file_path: str, sample_size: int = 10000) -> str:
        """
        Detect file encoding using chardet.
        
        Args:
            file_path: Path to CSV file
            sample_size: Number of bytes to sample
            
        Returns:
            Detected encoding (e.g., 'utf-8', 'iso-8859-1')
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except Exception:
            return 'utf-8'  # Default fallback
    
    def load_csv(
        self,
        file_path: str,
        header: bool = True,
        delimiter: str = ",",
        encoding: Optional[str] = None,
        infer_schema: bool = True,
        schema: Optional[StructType] = None,
        multiline: bool = False,
        quote_char: str = '"',
        escape_char: str = "\\",
        null_value: str = "",
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        **options
    ) -> DataFrame:
        """
        Load CSV file into Spark DataFrame.
        
        Args:
            file_path: Path to CSV file (local or cloud storage)
            header: Whether first row contains headers
            delimiter: Field delimiter
            encoding: File encoding (auto-detected if None)
            infer_schema: Whether to infer schema from data
            schema: Explicit schema (overrides infer_schema)
            multiline: Handle multiline fields
            quote_char: Character used for quoting
            escape_char: Escape character
            null_value: String representation of null
            date_format: Format for date columns
            timestamp_format: Format for timestamp columns
            **options: Additional Spark CSV reader options
            
        Returns:
            Spark DataFrame
        """
        # Auto-detect encoding if not provided and file is local
        if encoding is None and Path(file_path).exists():
            encoding = self.detect_encoding(file_path)
        elif encoding is None:
            encoding = 'utf-8'
        
        # Build reader options
        reader_options = {
            'header': 'true' if header else 'false',
            'delimiter': delimiter,
            'encoding': encoding,
            'inferSchema': 'true' if (infer_schema and schema is None) else 'false',
            'multiLine': 'true' if multiline else 'false',
            'quote': quote_char,
            'escape': escape_char,
            'nullValue': null_value,
            'mode': 'PERMISSIVE',  # Handle corrupt records
            'columnNameOfCorruptRecord': '_corrupt_record'
        }
        
        if date_format:
            reader_options['dateFormat'] = date_format
        
        if timestamp_format:
            reader_options['timestampFormat'] = timestamp_format
        
        # Merge with additional options
        reader_options.update(options)
        
        # Create reader
        reader = self.spark.read.format('csv')
        
        # Apply options
        for key, value in reader_options.items():
            reader = reader.option(key, value)
        
        # Apply schema if provided
        if schema:
            reader = reader.schema(schema)
        
        # Load data
        df = reader.load(file_path)
        
        return df
    
    def load_csv_with_metadata(
        self,
        file_path: str,
        add_source_metadata: bool = True,
        **load_options
    ) -> DataFrame:
        """
        Load CSV with additional source metadata columns.
        
        Args:
            file_path: Path to CSV file
            add_source_metadata: Whether to add metadata columns
            **load_options: Options passed to load_csv
            
        Returns:
            Spark DataFrame with metadata columns
        """
        from pyspark.sql.functions import input_file_name, current_timestamp, lit
        
        df = self.load_csv(file_path, **load_options)
        
        if add_source_metadata:
            df = df.withColumn("_source_file", input_file_name())
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            df = df.withColumn("_source_type", lit("csv"))
        
        return df
    
    def get_sample_data(
        self,
        file_path: str,
        num_rows: int = 100,
        **load_options
    ) -> DataFrame:
        """
        Load a sample of CSV data for preview/validation.
        
        Args:
            file_path: Path to CSV file
            num_rows: Number of rows to sample
            **load_options: Options passed to load_csv
            
        Returns:
            Sampled Spark DataFrame
        """
        df = self.load_csv(file_path, **load_options)
        return df.limit(num_rows)
    
    def load_multiple_csvs(
        self,
        file_pattern: str,
        **load_options
    ) -> DataFrame:
        """
        Load multiple CSV files matching a pattern.
        
        Args:
            file_pattern: Glob pattern for files (e.g., '/path/*.csv')
            **load_options: Options passed to load_csv
            
        Returns:
            Unified Spark DataFrame
        """
        return self.load_csv(file_pattern, **load_options)
    
    def validate_csv_structure(
        self,
        file_path: str,
        expected_columns: List[str],
        **load_options
    ) -> Dict[str, Any]:
        """
        Validate CSV structure against expected schema.
        
        Args:
            file_path: Path to CSV file
            expected_columns: List of expected column names
            **load_options: Options passed to load_csv
            
        Returns:
            Validation result dictionary
        """
        df = self.load_csv(file_path, **load_options)
        actual_columns = df.columns
        
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        return {
            'valid': len(missing_columns) == 0 and len(extra_columns) == 0,
            'expected_columns': expected_columns,
            'actual_columns': actual_columns,
            'missing_columns': list(missing_columns),
            'extra_columns': list(extra_columns),
            'row_count': df.count()
        }


def create_csv_loader(spark: Optional[SparkSession] = None) -> CSVLoader:
    """
    Factory function to create CSV loader.
    
    Args:
        spark: SparkSession (creates new one if None)
        
    Returns:
        CSVLoader instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-CSV-Loader").getOrCreate()
    
    return CSVLoader(spark)
