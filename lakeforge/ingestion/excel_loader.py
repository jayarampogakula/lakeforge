"""
LakeForge Excel Loader Module
Handles Excel file ingestion with multi-sheet support and normalization.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType
from typing import Optional, Dict, Any, List, Union
import pandas as pd
from pathlib import Path


class ExcelLoader:
    """
    Excel file loader with multi-sheet support and data normalization.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Excel Loader.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def load_excel(
        self,
        file_path: str,
        sheet_name: Union[str, int, List[Union[str, int]], None] = 0,
        header: int = 0,
        use_cols: Optional[List[str]] = None,
        skip_rows: Optional[int] = None,
        na_values: Optional[List[str]] = None,
        dtype: Optional[Dict[str, Any]] = None,
        **pandas_options
    ) -> DataFrame:
        """
        Load Excel file into Spark DataFrame.
        
        Args:
            file_path: Path to Excel file (.xlsx, .xls)
            sheet_name: Sheet name(s), index, or None for all sheets
            header: Row number to use as column names
            use_cols: Columns to parse
            skip_rows: Rows to skip at the start
            na_values: Additional strings to recognize as NA/NaN
            dtype: Data type for columns
            **pandas_options: Additional pandas read_excel options
            
        Returns:
            Spark DataFrame (or dict of DataFrames if multiple sheets)
        """
        # Read with pandas first (Excel files are typically small)
        pandas_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=header,
            usecols=use_cols,
            skiprows=skip_rows,
            na_values=na_values,
            dtype=dtype,
            **pandas_options
        )
        
        # Convert to Spark DataFrame
        if isinstance(pandas_df, dict):
            # Multiple sheets
            spark_dfs = {}
            for sheet, df in pandas_df.items():
                spark_dfs[sheet] = self._pandas_to_spark(df, file_path, sheet)
            return spark_dfs
        else:
            # Single sheet
            return self._pandas_to_spark(pandas_df, file_path, sheet_name)
    
    def _pandas_to_spark(
        self,
        pandas_df: pd.DataFrame,
        file_path: str,
        sheet_name: Union[str, int]
    ) -> DataFrame:
        """
        Convert pandas DataFrame to Spark DataFrame with metadata.
        
        Args:
            pandas_df: Pandas DataFrame
            file_path: Source file path
            sheet_name: Sheet name/index
            
        Returns:
            Spark DataFrame
        """
        from pyspark.sql.functions import lit, current_timestamp
        
        # Handle datetime columns - convert to string to avoid timezone issues
        for col in pandas_df.columns:
            if pd.api.types.is_datetime64_any_dtype(pandas_df[col]):
                pandas_df[col] = pandas_df[col].astype(str)
        
        # Create Spark DataFrame
        spark_df = self.spark.createDataFrame(pandas_df)
        
        # Add metadata
        spark_df = spark_df.withColumn("_source_file", lit(file_path))
        spark_df = spark_df.withColumn("_source_sheet", lit(str(sheet_name)))
        spark_df = spark_df.withColumn("_ingestion_timestamp", current_timestamp())
        spark_df = spark_df.withColumn("_source_type", lit("excel"))
        
        return spark_df
    
    def load_all_sheets(
        self,
        file_path: str,
        normalize: bool = False,
        **load_options
    ) -> Union[Dict[str, DataFrame], DataFrame]:
        """
        Load all sheets from Excel file.
        
        Args:
            file_path: Path to Excel file
            normalize: If True, union all sheets into single DataFrame
            **load_options: Options passed to load_excel
            
        Returns:
            Dict of DataFrames (one per sheet) or single normalized DataFrame
        """
        sheets_dict = self.load_excel(file_path, sheet_name=None, **load_options)
        
        if normalize and isinstance(sheets_dict, dict):
            return self.normalize_sheets(sheets_dict)
        
        return sheets_dict
    
    def normalize_sheets(
        self,
        sheets_dict: Dict[str, DataFrame]
    ) -> DataFrame:
        """
        Normalize multiple sheets into single DataFrame.
        Handles sheets with different schemas by converting to string.
        
        Args:
            sheets_dict: Dictionary of sheet_name -> DataFrame
            
        Returns:
            Normalized Spark DataFrame
        """
        from pyspark.sql.functions import col
        
        # Get all unique columns across all sheets
        all_columns = set()
        for df in sheets_dict.values():
            all_columns.update(df.columns)
        
        # Normalize each sheet to have all columns
        normalized_dfs = []
        for sheet_name, df in sheets_dict.items():
            # Add missing columns as null
            for col_name in all_columns:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast(StringType()) if col_name in df.columns else None)
            
            # Ensure column order is consistent
            df = df.select(sorted(all_columns))
            normalized_dfs.append(df)
        
        # Union all sheets
        result_df = normalized_dfs[0]
        for df in normalized_dfs[1:]:
            result_df = result_df.union(df)
        
        return result_df
    
    def get_sheet_names(self, file_path: str) -> List[str]:
        """
        Get list of sheet names in Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            List of sheet names
        """
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names
    
    def get_sample_data(
        self,
        file_path: str,
        sheet_name: Union[str, int] = 0,
        num_rows: int = 100,
        **load_options
    ) -> DataFrame:
        """
        Load a sample of Excel data for preview/validation.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to sample from
            num_rows: Number of rows to sample
            **load_options: Options passed to load_excel
            
        Returns:
            Sampled Spark DataFrame
        """
        # Use nrows in pandas for efficiency
        load_options['nrows'] = num_rows
        return self.load_excel(file_path, sheet_name=sheet_name, **load_options)
    
    def validate_excel_structure(
        self,
        file_path: str,
        sheet_name: Union[str, int] = 0,
        expected_columns: Optional[List[str]] = None,
        **load_options
    ) -> Dict[str, Any]:
        """
        Validate Excel structure against expected schema.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to validate
            expected_columns: List of expected column names
            **load_options: Options passed to load_excel
            
        Returns:
            Validation result dictionary
        """
        df = self.load_excel(file_path, sheet_name=sheet_name, **load_options)
        
        # Remove metadata columns from validation
        actual_columns = [c for c in df.columns if not c.startswith('_')]
        
        result = {
            'valid': True,
            'sheet_name': sheet_name,
            'actual_columns': actual_columns,
            'row_count': df.count()
        }
        
        if expected_columns:
            missing_columns = set(expected_columns) - set(actual_columns)
            extra_columns = set(actual_columns) - set(expected_columns)
            
            result.update({
                'valid': len(missing_columns) == 0 and len(extra_columns) == 0,
                'expected_columns': expected_columns,
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns)
            })
        
        return result
    
    def clean_excel_data(
        self,
        df: DataFrame,
        remove_empty_rows: bool = True,
        remove_empty_cols: bool = True,
        trim_strings: bool = True
    ) -> DataFrame:
        """
        Clean Excel data (remove empty rows/columns, trim strings).
        
        Args:
            df: Spark DataFrame
            remove_empty_rows: Remove rows where all values are null
            remove_empty_cols: Remove columns where all values are null
            trim_strings: Trim whitespace from string columns
            
        Returns:
            Cleaned DataFrame
        """
        from pyspark.sql.functions import trim, col, when
        
        # Trim string columns
        if trim_strings:
            for column in df.columns:
                if dict(df.dtypes)[column] == 'string':
                    df = df.withColumn(column, trim(col(column)))
        
        # Remove rows where all non-metadata columns are null
        if remove_empty_rows:
            non_meta_cols = [c for c in df.columns if not c.startswith('_')]
            if non_meta_cols:
                # Create condition for at least one non-null value
                condition = None
                for column in non_meta_cols:
                    col_condition = col(column).isNotNull()
                    condition = col_condition if condition is None else (condition | col_condition)
                
                if condition is not None:
                    df = df.filter(condition)
        
        return df


def create_excel_loader(spark: Optional[SparkSession] = None) -> ExcelLoader:
    """
    Factory function to create Excel loader.
    
    Args:
        spark: SparkSession (creates new one if None)
        
    Returns:
        ExcelLoader instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Excel-Loader").getOrCreate()
    
    return ExcelLoader(spark)
