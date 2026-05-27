"""
LakeForge SCD Type 2 Module
Implements Slowly Changing Dimension Type 2 logic for historical tracking.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_date, md5, concat_ws, 
    max as _max, row_number, coalesce
)
from pyspark.sql.window import Window
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from delta.tables import DeltaTable


class SCDType2Handler:
    """
    Handler for SCD Type 2 operations on Delta tables.
    Maintains historical records with effective dates and current flags.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        effective_date_col: str = "effective_start_date",
        end_date_col: str = "effective_end_date",
        current_flag_col: str = "is_current",
        hash_col: str = "record_hash"
    ):
        """
        Initialize SCD Type 2 handler.
        
        Args:
            spark: Active SparkSession
            effective_date_col: Name of effective start date column
            end_date_col: Name of effective end date column
            current_flag_col: Name of current flag column
            hash_col: Name of hash column for change detection
        """
        self.spark = spark
        self.effective_date_col = effective_date_col
        self.end_date_col = end_date_col
        self.current_flag_col = current_flag_col
        self.hash_col = hash_col
        self.high_date = date(9999, 12, 31)
    
    def add_scd_columns(
        self,
        df: DataFrame,
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        effective_date: Optional[date] = None
    ) -> DataFrame:
        """
        Add SCD Type 2 metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            key_columns: Business key columns
            tracked_columns: Columns to track for changes (all if None)
            effective_date: Effective date for the records (today if None)
            
        Returns:
            DataFrame with SCD columns
        """
        # Determine tracked columns
        if tracked_columns is None:
            tracked_columns = [c for c in df.columns if c not in key_columns]
        
        # Set effective date
        if effective_date is None:
            effective_date = date.today()
        
        # Add hash column for change detection
        hash_columns = key_columns + tracked_columns
        df = df.withColumn(
            self.hash_col,
            md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in hash_columns]))
        )
        
        # Add SCD metadata columns
        df = df.withColumn(self.effective_date_col, lit(effective_date))
        df = df.withColumn(self.end_date_col, lit(self.high_date))
        df = df.withColumn(self.current_flag_col, lit(True))
        
        return df
    
    def merge_scd_type2(
        self,
        source_df: DataFrame,
        target_table: str,
        catalog: str,
        schema: str,
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        effective_date: Optional[date] = None,
        create_if_not_exists: bool = True
    ) -> Dict[str, Any]:
        """
        Merge source data into target table using SCD Type 2 logic.
        
        Args:
            source_df: Source DataFrame with new/updated records
            target_table: Target table name
            catalog: Catalog name
            schema: Schema name
            key_columns: Business key columns for matching
            tracked_columns: Columns to track for changes
            effective_date: Effective date for new records
            create_if_not_exists: Create table if it doesn't exist
            
        Returns:
            Merge statistics dictionary
        """
        full_table_name = f"{catalog}.{schema}.{target_table}"
        
        # Set effective date
        if effective_date is None:
            effective_date = date.today()
        
        # Add SCD columns to source
        source_df = self.add_scd_columns(
            source_df,
            key_columns,
            tracked_columns,
            effective_date
        )
        
        # Check if target table exists
        table_exists = self.spark.catalog.tableExists(full_table_name)
        
        if not table_exists:
            if create_if_not_exists:
                # Create initial table
                source_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
                
                return {
                    "operation": "initial_load",
                    "table": full_table_name,
                    "records_inserted": source_df.count(),
                    "records_updated": 0,
                    "records_unchanged": 0
                }
            else:
                raise ValueError(f"Target table {full_table_name} does not exist")
        
        # Load target table
        target_delta = DeltaTable.forName(self.spark, full_table_name)
        target_df = target_delta.toDF()
        
        # Get current records only
        current_target_df = target_df.filter(col(self.current_flag_col) == True)
        
        # Perform SCD Type 2 merge
        metrics = self._perform_scd_merge(
            source_df,
            target_delta,
            current_target_df,
            key_columns,
            effective_date,
            full_table_name
        )
        
        return metrics
    
    def _perform_scd_merge(
        self,
        source_df: DataFrame,
        target_delta: DeltaTable,
        current_target_df: DataFrame,
        key_columns: List[str],
        effective_date: date,
        full_table_name: str
    ) -> Dict[str, Any]:
        """
        Internal method to perform the actual SCD Type 2 merge.
        
        Args:
            source_df: Source DataFrame with SCD columns
            target_delta: Target Delta table
            current_target_df: Current records from target
            key_columns: Business key columns
            effective_date: Effective date for new records
            full_table_name: Full table name for logging
            
        Returns:
            Merge statistics
        """
        # Join source with current target to identify changes
        join_condition = " AND ".join([f"source.{k} = target.{k}" for k in key_columns])
        
        matched_df = source_df.alias("source").join(
            current_target_df.alias("target"),
            [col(f"source.{k}") == col(f"target.{k}") for k in key_columns],
            "left"
        )
        
        # Identify record types
        # New records: target hash is null
        # Changed records: hashes differ
        # Unchanged records: hashes match
        
        new_records = matched_df.filter(col(f"target.{self.hash_col}").isNull())
        changed_records = matched_df.filter(
            (col(f"target.{self.hash_col}").isNotNull()) &
            (col(f"source.{self.hash_col}") != col(f"target.{self.hash_col}"))
        )
        unchanged_records = matched_df.filter(
            col(f"source.{self.hash_col}") == col(f"target.{self.hash_col}")
        )
        
        # Count metrics
        new_count = new_records.count()
        changed_count = changed_records.count()
        unchanged_count = unchanged_records.count()
        
        # Step 1: Expire changed records (set end date and current flag)
        if changed_count > 0:
            # Build update condition for changed records
            update_keys = [col(f"target.{k}") == col(f"updates.{k}") for k in key_columns]
            update_condition = update_keys[0]
            for condition in update_keys[1:]:
                update_condition = update_condition & condition
            
            # Create DataFrame with keys of changed records
            changed_keys = changed_records.select([col(f"source.{k}").alias(k) for k in key_columns])
            
            # Update target: expire current records
            target_delta.alias("target").merge(
                changed_keys.alias("updates"),
                update_condition
            ).whenMatchedUpdate(
                condition=col(f"target.{self.current_flag_col}") == True,
                set={
                    self.end_date_col: lit(effective_date),
                    self.current_flag_col: lit(False)
                }
            ).execute()
        
        # Step 2: Insert new versions of changed records and new records
        records_to_insert = new_records.select("source.*").union(
            changed_records.select("source.*")
        ) if changed_count > 0 else new_records.select("source.*")
        
        if records_to_insert.count() > 0:
            # Append new records
            records_to_insert.write.format("delta").mode("append").saveAsTable(full_table_name)
        
        return {
            "operation": "scd_type2_merge",
            "table": full_table_name,
            "effective_date": effective_date.isoformat(),
            "records_inserted": new_count + changed_count,
            "records_updated": changed_count,
            "records_unchanged": unchanged_count,
            "timestamp": datetime.now().isoformat()
        }
    
    def get_current_records(
        self,
        catalog: str,
        schema: str,
        table_name: str
    ) -> DataFrame:
        """
        Get current (active) records from SCD Type 2 table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            
        Returns:
            DataFrame with current records only
        """
        full_table_name = f"{catalog}.{schema}.{table_name}"
        df = self.spark.table(full_table_name)
        
        return df.filter(col(self.current_flag_col) == True)
    
    def get_record_history(
        self,
        catalog: str,
        schema: str,
        table_name: str,
        key_values: Dict[str, Any]
    ) -> DataFrame:
        """
        Get historical records for a specific business key.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            key_values: Dictionary of key_column -> value
            
        Returns:
            DataFrame with all versions of the record
        """
        full_table_name = f"{catalog}.{schema}.{table_name}"
        df = self.spark.table(full_table_name)
        
        # Build filter condition
        filter_condition = None
        for key_col, key_val in key_values.items():
            condition = col(key_col) == lit(key_val)
            filter_condition = condition if filter_condition is None else (filter_condition & condition)
        
        if filter_condition is not None:
            df = df.filter(filter_condition)
        
        return df.orderBy(col(self.effective_date_col))
    
    def get_records_as_of_date(
        self,
        catalog: str,
        schema: str,
        table_name: str,
        as_of_date: date
    ) -> DataFrame:
        """
        Get records as they existed on a specific date (time travel).
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            as_of_date: Date to retrieve records for
            
        Returns:
            DataFrame with records as of the specified date
        """
        full_table_name = f"{catalog}.{schema}.{table_name}"
        df = self.spark.table(full_table_name)
        
        # Filter for records effective on the specified date
        return df.filter(
            (col(self.effective_date_col) <= lit(as_of_date)) &
            (col(self.end_date_col) > lit(as_of_date))
        )


def create_scd_type2_handler(
    spark: Optional[SparkSession] = None,
    **kwargs
) -> SCDType2Handler:
    """
    Factory function to create SCD Type 2 handler.
    
    Args:
        spark: SparkSession (creates new one if None)
        **kwargs: Additional arguments for SCDType2Handler
        
    Returns:
        SCDType2Handler instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-SCD-Type2").getOrCreate()
    
    return SCDType2Handler(spark, **kwargs)
