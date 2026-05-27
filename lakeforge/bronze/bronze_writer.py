"""
LakeForge Bronze Writer Module
Writes data to Bronze layer Delta tables with audit columns and file tracking.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws, col, input_file_name
from typing import Optional, List, Dict, Any
from datetime import datetime
from delta.tables import DeltaTable


class BronzeWriter:
    """
    Writer for Bronze layer Delta tables with audit capabilities.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Bronze Writer.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def add_audit_columns(
        self,
        df: DataFrame,
        add_hash_key: bool = True,
        hash_columns: Optional[List[str]] = None,
        add_ingestion_timestamp: bool = True,
        add_ingestion_date: bool = True,
        add_source_system: bool = True,
        source_system: str = "unknown"
    ) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            add_hash_key: Add MD5 hash key for deduplication
            hash_columns: Columns to include in hash (all if None)
            add_ingestion_timestamp: Add ingestion timestamp
            add_ingestion_date: Add ingestion date
            add_source_system: Add source system identifier
            source_system: Name of source system
            
        Returns:
            DataFrame with audit columns
        """
        from pyspark.sql.functions import to_date
        
        # Add ingestion timestamp
        if add_ingestion_timestamp:
            if "_ingestion_timestamp" not in df.columns:
                df = df.withColumn("_ingestion_timestamp", current_timestamp())
        
        # Add ingestion date
        if add_ingestion_date:
            if "_ingestion_date" not in df.columns:
                df = df.withColumn("_ingestion_date", to_date(current_timestamp()))
        
        # Add source system
        if add_source_system:
            if "_source_system" not in df.columns:
                df = df.withColumn("_source_system", lit(source_system))
        
        # Add hash key for deduplication
        if add_hash_key:
            if hash_columns is None:
                # Use all non-audit columns
                hash_columns = [c for c in df.columns if not c.startswith('_')]
            
            if hash_columns:
                # Create hash from specified columns
                df = df.withColumn(
                    "_record_hash",
                    md5(concat_ws("||", *[col(c).cast("string") for c in hash_columns]))
                )
        
        return df
    
    def write_to_bronze(
        self,
        df: DataFrame,
        target_table: str,
        catalog: str,
        schema: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        add_audit_columns: bool = True,
        source_system: str = "unknown",
        merge_keys: Optional[List[str]] = None,
        optimize_after_write: bool = False,
        z_order_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Write DataFrame to Bronze layer Delta table.
        
        Args:
            df: DataFrame to write
            target_table: Table name
            catalog: Catalog name
            schema: Schema name
            mode: Write mode ('append', 'overwrite', 'merge')
            partition_by: Columns to partition by
            add_audit_columns: Whether to add audit columns
            source_system: Source system identifier
            merge_keys: Keys for merge operation (required if mode='merge')
            optimize_after_write: Run OPTIMIZE after write
            z_order_by: Columns to Z-order by during optimization
            
        Returns:
            Write statistics dictionary
        """
        from delta.tables import DeltaTable
        
        # Add audit columns if requested
        if add_audit_columns:
            df = self.add_audit_columns(df, source_system=source_system)
        
        # Full table name
        full_table_name = f"{catalog}.{schema}.{target_table}"
        
        # Track metrics
        metrics = {
            "table": full_table_name,
            "mode": mode,
            "start_time": datetime.now().isoformat(),
            "rows_written": df.count()
        }
        
        try:
            if mode == "merge" and merge_keys:
                # Merge operation
                metrics.update(self._merge_to_table(
                    df, full_table_name, merge_keys, partition_by
                ))
            else:
                # Standard write operation
                writer = df.write.format("delta").mode(mode)
                
                if partition_by:
                    writer = writer.partitionBy(partition_by)
                
                writer.saveAsTable(full_table_name)
            
            # Optimize if requested
            if optimize_after_write:
                self._optimize_table(full_table_name, z_order_by)
                metrics["optimized"] = True
            
            metrics["status"] = "success"
            metrics["end_time"] = datetime.now().isoformat()
            
        except Exception as e:
            metrics["status"] = "failed"
            metrics["error"] = str(e)
            metrics["end_time"] = datetime.now().isoformat()
            raise
        
        return metrics
    
    def _merge_to_table(
        self,
        df: DataFrame,
        full_table_name: str,
        merge_keys: List[str],
        partition_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Perform merge (upsert) operation on Delta table.
        
        Args:
            df: Source DataFrame
            full_table_name: Target table name (catalog.schema.table)
            merge_keys: Columns to match on for merge
            partition_by: Partition columns
            
        Returns:
            Merge metrics
        """
        # Check if table exists
        if not self.spark.catalog.tableExists(full_table_name):
            # Table doesn't exist, create it
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.saveAsTable(full_table_name)
            
            return {
                "merge_type": "initial_load",
                "rows_inserted": df.count()
            }
        
        # Table exists, perform merge
        target_table = DeltaTable.forName(self.spark, full_table_name)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Perform merge
        merge_builder = (
            target_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )
        
        merge_result = merge_builder.execute()
        
        # Get merge metrics (if available)
        return {
            "merge_type": "upsert",
            "merge_keys": merge_keys
        }
    
    def _optimize_table(
        self,
        full_table_name: str,
        z_order_by: Optional[List[str]] = None
    ):
        """
        Optimize Delta table.
        
        Args:
            full_table_name: Table name (catalog.schema.table)
            z_order_by: Columns to Z-order by
        """
        optimize_sql = f"OPTIMIZE {full_table_name}"
        
        if z_order_by:
            z_order_cols = ", ".join(z_order_by)
            optimize_sql += f" ZORDER BY ({z_order_cols})"
        
        self.spark.sql(optimize_sql)
    
    def create_file_tracker_table(
        self,
        catalog: str,
        schema: str,
        table_name: str = "file_tracker"
    ):
        """
        Create a file tracker table to track ingested files.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Tracker table name
        """
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            file_path STRING,
            file_name STRING,
            file_size_bytes BIGINT,
            file_modification_time TIMESTAMP,
            ingestion_timestamp TIMESTAMP,
            ingestion_date DATE,
            target_table STRING,
            rows_ingested BIGINT,
            status STRING,
            error_message STRING
        )
        USING DELTA
        """
        
        self.spark.sql(create_sql)
    
    def track_file_ingestion(
        self,
        catalog: str,
        schema: str,
        file_path: str,
        target_table: str,
        rows_ingested: int,
        status: str = "success",
        error_message: Optional[str] = None,
        tracker_table: str = "file_tracker"
    ):
        """
        Log file ingestion to tracker table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            file_path: Path to ingested file
            target_table: Target table name
            rows_ingested: Number of rows ingested
            status: Ingestion status
            error_message: Error message if failed
            tracker_table: Tracker table name
        """
        from pathlib import Path
        import os
        
        # Get file metadata
        file_name = Path(file_path).name
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)) if os.path.exists(file_path) else None
        
        # Create tracking record
        tracking_data = [{
            "file_path": file_path,
            "file_name": file_name,
            "file_size_bytes": file_size,
            "file_modification_time": file_mod_time,
            "ingestion_timestamp": datetime.now(),
            "ingestion_date": datetime.now().date(),
            "target_table": target_table,
            "rows_ingested": rows_ingested,
            "status": status,
            "error_message": error_message
        }]
        
        tracking_df = self.spark.createDataFrame(tracking_data)
        
        # Append to tracker table
        full_tracker_name = f"{catalog}.{schema}.{tracker_table}"
        tracking_df.write.format("delta").mode("append").saveAsTable(full_tracker_name)


def create_bronze_writer(spark: Optional[SparkSession] = None) -> BronzeWriter:
    """
    Factory function to create Bronze writer.
    
    Args:
        spark: SparkSession (creates new one if None)
        
    Returns:
        BronzeWriter instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Bronze-Writer").getOrCreate()
    
    return BronzeWriter(spark)
