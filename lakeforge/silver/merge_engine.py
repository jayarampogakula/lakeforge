"""
LakeForge Silver Merge Engine Module
Handles upserts, merges, and overwrites of Silver layer tables.
"""
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import List, Optional, Dict, Any

class SilverMergeEngine:
    """
    Engine to write/merge clean datasets into Delta tables.
    """
    
    @staticmethod
    def merge(
        spark: SparkSession,
        df: DataFrame,
        target_table: str,
        merge_keys: List[str],
        mode: str = "merge",
        partition_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Write or merge DataFrame into target Delta table.
        
        Args:
            spark: Active SparkSession
            df: Input DataFrame
            target_table: Target table name (catalog.schema.table)
            merge_keys: Keys to join on for merge
            mode: Write mode ('merge', 'append', 'overwrite')
            partition_by: Partition columns
            
        Returns:
            Write stats
        """
        table_exists = spark.catalog.tableExists(target_table)
        
        if not table_exists:
            # Create new table
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.saveAsTable(target_table)
            return {
                "operation": "initial_load",
                "table": target_table,
                "records_inserted": df.count(),
                "records_updated": 0
            }
            
        if mode == "merge" and merge_keys:
            # Perform upsert
            delta_table = DeltaTable.forName(spark, target_table)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
            
            delta_table.alias("target") \
                .merge(df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
                
            return {
                "operation": "upsert_merge",
                "table": target_table,
                "records_inserted": df.count(),
                "merge_keys": merge_keys
            }
            
        else:
            # Overwrite or Append
            writer = df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.saveAsTable(target_table)
            return {
                "operation": mode,
                "table": target_table,
                "records_written": df.count()
            }
