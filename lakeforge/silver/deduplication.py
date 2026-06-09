"""
LakeForge Deduplication Module
Provides strategies to deduplicate Spark DataFrames.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from typing import List

class Deduplicator:
    """
    Deduplicator utility to drop duplicate records using strategies like keeping the latest record.
    """
    
    @staticmethod
    def deduplicate(
        df: DataFrame,
        keys: List[str],
        strategy: str = "keep_latest",
        order_by: str = "_ingestion_timestamp"
    ) -> DataFrame:
        """
        Deduplicate DataFrame based on business keys and a strategy.
        
        Args:
            df: Input DataFrame
            keys: List of columns defining the deduplication key
            strategy: Strategy to use ('keep_latest', 'keep_first')
            order_by: Column to order by for picking the record to keep
            
        Returns:
            Deduplicated DataFrame
        """
        if not keys:
            return df
            
        if strategy in ["keep_latest", "keep_first"]:
            # Window partition by key and order by order_by
            order_col = col(order_by).desc() if strategy == "keep_latest" else col(order_by).asc()
            window_spec = Window.partitionBy(*keys).orderBy(order_col)
            
            # Apply window and filter
            deduped_df = df.withColumn("_row_num", row_number().over(window_spec)) \
                           .filter(col("_row_num") == 1) \
                           .drop("_row_num")
            return deduped_df
            
        # Fallback to simple dropDuplicates if no ordering or unrecognized strategy
        return df.dropDuplicates(keys)
