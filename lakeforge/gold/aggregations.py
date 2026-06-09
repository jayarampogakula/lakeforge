"""
LakeForge Gold Aggregations Module
Builds dynamic aggregations and KPI calculations for Gold layer tables.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, List

class GoldAggregator:
    """
    Constructs and executes dynamic group-by aggregations and joins.
    """
    
    @staticmethod
    def aggregate(
        spark: SparkSession,
        source_dfs: Dict[str, DataFrame],
        config: Dict[str, Any]
    ) -> DataFrame:
        """
        Execute dynamic SQL aggregations based on config.
        
        Args:
            spark: Active SparkSession
            source_dfs: Dict of alias -> DataFrame
            config: Gold aggregation configuration dict
            
        Returns:
            Aggregated DataFrame
        """
        # Register all source DataFrames as temp views using their aliases
        for alias, df in source_dfs.items():
            df.createOrReplaceTempView(alias)
            
        # Build SELECT clause
        select_parts = []
        group_cols = config.get("group_by", [])
        for col_name in group_cols:
            select_parts.append(col_name)
            
        for agg in config.get("aggregations", []):
            select_parts.append(f"{agg['expression']} AS {agg['name']}")
            
        select_clause = ", ".join(select_parts)
        
        # Build FROM / JOIN clause
        if "join_logic" in config:
            from_clause = config["join_logic"]
        else:
            # Single table
            from_clause = list(config["source_tables"].keys())[0]
            
        # Build GROUP BY clause
        group_by_clause = ", ".join(group_cols)
        
        # Assemble query
        query = f"SELECT {select_clause} FROM {from_clause}"
        if group_by_clause:
            query += f" GROUP BY {group_by_clause}"
            
        # Run query
        result_df = spark.sql(query)
        return result_df
