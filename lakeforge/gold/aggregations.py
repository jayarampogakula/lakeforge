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
            
        # 1. Custom Python Aggregation Function support
        custom_mod = config.get("custom_module") or config.get("module")
        custom_fn = config.get("custom_function") or config.get("function")
        if custom_mod and custom_fn:
            from lakeforge.utilities.dynamic_runner import DynamicRunner
            params = config.get("params", {})
            try:
                return DynamicRunner.load_and_execute(custom_mod, custom_fn, source_dfs=source_dfs, spark=spark, **params)
            except TypeError:
                try:
                    return DynamicRunner.load_and_execute(custom_mod, custom_fn, source_dfs, **params)
                except TypeError:
                    primary_df = list(source_dfs.values())[0] if source_dfs else None
                    return DynamicRunner.load_and_execute(custom_mod, custom_fn, primary_df, **params)

        # 2. Direct SQL Query / sql_file support
        if "query" in config and config["query"]:
            return spark.sql(config["query"])

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
