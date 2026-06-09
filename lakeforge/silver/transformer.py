"""
LakeForge Silver Transformer Module
Executes configuration-driven transformations on Spark DataFrames.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, regexp_replace, lower, upper
from typing import List, Dict, Any
from lakeforge.silver.deduplication import Deduplicator
from lakeforge.utilities.dynamic_runner import DynamicRunner

class SilverTransformer:
    """
    Applies a chain of transformations defined in configuration to a DataFrame.
    """
    
    @staticmethod
    def transform(df: DataFrame, configs: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply configuration-driven transformations to DataFrame.
        
        Args:
            df: Input DataFrame
            configs: List of transformation config dictionaries
            
        Returns:
            Transformed DataFrame
        """
        transformed_df = df
        
        for config in configs:
            trans_type = config.get("type")
            
            if trans_type == "deduplicate":
                transformed_df = Deduplicator.deduplicate(
                    df=transformed_df,
                    keys=config.get("columns", []),
                    strategy=config.get("strategy", "keep_latest"),
                    order_by=config.get("order_by", "_ingestion_timestamp")
                )
                
            elif trans_type == "standardize":
                col_name = config.get("column")
                op = config.get("operation")
                if col_name in transformed_df.columns:
                    if op == "lowercase":
                        transformed_df = transformed_df.withColumn(col_name, lower(col(col_name)))
                    elif op == "uppercase":
                        transformed_df = transformed_df.withColumn(col_name, upper(col(col_name)))
                    elif op == "remove_non_numeric":
                        transformed_df = transformed_df.withColumn(col_name, regexp_replace(col(col_name), r"[^0-9]", ""))
                        
            elif trans_type == "derived_column":
                col_name = config.get("name")
                expression = config.get("expression")
                transformed_df = transformed_df.withColumn(col_name, expr(expression))
                
            elif trans_type == "filter":
                condition = config.get("condition")
                transformed_df = transformed_df.filter(expr(condition))
                
            elif trans_type == "cast":
                col_name = config.get("column")
                target_type = config.get("target_type")
                if col_name in transformed_df.columns:
                    transformed_df = transformed_df.withColumn(col_name, col(col_name).cast(target_type))
                    
            elif trans_type == "drop":
                cols_to_drop = config.get("columns", [config.get("column")] if config.get("column") else [])
                transformed_df = transformed_df.drop(*cols_to_drop)
                
            elif trans_type == "rename":
                if "mapping" in config:
                    for old_name, new_name in config["mapping"].items():
                        transformed_df = transformed_df.withColumnRenamed(old_name, new_name)
                elif "column" in config and "target_name" in config:
                    transformed_df = transformed_df.withColumnRenamed(config["column"], config["target_name"])
                    
            elif trans_type == "join":
                right_table = config["right_table"]
                join_keys = config["join_keys"]
                join_type = config.get("join_type", "inner")
                
                # Fetch right DataFrame from Spark Session
                spark_session = transformed_df.sparkSession
                right_df = spark_session.table(right_table)
                
                transformed_df = transformed_df.join(right_df, on=join_keys, how=join_type)
                    
            elif trans_type == "custom_function":
                module = config.get("module")
                func = config.get("function")
                transformed_df = DynamicRunner.load_and_execute(module, func, transformed_df)
                
            elif trans_type == "custom":
                module = config.get("module")
                func = config.get("function")
                transformed_df = DynamicRunner.load_and_execute(module, func, transformed_df)
                
        return transformed_df
