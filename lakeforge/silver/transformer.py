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
    def enforce_schema(df: DataFrame, schema_def: Any) -> DataFrame:
        """
        Enforce an explicit schema and datatypes on the DataFrame.

        Supports:
          1. DDL schema string: "id INT, name STRING, created_at TIMESTAMP"
          2. Dict mapping: {"id": "int", "name": "string", "created_at": "timestamp"}
        """
        if not schema_def:
            return df

        if isinstance(schema_def, dict):
            select_exprs = [col(c).cast(t).alias(c) for c, t in schema_def.items() if c in df.columns]
            if select_exprs:
                return df.select(*select_exprs)
            return df

        elif isinstance(schema_def, str):
            # Parse Spark SQL DDL string via active SparkSession or StructType
            try:
                spark_sess = getattr(df, "sparkSession", None)
                if spark_sess:
                    parsed = spark_sess.createDataFrame([], schema=schema_def).schema
                    select_exprs = [col(f.name).cast(f.dataType).alias(f.name) for f in parsed.fields if f.name in df.columns]
                    if select_exprs:
                        return df.select(*select_exprs)
            except Exception:
                pass

            try:
                from pyspark.sql.types import _parse_datatype_string, StructType
                parsed = _parse_datatype_string(schema_def)
                if isinstance(parsed, StructType):
                    select_exprs = [col(f.name).cast(f.dataType).alias(f.name) for f in parsed.fields if f.name in df.columns]
                    if select_exprs:
                        return df.select(*select_exprs)
            except Exception:
                pass

            # Fallback for simple comma-separated "name type" pairs
            select_exprs = []
            for field_str in schema_def.split(","):
                parts = field_str.strip().split()
                if len(parts) >= 2:
                    c, t = parts[0], parts[1]
                    if c in df.columns:
                        select_exprs.append(col(c).cast(t).alias(c))
            if select_exprs:
                return df.select(*select_exprs)

        return df

    @staticmethod
    def transform(df: DataFrame, configs: Any, schema: Any = None) -> DataFrame:
        """
        Apply configuration-driven transformations to DataFrame.
        
        Args:
            df: Input DataFrame
            configs: List of transformation config dictionaries OR a single transformation config dict
            schema: Optional schema definition to enforce
            
        Returns:
            Transformed DataFrame
        """
        transformed_df = df
        
        # Support full transformation config dict directly (with custom_module, columns, dedup, query)
        if isinstance(configs, dict):
            # 1. Custom DataFrame function at config level
            custom_mod = configs.get("custom_module") or configs.get("module")
            custom_fn = configs.get("custom_function") or configs.get("function")
            if custom_mod and custom_fn:
                params = configs.get("params", {})
                transformed_df = DynamicRunner.load_and_execute(custom_mod, custom_fn, transformed_df, **params)
                if schema:
                    transformed_df = SilverTransformer.enforce_schema(transformed_df, schema)
                return transformed_df

            if configs.get("query"):
                spark_session = getattr(transformed_df, "sparkSession", None)
                if spark_session:
                    source_tbl = configs.get("source_table", "source_table")
                    transformed_df.createOrReplaceTempView(source_tbl)
                    transformed_df.createOrReplaceTempView("source_df")
                    transformed_df = spark_session.sql(configs["query"])
                    if schema:
                        transformed_df = SilverTransformer.enforce_schema(transformed_df, schema)
                    return transformed_df

            # 2. Custom column functions list if present
            for c_spec in configs.get("custom_columns", []):
                col_name = c_spec.get("column")
                target_col = c_spec.get("target_column", col_name)
                c_mod = c_spec.get("module")
                c_fn = c_spec.get("function")
                c_params = c_spec.get("params", {})
                if c_mod and c_fn and col_name in transformed_df.columns:
                    col_res = DynamicRunner.load_and_execute(c_mod, c_fn, col(col_name), **c_params)
                    transformed_df = transformed_df.withColumn(target_col, col_res)

            # 3. Apply column expressions if present in dict
            for col_name, expression in (configs.get("columns") or {}).items():
                transformed_df = transformed_df.withColumn(col_name, expr(expression))

            # 4. Apply deduplication if present in dict
            dedup = configs.get("deduplication")
            if dedup and dedup.get("key_columns"):
                transformed_df = Deduplicator.deduplicate(
                    df=transformed_df,
                    keys=dedup.get("key_columns", []),
                    strategy=dedup.get("strategy", "keep_latest"),
                    order_by=dedup.get("order_column", "_ingestion_timestamp")
                )

            configs = configs.get("transformations", [])

        if not isinstance(configs, list):
            configs = []
        
        for config in configs:
            trans_type = config.get("type")
            
            if trans_type in ["custom_dataframe", "custom_function", "custom"]:
                module = config.get("module")
                func = config.get("function")
                params = config.get("params", {})
                transformed_df = DynamicRunner.load_and_execute(module, func, transformed_df, **params)

            elif trans_type == "custom_column":
                col_name = config.get("column")
                target_col = config.get("target_column", col_name)
                module = config.get("module")
                func = config.get("function")
                params = config.get("params", {})
                if module and func and col_name in transformed_df.columns:
                    col_res = DynamicRunner.load_and_execute(module, func, col(col_name), **params)
                    transformed_df = transformed_df.withColumn(target_col, col_res)

            elif trans_type in ["custom_sql", "sql", "query"]:
                spark_session = getattr(transformed_df, "sparkSession", None)
                sql_expr = config.get("query") or config.get("expression")
                if spark_session and sql_expr:
                    transformed_df.createOrReplaceTempView("source_df")
                    transformed_df = spark_session.sql(sql_expr)

            elif trans_type in ["drop_duplicates", "deduplicate"]:
                transformed_df = Deduplicator.deduplicate(
                    df=transformed_df,
                    keys=config.get("columns", config.get("keys", [])),
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
                        
            elif trans_type in ["add_derived_column", "derived_column"]:
                col_name = config.get("name")
                expression = config.get("expression")
                transformed_df = transformed_df.withColumn(col_name, expr(expression))
                
            elif trans_type in ["filter_rows", "filter"]:
                condition = config.get("condition")
                transformed_df = transformed_df.filter(expr(condition))
                
            elif trans_type in ["cast_columns", "cast"]:
                if "columns" in config and isinstance(config["columns"], dict):
                    for c_name, t_type in config["columns"].items():
                        if c_name in transformed_df.columns:
                            transformed_df = transformed_df.withColumn(c_name, col(c_name).cast(t_type))
                elif "column" in config and "target_type" in config:
                    col_name = config["column"]
                    if col_name in transformed_df.columns:
                        transformed_df = transformed_df.withColumn(col_name, col(col_name).cast(config["target_type"]))
                    
            elif trans_type in ["drop_columns", "drop"]:
                cols_to_drop = config.get("columns", [config.get("column")] if config.get("column") else [])
                transformed_df = transformed_df.drop(*cols_to_drop)
                
            elif trans_type in ["rename_columns", "rename"]:
                if "mapping" in config:
                    for old_name, new_name in config["mapping"].items():
                        transformed_df = transformed_df.withColumnRenamed(old_name, new_name)
                elif "column" in config and "target_name" in config:
                    transformed_df = transformed_df.withColumnRenamed(config["column"], config["target_name"])
                    
            elif trans_type in ["select_columns", "select"]:
                cols_to_select = config.get("columns", [])
                if cols_to_select:
                    transformed_df = transformed_df.select(*cols_to_select)
                    
            elif trans_type == "select_and_cast" or trans_type == "project":
                col_mappings = config.get("columns", {})
                if col_mappings:
                    select_exprs = [col(c).cast(t).alias(c) for c, t in col_mappings.items()]
                    transformed_df = transformed_df.select(*select_exprs)
                    
            elif trans_type in ["join_tables", "join"]:
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

            elif trans_type in ["enforce_schema", "schema"]:
                schema_def = config.get("schema") or config.get("columns")
                transformed_df = SilverTransformer.enforce_schema(transformed_df, schema_def)

        if schema:
            transformed_df = SilverTransformer.enforce_schema(transformed_df, schema)

        return transformed_df
