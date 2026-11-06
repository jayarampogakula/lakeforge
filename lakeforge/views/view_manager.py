"""
LakeForge View Manager Module
Handles creating temporary and persistent Spark SQL views based on tables or custom queries.
Supports both Databricks Unity Catalog (3-part namespaces) and standard PySpark (1/2-part namespaces).
"""
import logging
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession
from lakeforge.metadata.config_parser import ViewConfig

class ViewManager:
    """Manages creation of Spark SQL views."""
    
    def __init__(self, spark: SparkSession, environment_config: Optional[Dict[str, Any]] = None):
        """
        Initialize ViewManager.
        
        Args:
            spark: SparkSession instance
            environment_config: Environment properties dict (e.g. catalog, schemas) for query interpolation
        """
        self.spark = spark
        self.env_config = environment_config or {}
        self.logger = logging.getLogger(__name__)

    def create_view(self, config: ViewConfig) -> None:
        """
        Create a single view (persistent or temporary) in Spark.
        
        Args:
            config: ViewConfig dataclass instance
        """
        self.logger.info(f"Creating view {config.view_name} (type: {config.view_type})")
        is_uc = False
        
        # 1. Determine DDL header
        if config.view_type == "temp":
            ddl_prefix = f"CREATE OR REPLACE TEMPORARY VIEW {config.view_name}"
        else:
            # Ensure catalog and schema exist if Unity Catalog multi-part namespace is supported
            try:
                self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema}")
                ddl_prefix = f"CREATE OR REPLACE VIEW {config.catalog}.{config.schema}.{config.view_name}"
                is_uc = True
            except Exception as e:
                self.logger.warning(f"Could not use multi-part catalog {config.catalog} (expected in local/OSS Spark): {str(e)}")
                # Fallback to local Spark catalog schema (database) creation
                try:
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema}")
                    ddl_prefix = f"CREATE OR REPLACE VIEW {config.schema}.{config.view_name}"
                except Exception as e2:
                    self.logger.warning(f"Could not create schema {config.schema}: {str(e2)}")
                    ddl_prefix = f"CREATE OR REPLACE VIEW {config.view_name}"
            
        # 2. Determine SELECT definition query
        if config.definition_type == "table":
            source = config.source_table
            if "." not in source:
                if config.view_type == "temp":
                    # For temp views, source table resolves directly in the current session
                    source = config.source_table
                elif is_uc:
                    source = f"{config.catalog}.{config.schema}.{source}"
                elif config.schema:
                    source = f"{config.schema}.{source}"
                
            cols = "*"
            if config.select_columns:
                cols = ", ".join(config.select_columns)
                
            sql_def = f"SELECT {cols} FROM {source}"
            if config.filter_condition:
                sql_def += f" WHERE {config.filter_condition}"
        elif config.definition_type == "query":
            # Interpolate custom query with environment configurations
            sql_def = config.query
            if self.env_config:
                try:
                    sql_def = sql_def.format(**self.env_config)
                except KeyError as ke:
                    self.logger.warning(f"Key {ke} not found in environment config for view query interpolation")
            
            # If running in local/OSS Spark (non-UC), strip multi-part catalog references if present
            if not is_uc and config.catalog:
                cat_prefix = f"{config.catalog}."
                if cat_prefix in sql_def:
                    sql_def = sql_def.replace(cat_prefix, "")
        else:
            raise ValueError(f"Invalid view definition_type: {config.definition_type}")
            
        # 3. Assemble and run DDL query
        full_query = f"{ddl_prefix} AS {sql_def}"
        self.logger.debug(f"Running view DDL query: {full_query}")
        self.spark.sql(full_query)
        self.logger.info(f"Successfully materialized view: {config.view_name}")

    def create_views(self, configs: List[ViewConfig]) -> None:
        """
        Create multiple views from a list of ViewConfigs.
        
        Args:
            configs: List of ViewConfig objects
        """
        for config in configs:
            self.create_view(config)
            
            
def create_view_manager(spark: SparkSession, environment_config: Optional[Dict[str, Any]] = None) -> ViewManager:
    """
    Factory function to create a ViewManager instance.
    """
    return ViewManager(spark, environment_config)
