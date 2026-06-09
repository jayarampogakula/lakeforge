"""
Snowflake Data Warehouse Loader for LakeForge
Load data from Snowflake using Spark connector
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class SnowflakeLoader:
    """Load data from Snowflake"""
    
    def __init__(
        self,
        spark: SparkSession,
        account: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        warehouse: str,
        role: Optional[str] = None
    ):
        """
        Initialize Snowflake loader
        
        Args:
            spark: SparkSession instance
            account: Snowflake account identifier
            user: Username
            password: Password
            database: Database name
            schema: Schema name
            warehouse: Virtual warehouse name
            role: Role name (optional)
        """
        self.spark = spark
        self.sfOptions = {
            "sfURL": f"{account}.snowflakecomputing.com",
            "sfUser": user,
            "sfPassword": password,
            "sfDatabase": database,
            "sfSchema": schema,
            "sfWarehouse": warehouse
        }
        
        if role:
            self.sfOptions["sfRole"] = role
        
        self.logger = logging.getLogger(__name__)
    
    def load_table(
        self,
        table_name: str,
        use_pushdown: bool = True
    ) -> DataFrame:
        """
        Load table from Snowflake
        
        Args:
            table_name: Table name (schema.table or just table)
            use_pushdown: Enable query pushdown for better performance
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading Snowflake table: {table_name}")
        
        options = self.sfOptions.copy()
        options["dbtable"] = table_name
        
        if use_pushdown:
            options["autopushdown"] = "on"
        
        reader = self.spark.read.format("snowflake")
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load()
    
    def load_query(self, query: str) -> DataFrame:
        """
        Load data using SQL query
        
        Args:
            query: Snowflake SQL query
        
        Returns:
            DataFrame
        """
        self.logger.info("Executing Snowflake query")
        
        options = self.sfOptions.copy()
        options["query"] = query
        
        reader = self.spark.read.format("snowflake")
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load()
    
    def load_incremental(
        self,
        table_name: str,
        timestamp_column: str,
        last_timestamp: str
    ) -> DataFrame:
        """
        Load incremental data
        
        Args:
            table_name: Table name
            timestamp_column: Timestamp column
            last_timestamp: Last loaded timestamp
        
        Returns:
            DataFrame with new records
        """
        query = f"""
            SELECT * FROM {table_name}
            WHERE {timestamp_column} > '{last_timestamp}'
        """
        return self.load_query(query)
