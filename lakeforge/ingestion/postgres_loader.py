"""
PostgreSQL Database Loader for LakeForge
Load data from PostgreSQL databases
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class PostgresLoader:
    """Load data from PostgreSQL"""
    
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        jdbc_driver: str = "org.postgresql.Driver"
    ):
        """
        Initialize PostgreSQL loader
        
        Args:
            spark: SparkSession instance
            host: PostgreSQL host
            port: PostgreSQL port (default 5432)
            database: Database name
            user: Username
            password: Password
            jdbc_driver: JDBC driver class
        """
        self.spark = spark
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": jdbc_driver
        }
        self.logger = logging.getLogger(__name__)
    
    def load_table(
        self,
        table_name: str,
        num_partitions: int = 4,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None
    ) -> DataFrame:
        """
        Load table from PostgreSQL
        
        Args:
            table_name: Fully qualified table name (schema.table)
            num_partitions: Number of parallel partitions
            partition_column: Column for partitioning
            lower_bound: Lower bound for partition column
            upper_bound: Upper bound for partition column
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading PostgreSQL table: {table_name}")
        
        if partition_column and lower_bound is not None and upper_bound is not None:
            return self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=self.connection_properties
            )
        else:
            return self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.connection_properties
            )
    
    def load_query(self, query: str) -> DataFrame:
        """
        Load data using SQL query
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame
        """
        self.logger.info("Executing PostgreSQL query")
        
        subquery = f"({query}) tmp"
        
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=subquery,
            properties=self.connection_properties
        )
    
    def load_cdc(
        self,
        table_name: str,
        timestamp_column: str,
        last_timestamp: str,
        deleted_flag_column: Optional[str] = None
    ) -> DataFrame:
        """
        Load CDC data from PostgreSQL
        
        Args:
            table_name: Table name
            timestamp_column: Modification timestamp column
            last_timestamp: Last loaded timestamp
            deleted_flag_column: Soft delete flag column (optional)
        
        Returns:
            DataFrame with changes
        """
        query = f"""
            SELECT * FROM {table_name}
            WHERE {timestamp_column} > '{last_timestamp}'
        """
        
        if deleted_flag_column:
            query += f" OR {deleted_flag_column} = true"
        
        return self.load_query(query)
