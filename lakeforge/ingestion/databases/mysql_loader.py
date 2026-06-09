"""
MySQL Database Loader for LakeForge
Load data from MySQL and MariaDB databases
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

class MySQLLoader:
    """Load data from MySQL/MariaDB"""
    
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        """
        Initialize MySQL loader
        
        Args:
            spark: SparkSession instance
            host: MySQL host
            port: MySQL port (default 3306)
            database: Database name
            user: Username
            password: Password
        """
        self.spark = spark
        self.jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
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
        """Load table from MySQL"""
        self.logger.info(f"Loading MySQL table: {table_name}")
        
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
        """Load data using SQL query"""
        self.logger.info("Executing MySQL query")
        subquery = f"({query}) tmp"
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=subquery,
            properties=self.connection_properties
        )
