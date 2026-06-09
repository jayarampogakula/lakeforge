"""
Azure SQL Database Loader for LakeForge
Load data from Azure SQL Database
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

class AzureSQLLoader:
    """Load data from Azure SQL Database"""
    
    def __init__(
        self,
        spark: SparkSession,
        server: str,
        database: str,
        user: str,
        password: str,
        port: int = 1433
    ):
        """
        Initialize Azure SQL loader
        
        Args:
            spark: SparkSession instance
            server: Azure SQL server (servername.database.windows.net)
            database: Database name
            user: Username
            password: Password
            port: Port (default 1433)
        """
        self.spark = spark
        self.jdbc_url = f"jdbc:sqlserver://{server}:{port};database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        self.logger = logging.getLogger(__name__)
    
    def load_table(self, table_name: str) -> DataFrame:
        """Load table from Azure SQL"""
        self.logger.info(f"Loading Azure SQL table: {table_name}")
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table_name,
            properties=self.connection_properties
        )
    
    def load_query(self, query: str) -> DataFrame:
        """Load data using SQL query"""
        self.logger.info("Executing Azure SQL query")
        subquery = f"({query}) tmp"
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=subquery,
            properties=self.connection_properties
        )
