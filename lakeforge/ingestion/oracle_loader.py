"""
Oracle Database Loader for LakeForge
Supports both on-premise and cloud Oracle databases
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, List
import logging

class OracleLoader:
    """Load data from Oracle Database using JDBC"""
    
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        service_name: str,
        user: str,
        password: str,
        jdbc_driver: str = "oracle.jdbc.OracleDriver"
    ):
        """
        Initialize Oracle loader
        
        Args:
            spark: SparkSession instance
            host: Oracle host address
            port: Oracle port (default 1521)
            service_name: Oracle service name or SID
            user: Database username
            password: Database password
            jdbc_driver: JDBC driver class
        """
        self.spark = spark
        self.jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
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
        Load entire table from Oracle
        
        Args:
            table_name: Fully qualified table name (schema.table)
            num_partitions: Number of parallel partitions for reading
            partition_column: Column for partitioning the read
            lower_bound: Lower bound for partition column
            upper_bound: Upper bound for partition column
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading Oracle table: {table_name}")
        
        reader = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table_name,
            properties=self.connection_properties
        )
        
        # Enable partitioned read for better performance
        if partition_column and lower_bound is not None and upper_bound is not None:
            reader = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=self.connection_properties
            )
        
        return reader
    
    def load_query(self, query: str) -> DataFrame:
        """
        Load data using custom SQL query
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Executing Oracle query")
        
        # Wrap query in subquery
        subquery = f"({query}) tmp"
        
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=subquery,
            properties=self.connection_properties
        )
    
    def load_incremental(
        self,
        table_name: str,
        timestamp_column: str,
        last_timestamp: str
    ) -> DataFrame:
        """
        Load incremental data based on timestamp
        
        Args:
            table_name: Table name
            timestamp_column: Timestamp column for filtering
            last_timestamp: Last loaded timestamp
        
        Returns:
            DataFrame with new/updated records
        """
        query = f"""
            SELECT * FROM {table_name}
            WHERE {timestamp_column} > TIMESTAMP '{last_timestamp}'
        """
        return self.load_query(query)
