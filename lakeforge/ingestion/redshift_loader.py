"""
AWS Redshift Loader for LakeForge
Optimized loading from Amazon Redshift using UNLOAD to S3
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class RedshiftLoader:
    """Load data from AWS Redshift"""
    
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        temp_s3_bucket: str,
        iam_role: Optional[str] = None
    ):
        """
        Initialize Redshift loader
        
        Args:
            spark: SparkSession instance
            host: Redshift cluster endpoint
            port: Redshift port (default 5439)
            database: Database name
            user: Database username
            password: Database password
            temp_s3_bucket: S3 bucket for temporary data (s3://bucket/path/)
            iam_role: IAM role ARN for Redshift UNLOAD (optional)
        """
        self.spark = spark
        self.jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"
        self.temp_s3_bucket = temp_s3_bucket
        self.iam_role = iam_role
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "com.amazon.redshift.jdbc.Driver"
        }
        self.logger = logging.getLogger(__name__)
    
    def load_table(
        self,
        table_name: str,
        use_unload: bool = True
    ) -> DataFrame:
        """
        Load table from Redshift
        
        Args:
            table_name: Fully qualified table name (schema.table)
            use_unload: Use Redshift UNLOAD for better performance
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading Redshift table: {table_name}")
        
        reader = self.spark.read.format("io.github.spark_redshift_community.spark.redshift")
        
        options = {
            "url": self.jdbc_url,
            "dbtable": table_name,
            "tempdir": self.temp_s3_bucket,
            "user": self.connection_properties["user"],
            "password": self.connection_properties["password"]
        }
        
        if self.iam_role:
            options["aws_iam_role"] = self.iam_role
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load()
    
    def load_query(self, query: str) -> DataFrame:
        """
        Load data using custom SQL query
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame
        """
        self.logger.info("Executing Redshift query")
        
        reader = self.spark.read.format("io.github.spark_redshift_community.spark.redshift")
        
        options = {
            "url": self.jdbc_url,
            "query": query,
            "tempdir": self.temp_s3_bucket,
            "user": self.connection_properties["user"],
            "password": self.connection_properties["password"]
        }
        
        if self.iam_role:
            options["aws_iam_role"] = self.iam_role
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load()
