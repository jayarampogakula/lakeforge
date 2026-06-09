"""
MongoDB Loader for LakeForge
Load data from MongoDB collections
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
import logging

class MongoDBLoader:
    """Load data from MongoDB"""
    
    def __init__(
        self,
        spark: SparkSession,
        connection_string: str,
        database: str
    ):
        """
        Initialize MongoDB loader
        
        Args:
            spark: SparkSession instance
            connection_string: MongoDB connection string
            database: Database name
        """
        self.spark = spark
        self.connection_string = connection_string
        self.database = database
        self.logger = logging.getLogger(__name__)
    
    def load_collection(
        self,
        collection: str,
        pipeline: Optional[list] = None,
        **options
    ) -> DataFrame:
        """
        Load collection from MongoDB
        
        Args:
            collection: Collection name
            pipeline: Aggregation pipeline (list of stages)
            **options: Additional MongoDB connector options
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading MongoDB collection: {collection}")
        
        reader = (self.spark.read
                  .format("mongodb")
                  .option("connection.uri", self.connection_string)
                  .option("database", self.database)
                  .option("collection", collection))
        
        if pipeline:
            import json
            reader = reader.option("aggregation.pipeline", json.dumps(pipeline))
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load()
    
    def load_with_filter(
        self,
        collection: str,
        filter_query: Dict
    ) -> DataFrame:
        """
        Load collection with filter
        
        Args:
            collection: Collection name
            filter_query: MongoDB filter query dict
        
        Returns:
            Filtered DataFrame
        """
        pipeline = [{"$match": filter_query}]
        return self.load_collection(collection, pipeline=pipeline)
