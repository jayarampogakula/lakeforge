"""
Kafka Streaming Data Loader for LakeForge
Supports Kafka topics with various serialization formats
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
import logging

class KafkaLoader:
    """Load streaming data from Apache Kafka topics"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def load_stream(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        kafka_options: Optional[Dict] = None,
        value_format: str = "json",
        starting_offsets: str = "earliest"
    ) -> DataFrame:
        """
        Load streaming data from Kafka topic
        
        Args:
            kafka_bootstrap_servers: Comma-separated Kafka broker addresses
            topic: Kafka topic name
            kafka_options: Additional Kafka consumer options
            value_format: Format of message value (json, avro, string)
            starting_offsets: Starting offset (earliest, latest, or JSON string)
        
        Returns:
            Streaming DataFrame
        """
        self.logger.info(f"Loading Kafka stream from topic: {topic}")
        
        # Base Kafka options
        base_options = {
            "kafka.bootstrap.servers": kafka_bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "failOnDataLoss": "false"
        }
        
        # Merge with custom options
        if kafka_options:
            base_options.update(kafka_options)
        
        # Read from Kafka
        df = self.spark.readStream.format("kafka").options(**base_options).load()
        
        # Parse value based on format
        if value_format == "json":
            from pyspark.sql.functions import col, from_json
            # User needs to provide schema separately
            self.logger.info("Kafka value format: JSON (schema inference required)")
            df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", 
                              "topic", "partition", "offset", "timestamp")
        elif value_format == "avro":
            self.logger.info("Kafka value format: Avro")
            # Avro deserialization logic here
            df = df.selectExpr("CAST(key AS STRING)", "value", 
                              "topic", "partition", "offset", "timestamp")
        else:
            # String format
            df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", 
                              "topic", "partition", "offset", "timestamp")
        
        return df
    
    def load_batch(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        kafka_options: Optional[Dict] = None,
        starting_offsets: str = "earliest",
        ending_offsets: str = "latest"
    ) -> DataFrame:
        """
        Load batch data from Kafka topic (bounded read)
        
        Args:
            kafka_bootstrap_servers: Comma-separated Kafka broker addresses
            topic: Kafka topic name
            kafka_options: Additional Kafka consumer options
            starting_offsets: Starting offset
            ending_offsets: Ending offset
        
        Returns:
            Batch DataFrame
        """
        self.logger.info(f"Loading Kafka batch from topic: {topic}")
        
        base_options = {
            "kafka.bootstrap.servers": kafka_bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "endingOffsets": ending_offsets
        }
        
        if kafka_options:
            base_options.update(kafka_options)
        
        df = self.spark.read.format("kafka").options(**base_options).load()
        
        return df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", 
                            "topic", "partition", "offset", "timestamp")
