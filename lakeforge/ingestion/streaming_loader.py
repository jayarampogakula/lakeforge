"""LakeForge Streaming Module"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

class StreamingLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def load_kafka_stream(self, kafka_servers: str, topics: list) -> DataFrame:
        return self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", ",".join(topics)).load() \
            .withColumn("_ingestion_timestamp", current_timestamp())

def create_streaming_loader(spark):
    return StreamingLoader(spark)
