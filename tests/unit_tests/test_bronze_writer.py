"""
Unit tests for LakeForge BronzeWriter auditing
"""
import sys
import pytest
has_real_pyspark = getattr(sys, "has_real_pyspark", True)
pytestmark = pytest.mark.skipif(not has_real_pyspark, reason="Requires real PySpark")
from pyspark.sql import SparkSession, Row
from lakeforge.bronze.bronze_writer import BronzeWriter, create_bronze_writer

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession fixture for testing."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("LakeForge-BronzeWriter-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield session
    session.stop()

def test_bronze_writer_add_audit_columns(spark):
    writer = create_bronze_writer(spark)
    data = [Row(id=1, name="Alice")]
    df = spark.createDataFrame(data)
    
    # Add audit columns with custom source system name
    df_audit = writer.add_audit_columns(
        df=df,
        add_hash_key=True,
        add_ingestion_timestamp=True,
        add_ingestion_date=True,
        add_source_system=True,
        source_system="crm_system"
    )
    
    cols = df_audit.columns
    assert "_ingestion_timestamp" in cols
    assert "_ingestion_date" in cols
    assert "_source_system" in cols
    assert "_record_hash" in cols
    
    row = df_audit.collect()[0]
    assert row["_source_system"] == "crm_system"
    assert len(row["_record_hash"]) == 32  # MD5 is 32 characters
