"""
Unit tests for LakeForge SilverTransformer transformations
"""
import pytest
from pyspark.sql import SparkSession, Row
from lakeforge.silver.transformer import SilverTransformer

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession fixture for testing."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("LakeForge-Transformer-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield session
    session.stop()

def test_silver_transformer_cast(spark):
    data = [Row(id="1", val="100")]
    df = spark.createDataFrame(data)
    
    # Cast id and val to int
    configs = [
        {"type": "cast", "column": "id", "target_type": "int"},
        {"type": "cast", "column": "val", "target_type": "double"}
    ]
    
    transformed_df = SilverTransformer.transform(df, configs)
    schema_dict = {f.name: str(f.dataType) for f in transformed_df.schema.fields}
    
    assert "IntegerType" in schema_dict["id"]
    assert "DoubleType" in schema_dict["val"]

def test_silver_transformer_standardize(spark):
    data = [Row(email="Alice@EXAMPLE.com", phone="+1-555-0199")]
    df = spark.createDataFrame(data)
    
    configs = [
        {"type": "standardize", "column": "email", "operation": "lowercase"},
        {"type": "standardize", "column": "phone", "operation": "remove_non_numeric"}
    ]
    
    transformed_df = SilverTransformer.transform(df, configs)
    row = transformed_df.collect()[0]
    
    assert row["email"] == "alice@example.com"
    assert row["phone"] == "15550199"

def test_silver_transformer_derived_column_and_filter(spark):
    data = [
        Row(id=1, price=10.0, qty=2),
        Row(id=2, price=20.0, qty=1),
        Row(id=3, price=5.0, qty=0)
    ]
    df = spark.createDataFrame(data)
    
    configs = [
        {"type": "derived_column", "name": "total", "expression": "price * qty"},
        {"type": "filter", "condition": "total > 5.0"}
    ]
    
    transformed_df = SilverTransformer.transform(df, configs)
    rows = transformed_df.orderBy("id").collect()
    
    # Row 1: total = 20.0 (passed)
    # Row 2: total = 20.0 (passed)
    # Row 3: total = 0.0 (filtered out)
    assert len(rows) == 2
    assert rows[0]["total"] == 20.0
    assert rows[1]["total"] == 20.0

def test_silver_transformer_deduplicate(spark):
    # Multiple records for same id, order by seq
    data = [
        Row(id=1, seq=1, val="first"),
        Row(id=1, seq=2, val="latest"),
        Row(id=2, seq=1, val="only")
    ]
    df = spark.createDataFrame(data)
    
    configs = [
        {
            "type": "deduplicate",
            "columns": ["id"],
            "strategy": "keep_latest",
            "order_by": "seq"
        }
    ]
    
    transformed_df = SilverTransformer.transform(df, configs)
    rows = transformed_df.orderBy("id").collect()
    
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["val"] == "latest"
    assert rows[1]["id"] == 2
    assert rows[1]["val"] == "only"

def test_silver_transformer_drop_and_rename(spark):
    data = [Row(id=1, name="Alice", drop_me="trash")]
    df = spark.createDataFrame(data)
    
    configs = [
        {"type": "drop", "columns": ["drop_me"]},
        {"type": "rename", "column": "name", "target_name": "full_name"}
    ]
    
    transformed_df = SilverTransformer.transform(df, configs)
    cols = transformed_df.columns
    
    assert "drop_me" not in cols
    assert "full_name" in cols
    assert "name" not in cols

def test_silver_transformer_join(spark):
    # Setup right table in local Spark catalog
    data_left = [Row(id=1, val="A"), Row(id=2, val="B")]
    df_left = spark.createDataFrame(data_left)
    
    data_right = [Row(id=1, desc="Desc A"), Row(id=2, desc="Desc B")]
    df_right = spark.createDataFrame(data_right)
    df_right.createOrReplaceTempView("right_table_temp")
    
    # Register right table in catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
    spark.sql("USE DATABASE my_db")
    spark.sql("DROP TABLE IF EXISTS my_db.right_table")
    spark.sql("CREATE TABLE my_db.right_table USING DELTA AS SELECT * FROM right_table_temp")
    
    configs = [
        {
            "type": "join",
            "right_table": "my_db.right_table",
            "join_keys": ["id"],
            "join_type": "inner"
        }
    ]
    
    transformed_df = SilverTransformer.transform(df_left, configs)
    rows = transformed_df.orderBy("id").collect()
    
    assert len(rows) == 2
    assert rows[0]["desc"] == "Desc A"
    assert rows[1]["desc"] == "Desc B"
    
    # Cleanup catalog
    spark.sql("DROP TABLE IF EXISTS my_db.right_table")
