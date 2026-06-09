"""
Unit tests for LakeForge DQEngine validations
"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from lakeforge.dq.dq_engine import DQEngine, create_dq_engine

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession fixture for testing."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("LakeForge-DQ-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .getOrCreate()
    yield session
    session.stop()

def test_null_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(id=1, name="Alice"), Row(id=2, name=None)]
    df = spark.createDataFrame(data)
    
    # Check 1: Should fail for 'name' because there is a null and threshold is 0.0
    res_fail = dq.run_null_check(df, "name", threshold=0.0)
    assert res_fail["passed"] is False
    assert res_fail["null_count"] == 1
    
    # Check 2: Should pass with threshold = 0.5 (50% nulls allowed)
    res_pass = dq.run_null_check(df, "name", threshold=0.5)
    assert res_pass["passed"] is True

def test_duplicate_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(id=1, val="A"), Row(id=1, val="B"), Row(id=2, val="C")]
    df = spark.createDataFrame(data)
    
    # Duplicates allowed = False: should fail
    res_fail = dq.run_duplicate_check(df, ["id"], allow_duplicates=False)
    assert res_fail["passed"] is False
    assert res_fail["duplicate_count"] == 1
    
    # Duplicates on both id and val: should pass (all rows unique)
    res_pass = dq.run_duplicate_check(df, ["id", "val"], allow_duplicates=False)
    assert res_pass["passed"] is True

def test_regex_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(email="test@example.com"), Row(email="invalid_email")]
    df = spark.createDataFrame(data)
    
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    
    # 100% threshold: should fail
    res_fail = dq.run_regex_check(df, "email", pattern, threshold=1.0)
    assert res_fail["passed"] is False
    
    # 50% threshold: should pass
    res_pass = dq.run_regex_check(df, "email", pattern, threshold=0.5)
    assert res_pass["passed"] is True

def test_range_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(val=10), Row(val=20), Row(val=30)]
    df = spark.createDataFrame(data)
    
    # Min=10, Max=25: Should fail (30 is out of bounds)
    res_fail = dq.run_range_check(df, "val", min_value=10, max_value=25, threshold=1.0)
    assert res_fail["passed"] is False
    
    # Min=10, Max=35: Should pass
    res_pass = dq.run_range_check(df, "val", min_value=10, max_value=35, threshold=1.0)
    assert res_pass["passed"] is True

def test_datatype_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(id=1, name="Alice")]
    df = spark.createDataFrame(data)
    
    res_pass = dq.run_datatype_check(df, "id", "int")
    assert res_pass["passed"] is True
    
    res_fail = dq.run_datatype_check(df, "name", "int")
    assert res_fail["passed"] is False

def test_allowed_values_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(status="Active"), Row(status="Inactive"), Row(status="Pending")]
    df = spark.createDataFrame(data)
    
    allowed = ["Active", "Inactive"]
    res_fail = dq.run_allowed_values_check(df, "status", allowed)
    assert res_fail["passed"] is False
    
    res_pass = dq.run_allowed_values_check(df, "status", allowed + ["Pending"])
    assert res_pass["passed"] is True

def test_null_rate_threshold_check(spark):
    dq = create_dq_engine(spark)
    data = [Row(val=None), Row(val=None), Row(val=10)]
    df = spark.createDataFrame(data)
    
    # Null rate is 2/3 = 66%. Limit 10%: fail
    res_fail = dq.run_null_rate_threshold_check(df, "val", threshold=0.10)
    assert res_fail["passed"] is False
    
    # Limit 70%: pass
    res_pass = dq.run_null_rate_threshold_check(df, "val", threshold=0.70)
    assert res_pass["passed"] is True
