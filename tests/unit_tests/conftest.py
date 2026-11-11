"""
Pytest configuration and shared fixtures for LakeForge test suite.
"""
import sys
import pytest

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped PySpark fixture with Delta Lake support.
    Shared across all unit tests to avoid multiple SparkSession restarts.
    """
    has_real_pyspark = getattr(sys, "has_real_pyspark", True)
    if not has_real_pyspark:
        pytest.skip("PySpark not available in this environment")

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark is not installed")

    builder = SparkSession.builder \
        .master("local[1]") \
        .appName("LakeForge-TestSuite") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "target/spark-warehouse")

    try:
        from delta import configure_spark_with_delta_pip
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        session = builder.getOrCreate()

    yield session
    try:
        session.stop()
    except Exception:
        pass
