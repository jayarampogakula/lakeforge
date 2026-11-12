import sys
from unittest.mock import MagicMock

# Try importing real pyspark before setting up mocks
try:
    import pyspark
    # Ensure it's not a mock
    if hasattr(pyspark, "__file__") and not isinstance(pyspark, MagicMock):
        sys.has_real_pyspark = True
    else:
        sys.has_real_pyspark = False
except ImportError:
    sys.has_real_pyspark = False

if not sys.has_real_pyspark:
    # Define placeholder classes for type checking / imports
    class MockDataFrame:
        pass

    class MockRow:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def __getitem__(self, item):
            return getattr(self, item)

    pyspark_mock = MagicMock()
    pyspark_sql_mock = MagicMock()
    pyspark_sql_mock.DataFrame = MockDataFrame
    pyspark_sql_mock.Row = MockRow
    
    # Inject mocks into sys.modules
    sys.modules['pyspark'] = pyspark_mock
    sys.modules['pyspark.sql'] = pyspark_sql_mock
    sys.modules['pyspark.sql.functions'] = MagicMock()
    sys.modules['pyspark.sql.types'] = MagicMock()
    sys.modules['pyspark.sql.window'] = MagicMock()
    sys.modules['delta'] = MagicMock()
    sys.modules['delta.tables'] = MagicMock()
    sys.modules['pandas'] = MagicMock()


import pytest

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped PySpark fixture with Delta Lake support.
    Shared across all unit and integration tests to avoid multiple SparkSession restarts.
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
