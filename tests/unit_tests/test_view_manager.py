"""
Unit tests for LakeForge ViewManager
"""
import sys
import pytest
has_real_pyspark = getattr(sys, "has_real_pyspark", True)
pytestmark = pytest.mark.skipif(not has_real_pyspark, reason="Requires real PySpark")
from pyspark.sql import SparkSession, Row
from lakeforge.views.view_manager import ViewManager, create_view_manager
from lakeforge.metadata.config_parser import ViewConfig

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession fixture for testing."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("LakeForge-ViewManager-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield session
    session.stop()

def test_create_temp_view_table(spark):
    view_manager = create_view_manager(spark)
    
    # 1. Create a mock table
    data = [Row(id=1, name="Alice", status="Active"), Row(id=2, name="Bob", status="Inactive")]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("my_customers")
    
    # 2. Configure a temp table-based view
    config = ViewConfig(
        view_name="v_active_custs",
        catalog="local",
        schema="default",
        definition_type="table",
        view_type="temp",
        source_table="my_customers",
        select_columns=["id", "name"],
        filter_condition="status = 'Active'"
    )
    
    # 3. Create view
    view_manager.create_view(config)
    
    # 4. Verify results
    v_df = spark.table("v_active_custs")
    rows = v_df.collect()
    
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"
    assert "status" not in v_df.columns

def test_create_persistent_view_query(spark):
    # Setup env config for formatting
    env_config = {"catalog": "local_cat", "gold_schema": "test_gold"}
    view_manager = create_view_manager(spark, env_config)
    
    # Create mock table inside default schema
    data = [Row(id=1, total_spent=150.00), Row(id=2, total_spent=50.00)]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("customer_summary")
    
    # Configure a persistent view using query type
    config = ViewConfig(
        view_name="v_vip_customers",
        catalog="local_cat",
        schema="test_gold",
        definition_type="query",
        view_type="persistent",
        # Custom query with environment placeholder (falls back to local schema default inside spark catalog view creation)
        query="SELECT id FROM customer_summary WHERE total_spent > 100.00"
    )
    
    # Create view (will fall back to creating in test_gold schema locally)
    view_manager.create_view(config)
    
    # Verify results
    v_df = spark.table("test_gold.v_vip_customers")
    rows = v_df.collect()
    
    assert len(rows) == 1
    assert rows[0]["id"] == 1
