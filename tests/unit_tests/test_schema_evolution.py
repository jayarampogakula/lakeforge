"""
Unit tests for Schema Drift & Evolution handling in LakeForge.
Tests strict, allow, and quarantine schema evolution policies.
"""
import pytest
from pyspark.sql import Row
from lakeforge.metadata.config_parser import ConfigParser, IngestionConfig
from lakeforge.bronze.bronze_writer import BronzeWriter, create_bronze_writer, SchemaDriftError


def test_schema_evolution_config_parsing():
    """Verify that IngestionConfig properly parses schema evolution policies."""
    cfg = IngestionConfig(
        source_name="customers_test",
        source_type="csv",
        source_path="data/test.csv",
        target_table="customers_test",
        target_catalog="lakeforge_dev",
        target_schema="bronze",
        schema_evolution="strict"
    )
    assert cfg.schema_evolution == "strict"

    # Default is "allow"
    cfg_default = ConfigParser.get_ingestion_config("configs", "sample_csv_ingestion", "dev")
    assert cfg_default.schema_evolution in ["allow", "strict", "quarantine"]


def test_schema_evolution_strict_mode(spark, tmp_path):
    """Verify that strict schema evolution raises SchemaDriftError when new columns appear."""
    table_name = "test_bronze_strict_drift"
    catalog = ""
    schema = "default"

    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{table_name}")
    except Exception:
        pass

    writer = create_bronze_writer(spark)

    # Initial write with 2 columns
    df1 = spark.createDataFrame([
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob")
    ])
    writer.write_to_bronze(
        df=df1,
        target_table=table_name,
        catalog=catalog,
        schema=schema,
        mode="overwrite",
        add_audit_columns=False,
        schema_evolution="strict"
    )

    # Second write with an unexpected new column 'age'
    df2 = spark.createDataFrame([
        Row(id=3, name="Charlie", age=30)
    ])

    with pytest.raises(SchemaDriftError) as exc_info:
        writer.write_to_bronze(
            df=df2,
            target_table=table_name,
            catalog=catalog,
            schema=schema,
            mode="append",
            add_audit_columns=False,
            schema_evolution="strict"
        )
    assert "Schema drift detected" in str(exc_info.value)
    assert "age" in str(exc_info.value)


def test_schema_evolution_allow_mode(spark, tmp_path):
    """Verify that 'allow' schema evolution seamlessly incorporates new columns."""
    table_name = "test_bronze_allow_drift"
    catalog = ""
    schema = "default"

    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{table_name}")
    except Exception:
        pass

    writer = create_bronze_writer(spark)

    # Initial write
    df1 = spark.createDataFrame([
        Row(id=1, city="New York")
    ])
    writer.write_to_bronze(
        df=df1,
        target_table=table_name,
        catalog=catalog,
        schema=schema,
        mode="overwrite",
        add_audit_columns=False,
        schema_evolution="allow"
    )

    # Second write with additional column 'country'
    df2 = spark.createDataFrame([
        Row(id=2, city="London", country="UK")
    ])
    res = writer.write_to_bronze(
        df=df2,
        target_table=table_name,
        catalog=catalog,
        schema=schema,
        mode="append",
        add_audit_columns=False,
        schema_evolution="allow"
    )
    assert res["status"] == "success"
    assert res.get("schema_evolved") is True
    assert "country" in res.get("evolved_columns", [])


def test_schema_evolution_quarantine_mode(spark, tmp_path):
    """Verify that 'quarantine' policy isolates drifted rows and protects target schema."""
    table_name = "test_bronze_quarantine_drift"
    catalog = ""
    schema = "default"

    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{table_name}")
        spark.sql(f"DROP TABLE IF EXISTS default.{table_name}_quarantine")
    except Exception:
        pass

    writer = create_bronze_writer(spark)

    # Initial write
    df1 = spark.createDataFrame([
        Row(id=1, status="ACTIVE")
    ])
    writer.write_to_bronze(
        df=df1,
        target_table=table_name,
        catalog=catalog,
        schema=schema,
        mode="overwrite",
        add_audit_columns=False,
        schema_evolution="quarantine"
    )

    # Second write with drifted column 'unknown_field'
    df2 = spark.createDataFrame([
        Row(id=2, status="PENDING", unknown_field="anomaly")
    ])
    res = writer.write_to_bronze(
        df=df2,
        target_table=table_name,
        catalog=catalog,
        schema=schema,
        mode="append",
        add_audit_columns=False,
        schema_evolution="quarantine"
    )
    assert res["status"] == "success"
    assert res.get("schema_drift_detected") is True
    assert "unknown_field" in res.get("drifted_columns", [])
