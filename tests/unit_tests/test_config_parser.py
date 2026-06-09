"""
Unit tests for modular ConfigParser
"""
import pytest
import json
from pathlib import Path
from lakeforge.metadata.config_parser import (
    ConfigParser,
    IngestionConfig,
    DQConfig,
    ViewConfig,
    load_ingestion_config,
    load_dq_config,
    load_view_configs
)

@pytest.fixture
def mock_config_dir(tmp_path):
    """Creates a temporary config directory layout."""
    # Create directories
    (tmp_path / "environments").mkdir()
    (tmp_path / "ingestion").mkdir()
    (tmp_path / "dq").mkdir()
    (tmp_path / "transformations").mkdir()
    (tmp_path / "aggregations").mkdir()
    (tmp_path / "views").mkdir()
    
    # env settings
    env_settings = {
        "dev": {
            "catalog": "lakehouse",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold"
        },
        "prod": {
            "catalog": "lakehouse_prod",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold"
        }
    }
    with open(tmp_path / "environments" / "env_settings.json", "w", encoding="utf-8") as f:
        json.dump(env_settings, f)
        
    # Ingestion customers
    cust_ingest = {
        "source_type": "csv",
        "file_path": "/path/to/customers.csv",
        "file_format": "csv",
        "options": {
            "header": "true",
            "inferSchema": "true"
        },
        "bronze_table": "customers",
        "silver_table": "customers_clean",
        "business_key": ["customer_id"],
        "merge_strategy": "upsert"
    }
    with open(tmp_path / "ingestion" / "customers.json", "w", encoding="utf-8") as f:
        json.dump(cust_ingest, f)
        
    # Ingestion transactions
    txn_ingest = {
        "source_type": "csv",
        "file_path": "/path/to/transactions.csv",
        "file_format": "csv",
        "bronze_table": "transactions",
        "silver_table": "transactions_clean",
        "business_key": ["transaction_id"],
        "merge_strategy": "append"
    }
    with open(tmp_path / "ingestion" / "transactions.json", "w", encoding="utf-8") as f:
        json.dump(txn_ingest, f)
        
    # DQ rules customers
    cust_dq = [
        {
            "rule_id": "c1",
            "rule_name": "id_not_null",
            "column": "customer_id",
            "validation_type": "not_null",
            "severity": "critical",
            "action": "quarantine"
        }
    ]
    with open(tmp_path / "dq" / "customers.json", "w", encoding="utf-8") as f:
        json.dump(cust_dq, f)
        
    # Silver transformations customers_clean
    cust_transform = {
        "source_table": "customers",
        "transformations": [
            {"type": "filter", "condition": "customer_id IS NOT NULL"}
        ]
    }
    with open(tmp_path / "transformations" / "customers_clean.json", "w", encoding="utf-8") as f:
        json.dump(cust_transform, f)
        
    # Gold aggregations customer_summary
    cust_agg = {
        "merge_strategy": "upsert",
        "business_key": ["customer_id"],
        "source_tables": {"customers": "silver.customers_clean"},
        "group_by": ["customer_id"],
        "aggregations": [{"name": "cnt", "expression": "count(*)"}]
    }
    with open(tmp_path / "aggregations" / "customer_summary.json", "w", encoding="utf-8") as f:
        json.dump(cust_agg, f)
        
    # View configuration reporting_views
    rep_views = {
        "v_active_customers": {
            "definition_type": "table",
            "schema_type": "silver",
            "source_table": "customers_clean",
            "select_columns": ["customer_id"]
        }
    }
    with open(tmp_path / "views" / "reporting_views.json", "w", encoding="utf-8") as f:
        json.dump(rep_views, f)
        
    return tmp_path

def test_load_merged_json(mock_config_dir):
    merged = ConfigParser.load_merged_json(str(mock_config_dir / "ingestion"))
    assert "customers" in merged
    assert "transactions" in merged
    assert merged["customers"]["source_type"] == "csv"
    assert merged["transactions"]["bronze_table"] == "transactions"

def test_get_environment_config(mock_config_dir):
    env_config = ConfigParser.get_environment_config(str(mock_config_dir), "dev")
    assert env_config["catalog"] == "lakehouse"
    assert env_config["bronze_schema"] == "bronze"

def test_get_ingestion_config(mock_config_dir):
    ing_config = ConfigParser.get_ingestion_config(str(mock_config_dir), "customers", "dev")
    assert isinstance(ing_config, IngestionConfig)
    assert ing_config.source_name == "customers"
    assert ing_config.target_table == "customers"
    assert ing_config.target_catalog == "lakehouse"
    assert ing_config.mode == "merge"
    assert ing_config.business_key == ["customer_id"]
    assert ing_config.merge_strategy == "upsert"

def test_get_dq_config(mock_config_dir):
    dq_config = ConfigParser.get_dq_config(str(mock_config_dir), "customers", "dev")
    assert isinstance(dq_config, DQConfig)
    assert dq_config.table_name == "customers"
    assert len(dq_config.rules) == 1
    assert dq_config.rules[0].rule_name == "id_not_null"

def test_get_silver_transformation_config(mock_config_dir):
    trans_config = ConfigParser.get_silver_transformation_config(str(mock_config_dir), "customers_clean", "dev")
    assert trans_config["source_table"] == "customers"
    assert len(trans_config["transformations"]) == 1

def test_get_gold_aggregation_config(mock_config_dir):
    agg_config = ConfigParser.get_gold_aggregation_config(str(mock_config_dir), "customer_summary", "dev")
    assert "customers" in agg_config["source_tables"]
    assert agg_config["group_by"] == ["customer_id"]
    assert agg_config["merge_strategy"] == "upsert"
    assert agg_config["business_key"] == ["customer_id"]

def test_get_view_configs(mock_config_dir):
    view_configs = ConfigParser.get_view_configs(str(mock_config_dir), "dev")
    assert len(view_configs) == 1
    assert isinstance(view_configs[0], ViewConfig)
    assert view_configs[0].view_name == "v_active_customers"
    assert view_configs[0].schema == "silver"
