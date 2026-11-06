"""
Unit tests for multi-tenant and client-scoped configuration resolution.
"""
import pytest
from lakeforge.metadata.config_parser import ConfigParser, load_view_configs


def test_client_scoped_ingestion_resolution():
    """Verify loading client-specific ingestion config with inheritance."""
    cfg = ConfigParser.get_ingestion_config("configs", "sales", "dev", client_id="client_alpha")
    assert cfg.source_name == "sales"
    assert cfg.target_catalog == "lakeforge_client_alpha_dev"
    assert cfg.client_id == "client_alpha"
    assert cfg.file_format == "csv"


def test_client_scoped_dq_resolution():
    """Verify loading client-specific DQ config."""
    dq_cfg = ConfigParser.get_dq_config("configs", "sales", "dev", client_id="client_alpha")
    assert dq_cfg.table_name == "sales"
    assert len(dq_cfg.rules) == 2
    rule_types = [r.rule_type for r in dq_cfg.rules]
    assert "null_check" in rule_types
    assert "range_check" in rule_types


def test_client_scoped_silver_transformation():
    """Verify loading client-specific silver transformation config."""
    trans_cfg = ConfigParser.get_silver_transformation_config("configs", "sales_clean", "dev", client_id="client_alpha")
    assert trans_cfg["source_table"] == "sales"
    assert trans_cfg["target_table"] == "sales_clean"
    assert "columns" in trans_cfg or "transformations" in trans_cfg


def test_client_scoped_views_resolution():
    """Verify loading client-specific views."""
    views = ConfigParser.get_view_configs("configs", "dev", client_id="client_alpha")
    view_names = [v.view_name for v in views]
    assert "v_client_alpha_sales_summary" in view_names
    assert "v_client_alpha_recent_sales" in view_names

    # Check custom query view
    summary_view = next(v for v in views if v.view_name == "v_client_alpha_sales_summary")
    assert summary_view.definition_type == "query"
    assert "GROUP BY region" in summary_view.query


def test_global_fallback_when_no_client():
    """Verify standard global configs are returned when client_id is None."""
    global_views = ConfigParser.get_view_configs("configs", "dev", client_id=None)
    view_names = [v.view_name for v in global_views]
    assert "v_active_customers" in view_names
    assert "v_high_value_transactions" in view_names
