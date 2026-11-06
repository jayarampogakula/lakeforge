"""
Unit tests for LakeForge CLI commands and validators
"""
import sys
import pytest
from pathlib import Path

from lakeforge.cli.validate_config import validate_all_configs
from lakeforge.cli.init_project import init_project


def test_cli_validate_configs():
    """Verify that the repository configs pass validation."""
    config_dir = Path(__file__).resolve().parents[2] / "configs"
    is_valid, summary = validate_all_configs(str(config_dir), environment="dev")
    assert is_valid is True
    assert summary["error_count"] == 0
    assert summary["valid_count"] > 0
    assert "environments" in summary["categories"]
    assert "ingestion" in summary["categories"]
    assert "dq" in summary["categories"]


def test_cli_init_project(tmp_path):
    """Verify that project scaffolding generates valid directory and starter configs."""
    init_project(str(tmp_path))
    assert (tmp_path / "configs" / "environments" / "env_settings.json").exists()
    assert (tmp_path / "configs" / "ingestion" / "sample_sales.json").exists()
    assert (tmp_path / "configs" / "dq" / "sample_sales.json").exists()
    assert (tmp_path / "configs" / "transformations" / "sales_clean.json").exists()
    assert (tmp_path / "configs" / "aggregations" / "sales_summary.json").exists()
    assert (tmp_path / "configs" / "views" / "reporting_views.json").exists()

    # Validate the generated project structure
    is_valid, summary = validate_all_configs(str(tmp_path / "configs"), environment="dev")
    assert is_valid is True
    assert summary["error_count"] == 0
