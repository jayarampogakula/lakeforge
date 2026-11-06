"""
Unit tests for LakeForge multi-stage deployment (dev, test, staging, prod).
"""
import pytest
from pathlib import Path
from lakeforge.metadata.config_parser import ConfigParser
from lakeforge.governance import GovernanceEngine
from lakeforge.cli.deploy import deploy_stage, SUPPORTED_STAGES


def test_env_settings_all_stages():
    """Verify that all standard enterprise stages are defined in env_settings.json."""
    config_dir = "configs"
    for stage in SUPPORTED_STAGES:
        env_cfg = ConfigParser.get_environment_config(config_dir, stage)
        assert env_cfg is not None
        assert "catalog" in env_cfg
        assert "bronze_schema" in env_cfg
        assert "silver_schema" in env_cfg
        assert "gold_schema" in env_cfg


def test_governance_stage_catalog_resolution():
    """Verify that governance grants dynamically adapt to the target deployment stage."""
    engine = GovernanceEngine()

    # Staging
    res_staging = engine.reconcile_permissions("configs", environment="staging", dry_run=True)
    stmts_staging = "\n".join(res_staging["statements"])
    assert "lakehouse_staging" in stmts_staging

    # Production
    res_prod = engine.reconcile_permissions("configs", environment="prod", dry_run=True)
    stmts_prod = "\n".join(res_prod["statements"])
    assert "lakehouse_prod" in stmts_prod


def test_deploy_stage_dry_run_dev():
    """Verify LakeForge CLI deploy command in dry-run mode for dev stage."""
    result = deploy_stage("dev", config_dir="configs", dry_run=True)
    assert result["status"] == "SUCCESS"
    assert result["validation_passed"] is True
    assert result["governance_grants"] > 0
    assert result["views_processed"] > 0
    assert result["pipelines_synced"] > 0
    assert result["workspace_path"] == "/Workspace/Shared/lakeforge/dev"


def test_deploy_stage_dry_run_prod():
    """Verify LakeForge CLI deploy command in dry-run mode for prod stage."""
    result = deploy_stage("prod", config_dir="configs", dry_run=True)
    assert result["status"] == "SUCCESS"
    assert result["validation_passed"] is True
    assert result["governance_grants"] > 0
    assert result["workspace_path"] == "/Workspace/Shared/lakeforge/prod"


def test_deploy_invalid_stage():
    """Verify that deploying with an unsupported stage raises ValueError."""
    with pytest.raises(ValueError, match="Invalid stage"):
        deploy_stage("invalid_env")
