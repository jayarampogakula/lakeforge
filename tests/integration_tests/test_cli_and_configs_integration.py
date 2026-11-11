"""
End-to-End CLI and Multi-Stage Deployment Integration Tests for LakeForge.
Tests the deployment CLI across dev, test, staging, and prod stages,
as well as full client-scoping integration.
"""
import pytest
from pathlib import Path
from lakeforge.cli.deploy import deploy_stage, SUPPORTED_STAGES
from lakeforge.cli.validate_config import validate_all_configs
from lakeforge.metadata.config_parser import ConfigParser
from lakeforge.governance.governance_engine import GovernanceEngine


def test_multistage_cli_deployment_integration():
    """Verify LakeForge stage deployer across all supported stages in dry-run mode."""
    config_dir = "configs"
    for stage in SUPPORTED_STAGES:
        res = deploy_stage(stage=stage, config_dir=config_dir, dry_run=True)
        assert res["status"] == "SUCCESS", f"Deployment dry-run failed for stage {stage}"
        assert res["validation_passed"] is True
        assert res["governance_grants"] > 0
        assert res["views_processed"] > 0
        assert res["pipelines_synced"] > 0
        assert f"/Workspace/Shared/lakeforge/{stage}" in res["workspace_path"]


def test_client_alpha_full_lifecycle_config_integration():
    """Verify that client_alpha configs resolve properly across all layers."""
    config_dir = "configs"
    client_id = "client_alpha"

    # 1. Ingestion
    ingest_cfg = ConfigParser.get_ingestion_config(config_dir, "sales", "dev", client_id=client_id)
    assert ingest_cfg.source_name == "sales"
    assert ingest_cfg.client_id == client_id
    assert ingest_cfg.target_table == "sales"

    # 2. DQ
    dq_cfg = ConfigParser.get_dq_config(config_dir, "sales", "dev", client_id=client_id)
    assert dq_cfg.table_name == "sales"
    assert len(dq_cfg.rules) >= 2

    # 3. Silver Transformation
    trans_cfg = ConfigParser.get_silver_transformation_config(config_dir, "sales_clean", "dev", client_id=client_id)
    assert trans_cfg["source_table"] == "sales"
    assert trans_cfg["target_table"] == "sales_clean"

    # 4. Views
    views = ConfigParser.get_view_configs(config_dir, "dev", client_id=client_id)
    view_names = [v.view_name for v in views]
    assert "v_client_alpha_recent_sales" in view_names or "v_client_alpha_sales_summary" in view_names

    # 5. Governance
    gov_cfg = GovernanceEngine.load_governance_config(config_dir, client_id=client_id, environment="dev")
    assert gov_cfg is not None
    assert len(gov_cfg.grants) > 0


def test_cli_validate_all_stages_integration():
    """Verify that all configurations pass validation in every stage."""
    for stage in SUPPORTED_STAGES:
        is_valid, summary = validate_all_configs("configs", environment=stage)
        assert is_valid is True, f"Config validation failed for stage {stage}: {summary.get('error_count')} errors"
        assert summary["error_count"] == 0
        assert summary["valid_count"] > 0
