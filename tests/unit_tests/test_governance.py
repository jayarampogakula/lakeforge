"""
Unit tests for LakeForge Governance Engine and Unity Catalog access control reconciliation.
"""
import pytest
from pathlib import Path
from lakeforge.governance import (
    GovernanceEngine,
    create_governance_engine,
    AccessControlConfig,
    PrincipalPrivilege
)


def test_principal_privilege_ddl_generation():
    """Verify Unity Catalog GRANT DDL syntax generation."""
    priv = PrincipalPrivilege(
        principal="analysts_group",
        securable_type="TABLE",
        securable_name="lakeforge_dev.silver.customers_clean",
        privileges=["SELECT", "MODIFY"]
    )
    sql = priv.to_grant_sql()
    assert sql == "GRANT SELECT, MODIFY ON TABLE lakeforge_dev.silver.customers_clean TO `analysts_group`"


def test_governance_engine_load_and_dry_run():
    """Verify loading centralized access control config and executing dry run."""
    engine = create_governance_engine(spark=None)
    result = engine.reconcile_permissions(
        config_path_or_dir="configs",
        environment="dev",
        dry_run=True
    )
    assert result["dry_run"] is True
    assert result["total_statements"] > 0
    assert result["executed"] == result["total_statements"]
    assert result["failed"] == 0

    # Ensure catalog, schema, table, and view grants exist
    stmts_text = "\n".join(result["statements"])
    assert "ON CATALOG lakeforge_dev" in stmts_text
    assert "ON SCHEMA lakeforge_dev.gold" in stmts_text
    assert "ON TABLE lakeforge_dev.silver.customers_clean" in stmts_text
    assert "ON VIEW lakeforge_dev.gold.v_high_value_transactions" in stmts_text


def test_client_scoped_governance():
    """Verify multi-tenant client role inheritance and overrides."""
    engine = GovernanceEngine()
    result = engine.reconcile_permissions(
        config_path_or_dir="configs",
        environment="dev",
        client_id="client_alpha",
        dry_run=True
    )
    stmts_text = "\n".join(result["statements"])
    # Should include base grants plus client_alpha specific grants
    assert "lakeforge_client_alpha_dev" in stmts_text
    assert "`client_alpha_bi_group`" in stmts_text
