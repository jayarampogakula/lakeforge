"""
Unit tests for LakeForge Governance Engine and Unity Catalog access control reconciliation.
"""
import pytest
from pathlib import Path
from lakeforge.governance import (
    GovernanceEngine,
    create_governance_engine,
    AccessControlConfig,
    PrincipalPrivilege,
    MaskingFunction,
    ColumnMaskAssignment,
    RowFilter
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


def test_pii_masking_and_row_filters():
    """Verify Unity Catalog Column Masking and Row Filter DDL generation."""
    mask_fn = MaskingFunction(
        function_name="mask_ssn",
        catalog="lakeforge_dev",
        schema="silver",
        input_column="ssn",
        input_type="STRING",
        return_type="STRING",
        expression="CASE WHEN is_account_group_member('pii_admins') THEN ssn ELSE '***-**-****' END"
    )
    mask_sql = mask_fn.to_create_function_sql()
    assert mask_sql == "CREATE OR REPLACE FUNCTION lakeforge_dev.silver.mask_ssn(ssn STRING) RETURNS STRING RETURN CASE WHEN is_account_group_member('pii_admins') THEN ssn ELSE '***-**-****' END"

    col_mask = ColumnMaskAssignment(
        table_name="lakeforge_dev.silver.customers_clean",
        column_name="ssn",
        function_name="lakeforge_dev.silver.mask_ssn"
    )
    col_mask_sql = col_mask.to_alter_column_mask_sql()
    assert col_mask_sql == "ALTER TABLE lakeforge_dev.silver.customers_clean ALTER COLUMN ssn SET MASK lakeforge_dev.silver.mask_ssn"

    row_filter = RowFilter(
        filter_name="filter_region",
        catalog="lakeforge_dev",
        schema="silver",
        table_name="lakeforge_dev.silver.customers_clean",
        filter_columns=["region"],
        expression="is_account_group_member('global_data') OR region = current_user()"
    )
    rf_fn_sql = row_filter.to_create_filter_function_sql()
    rf_alter_sql = row_filter.to_alter_row_filter_sql()
    assert "CREATE OR REPLACE FUNCTION lakeforge_dev.silver.filter_region(region STRING) RETURNS BOOLEAN RETURN" in rf_fn_sql
    assert rf_alter_sql == "ALTER TABLE lakeforge_dev.silver.customers_clean SET ROW FILTER lakeforge_dev.silver.filter_region ON (region)"

    # Verify reconciliation from configs
    engine = GovernanceEngine()
    res = engine.reconcile_permissions(config_path_or_dir="configs", environment="dev", dry_run=True)
    stmts_text = "\n".join(res["statements"])
    assert "silver.mask_email" in stmts_text
    assert "ALTER COLUMN email SET MASK" in stmts_text
    assert "SET ROW FILTER" in stmts_text
    assert "filter_customers_by_region ON (region)" in stmts_text
