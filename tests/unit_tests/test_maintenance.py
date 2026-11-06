"""
Unit tests for LakeForge Table Maintenance Manager.
Tests OPTIMIZE, Z-ORDER, VACUUM, ANALYZE, and layer-wide maintenance.
"""
import pytest
from lakeforge.maintenance.table_maintenance import (
    TableMaintenanceManager,
    create_maintenance_manager,
    MaintenanceResult
)


def test_optimize_table_syntax_generation():
    """Verify OPTIMIZE and Z-ORDER SQL string construction."""
    mgr = TableMaintenanceManager(spark=None)
    res = mgr.optimize_table(
        table_name="lakeforge_dev.bronze.customers",
        zorder_by=["customer_id", "country"],
        where_clause="ingestion_date >= '2026-01-01'",
        dry_run=True
    )
    assert res["status"] == "PLANNED"
    assert res["command"] == "OPTIMIZE lakeforge_dev.bronze.customers WHERE ingestion_date >= '2026-01-01' ZORDER BY (customer_id, country)"


def test_vacuum_table_syntax_generation():
    """Verify VACUUM SQL string construction with retention hours."""
    mgr = TableMaintenanceManager(spark=None)
    res = mgr.vacuum_table(
        table_name="lakeforge_dev.silver.orders_clean",
        retain_hours=336,  # 14 days
        dry_run=True
    )
    assert res["status"] == "PLANNED"
    assert res["command"] == "VACUUM lakeforge_dev.silver.orders_clean RETAIN 336 HOURS DRY RUN"


def test_analyze_table_syntax_generation():
    """Verify ANALYZE TABLE COMPUTE STATISTICS SQL string construction."""
    mgr = TableMaintenanceManager(spark=None)
    res = mgr.analyze_table(
        table_name="lakeforge_dev.gold.customer_metrics",
        columns=["customer_id", "total_spend"],
        dry_run=True
    )
    assert res["status"] == "PLANNED"
    assert res["command"] == "ANALYZE TABLE lakeforge_dev.gold.customer_metrics COMPUTE STATISTICS FOR COLUMNS customer_id, total_spend"


def test_run_table_maintenance_suite():
    """Verify execution of full maintenance suite on a single table."""
    mgr = create_maintenance_manager(spark=None)
    res = mgr.run_table_maintenance(
        table_name="lakeforge_dev.silver.customers_clean",
        zorder_by=["customer_id"],
        vacuum_retain_hours=168,
        dry_run=True
    )
    assert isinstance(res, MaintenanceResult)
    assert res.table_name == "lakeforge_dev.silver.customers_clean"
    assert "OPTIMIZE" in res.operations_executed
    assert "VACUUM" in res.operations_executed
    assert "ANALYZE" in res.operations_executed
    assert len(res.errors) == 0


def test_run_layer_maintenance_discovery():
    """Verify config-driven table discovery across layers for maintenance."""
    mgr = TableMaintenanceManager(spark=None)
    summary = mgr.run_layer_maintenance(
        config_dir="configs",
        stage="dev",
        layer="all",
        vacuum_retain_hours=168,
        dry_run=True
    )
    assert summary["stage"] == "dev"
    assert summary["layer"] == "all"
    assert summary["tables_processed"] > 0
    assert len(summary["results"]) == summary["tables_processed"]

    # Verify that bronze, silver, or gold tables were discovered
    table_names = [r["table_name"] for r in summary["results"]]
    assert any("bronze" in name or "customers" in name for name in table_names)
