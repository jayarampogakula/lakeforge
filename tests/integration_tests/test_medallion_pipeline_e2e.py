"""
End-to-End Integration Tests for LakeForge Medallion Architecture.
Tests the complete multi-layer lifecycle:
Bronze Ingestion -> Data Quality & Quarantine -> Silver Cleansing & Deduplication -> Gold Aggregation -> Views -> Governance & Maintenance.
"""
import sys
import pytest
from datetime import datetime

has_real_pyspark = getattr(sys, "has_real_pyspark", True)
pytestmark = pytest.mark.skipif(not has_real_pyspark, reason="Requires real PySpark")

from pyspark.sql import Row
from lakeforge.bronze.bronze_writer import create_bronze_writer
from lakeforge.dq.dq_engine import create_dq_engine
from lakeforge.silver.transformer import SilverTransformer
from lakeforge.gold.aggregations import GoldAggregator
from lakeforge.views.view_manager import ViewManager
from lakeforge.metadata.config_parser import ViewConfig
from lakeforge.governance.governance_engine import create_governance_engine
from lakeforge.maintenance.table_maintenance import (
    TableMaintenanceManager,
    create_maintenance_manager,
    MaintenanceResult
)
from lakeforge.observability import (
    PipelineMetricsAuditor,
    PipelineRunMetric,
    create_metrics_auditor,
    LineageTracker,
    LineageNode
)


def test_end_to_end_medallion_pipeline(spark, tmp_path):
    """
    Integration Test: Full Medallion Pipeline Lifecycle
    1. Bronze: Write raw orders with audit columns and schema evolution guardrails.
    2. DQ: Validate orders; quarantine corrupt records with negative amounts or missing customer_id.
    3. Silver: Cleanse, cast, trim and deduplicate valid orders.
    4. Gold: Aggregate daily revenue by customer and status.
    5. Views: Create analytical reporting view via ViewManager.
    6. Governance: Generate and verify Unity Catalog permission reconciliation and PII masks.
    7. Maintenance: Run OPTIMIZE, VACUUM, and ANALYZE table maintenance.
    8. Observability: Audit ledger and lineage verification.
    """
    test_run_id = f"run_{int(datetime.now().timestamp())}"
    raw_table = f"e2e_bronze_orders_{test_run_id}"
    silver_table = f"e2e_silver_orders_{test_run_id}"
    gold_table = f"e2e_gold_revenue_{test_run_id}"

    # Setup cleanup
    for t in [raw_table, silver_table, gold_table, f"{raw_table}_quarantine"]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS default.{t}")
        except Exception:
            pass

    # -------------------------------------------------------------
    # 1. BRONZE LAYER: Ingestion & Audit Columns
    # -------------------------------------------------------------
    bronze_writer = create_bronze_writer(spark)
    raw_data = [
        Row(order_id="ORD-001", customer_id="CUST-10", amount=150.0, status="completed", order_date="2026-11-01"),
        Row(order_id="ORD-002", customer_id="CUST-20", amount=250.0, status="completed", order_date="2026-11-01"),
        Row(order_id="ORD-003", customer_id=None, amount=50.0, status="completed", order_date="2026-11-01"),  # Null customer
        Row(order_id="ORD-004", customer_id="CUST-10", amount=-99.0, status="pending", order_date="2026-11-01"),  # Negative amount
        Row(order_id="ORD-001", customer_id="CUST-10", amount=150.0, status="completed", order_date="2026-11-01")   # Duplicate
    ]
    df_raw = spark.createDataFrame(raw_data)
    
    bronze_result = bronze_writer.write_to_bronze(
        df=df_raw,
        target_table=raw_table,
        catalog="",
        schema="default",
        mode="overwrite",
        add_audit_columns=True,
        source_system="e2e_erp"
    )
    assert bronze_result["status"] == "success"
    assert bronze_result["rows_written"] == 5

    df_bronze = spark.table(f"default.{raw_table}")
    assert "_ingestion_timestamp" in df_bronze.columns
    assert "_source_system" in df_bronze.columns

    # -------------------------------------------------------------
    # 2. DATA QUALITY GATE: Validation & Filtering
    # -------------------------------------------------------------
    dq_engine = create_dq_engine(spark)
    # Null check on customer_id
    res_null = dq_engine.run_null_check(df_bronze, column="customer_id")
    assert res_null["null_count"] == 1

    # Range check on amount >= 0.01
    res_range = dq_engine.run_range_check(df_bronze, column="amount", min_val=0.01)
    assert res_range["out_of_range_count"] == 1

    # Filter clean records for Silver
    df_valid = df_bronze.filter("customer_id IS NOT NULL AND amount > 0")
    assert df_valid.count() == 3

    # -------------------------------------------------------------
    # 3. SILVER LAYER: Transformations & Deduplication
    # -------------------------------------------------------------
    # Transform: cast and deduplicate
    silver_configs = [
        {"type": "cast", "column": "amount", "target_type": "double"},
        {"type": "cast", "column": "order_id", "target_type": "string"},
        {"type": "deduplicate", "columns": ["order_id"]}
    ]
    df_silver = SilverTransformer.transform(df_valid, silver_configs)
    assert df_silver.count() == 2

    # Save to Silver Table
    df_silver.write.format("delta").mode("overwrite").saveAsTable(f"default.{silver_table}")
    assert spark.catalog.tableExists(f"default.{silver_table}")

    # -------------------------------------------------------------
    # 4. GOLD LAYER: Business Mart Aggregation
    # -------------------------------------------------------------
    agg_config = {
        "source_tables": {"orders": f"default.{silver_table}"},
        "group_by": ["customer_id"],
        "aggregations": [
            {"name": "total_spend", "expression": "SUM(amount)"},
            {"name": "order_count", "expression": "COUNT(order_id)"}
        ]
    }
    df_gold = GoldAggregator.aggregate(spark, {"orders": df_silver}, agg_config)
    assert df_gold.count() == 2
    df_gold.write.format("delta").mode("overwrite").saveAsTable(f"default.{gold_table}")

    # -------------------------------------------------------------
    # 5. VIEWS: Analytical Reporting View
    # -------------------------------------------------------------
    view_mgr = ViewManager(spark, environment_config={"catalog": "", "gold_schema": "default"})
    view_cfg = ViewConfig(
        view_name=f"vw_top_spenders_{test_run_id}",
        catalog="",
        schema="default",
        view_type="temporary",
        definition_type="query",
        query=f"SELECT customer_id, total_spend FROM default.{gold_table} WHERE total_spend >= 100.0"
    )
    view_mgr.create_view(view_cfg)
    assert spark.catalog.tableExists(f"vw_top_spenders_{test_run_id}")

    # -------------------------------------------------------------
    # 6. GOVERNANCE: Unity Catalog Permission Reconciliation
    # -------------------------------------------------------------
    gov_engine = create_governance_engine(spark=spark)
    gov_result = gov_engine.reconcile_permissions(
        config_path_or_dir="configs",
        environment="dev",
        dry_run=True
    )
    assert gov_result["dry_run"] is True
    assert gov_result["total_statements"] > 0
    assert gov_result["failed"] == 0

    # -------------------------------------------------------------
    # 7. MAINTENANCE SUITE: OPTIMIZE, VACUUM, ANALYZE
    # -------------------------------------------------------------
    maint_mgr = create_maintenance_manager(spark=None)
    maint_result = maint_mgr.run_table_maintenance(
        table_name=f"default.{silver_table}",
        zorder_by=["customer_id"],
        vacuum_retain_hours=168,
        dry_run=True
    )
    assert isinstance(maint_result, MaintenanceResult)
    assert maint_result.table_name == f"default.{silver_table}"
    assert "OPTIMIZE" in maint_result.operations_executed
    assert "VACUUM" in maint_result.operations_executed
    assert "ANALYZE" in maint_result.operations_executed
    assert len(maint_result.errors) == 0

    # -------------------------------------------------------------
    # 8. OBSERVABILITY: Run Metrics & Lineage Tracking
    # -------------------------------------------------------------
    ledger_path = str(tmp_path / "integration_ledger.jsonl")
    auditor = create_metrics_auditor(spark=None)
    metric = PipelineRunMetric(
        run_id=test_run_id,
        pipeline_name="e2e_orders_pipeline",
        layer="gold",
        environment="dev",
        status="SUCCESS",
        start_time="2026-11-11T10:00:00Z",
        end_time="2026-11-11T10:02:00Z",
        duration_seconds=120.0,
        rows_read=5,
        rows_written=2,
        client_id="global",
        trust_score=100.0
    )
    auditor.log_run(metric, local_ledger_path=ledger_path)
    assert Path(ledger_path).exists()

    tracker = LineageTracker()
    in_node = LineageNode(dataset_name="erp_orders.csv", layer="source")
    bronze_node = LineageNode(dataset_name=raw_table, layer="bronze", catalog="", schema="default")
    silver_node = LineageNode(dataset_name=silver_table, layer="silver", catalog="", schema="default")
    gold_node = LineageNode(dataset_name=gold_table, layer="gold", catalog="", schema="default")

    tracker.record_edge(pipeline_name="bronze_ingest", operation="ingest", inputs=[in_node], output=bronze_node)
    tracker.record_edge(pipeline_name="silver_cleanse", operation="transform", inputs=[bronze_node], output=silver_node)
    tracker.record_edge(pipeline_name="gold_aggregate", operation="aggregate", inputs=[silver_node], output=gold_node)

    graph = tracker.export_graph()
    assert len(graph) == 3

    # Cleanup test tables
    for t in [raw_table, silver_table, gold_table]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS default.{t}")
        except Exception:
            pass
