"""
Unit tests for LakeForge Observability components (metrics ledger, alerts, lineage).
"""
import pytest
import json
from pathlib import Path
from lakeforge.observability import (
    PipelineMetricsAuditor,
    PipelineRunMetric,
    create_metrics_auditor,
    AlertDispatcher,
    create_alert_dispatcher,
    LineageTracker,
    LineageNode
)


def test_metrics_auditor_local_ledger(tmp_path):
    """Verify metrics logging to local audit ledger."""
    ledger_file = tmp_path / "run_history.jsonl"
    auditor = create_metrics_auditor(spark=None)

    metric = PipelineRunMetric(
        run_id="run-12345",
        pipeline_name="sales_bronze",
        layer="bronze",
        environment="dev",
        status="SUCCESS",
        start_time="2026-11-06T12:00:00Z",
        end_time="2026-11-06T12:01:30Z",
        duration_seconds=90.0,
        rows_read=1000,
        rows_written=1000,
        client_id="client_alpha",
        trust_score=99.5
    )

    auditor.log_run(metric, local_ledger_path=str(ledger_file))
    assert ledger_file.exists()

    with open(ledger_file, "r", encoding="utf-8") as f:
        line = f.readline().strip()
        data = json.loads(line)
        assert data["run_id"] == "run-12345"
        assert data["client_id"] == "client_alpha"
        assert data["trust_score"] == 99.5


def test_alert_dispatcher_logging():
    """Verify AlertDispatcher dispatches without error."""
    dispatcher = create_alert_dispatcher(webhook_url=None)
    success = dispatcher.send_alert(
        title="Pipeline Succeeded",
        message="Bronze sales ingestion passed DQ",
        severity="INFO",
        context={"client_id": "client_alpha", "rows": 1000}
    )
    assert success is True


def test_lineage_tracker():
    """Verify LineageTracker records edges and exports graph."""
    tracker = LineageTracker()
    in_node = LineageNode(dataset_name="sales.csv", layer="source", client_id="client_alpha")
    out_node = LineageNode(dataset_name="sales", layer="bronze", catalog="lakeforge_client_alpha_dev", schema="bronze", client_id="client_alpha")

    edge = tracker.record_edge(
        pipeline_name="bronze_sales",
        operation="ingest",
        inputs=[in_node],
        output=out_node
    )
    assert edge.pipeline_name == "bronze_sales"
    assert out_node.fully_qualified_name == "lakeforge_client_alpha_dev.bronze.sales"

    graph = tracker.export_graph()
    assert len(graph) == 1
    assert graph[0]["operation"] == "ingest"
