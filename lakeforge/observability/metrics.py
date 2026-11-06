"""
LakeForge Observability - Pipeline Run Metrics and Audit Ledger.
Logs execution metrics (rows read, rows written, duration, trust score, status) into Delta monitoring tables.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineRunMetric:
    """Metrics recorded for a single pipeline run."""
    run_id: str
    pipeline_name: str
    layer: str  # bronze, silver, gold, views, governance
    environment: str
    status: str  # SUCCESS, FAILED, PARTIAL
    start_time: str
    end_time: str
    duration_seconds: float
    rows_read: int = 0
    rows_written: int = 0
    client_id: Optional[str] = None
    trust_score: Optional[float] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PipelineMetricsAuditor:
    """
    Audits and records pipeline execution metrics into Delta Lake and/or file ledger.
    """

    def __init__(self, spark=None, monitoring_catalog: str = "lakeforge_dev", monitoring_schema: str = "monitoring"):
        self.spark = spark
        self.monitoring_catalog = monitoring_catalog
        self.monitoring_schema = monitoring_schema
        self.target_table = f"{monitoring_catalog}.{monitoring_schema}.pipeline_run_history"
        self.logger = logging.getLogger(__name__)

    def log_run(self, metric: PipelineRunMetric, local_ledger_path: Optional[str] = None) -> None:
        """
        Record a run metric to Delta table if Spark is available, or to a local JSONL audit file.
        """
        record = metric.to_dict()
        self.logger.info(
            f"Run Audit: [{metric.status}] {metric.layer}.{metric.pipeline_name} "
            f"(client={metric.client_id or 'global'}, duration={metric.duration_seconds:.2f}s, "
            f"written={metric.rows_written}, trust={metric.trust_score})"
        )

        # 1. Delta Table Logging via Spark
        if self.spark is not None:
            try:
                # Ensure monitoring schema exists
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.monitoring_catalog}.{self.monitoring_schema}")
                metric_df = self.spark.createDataFrame([record])
                metric_df.write.format("delta").mode("append").saveAsTable(self.target_table)
                self.logger.info(f"Appended run metric to Delta table: {self.target_table}")
                return
            except Exception as e:
                self.logger.warning(f"Could not write metric to Delta table {self.target_table}: {str(e)}")

        # 2. Fallback to Local Audit Ledger (JSON Lines)
        if local_ledger_path:
            p = Path(local_ledger_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            self.logger.debug(f"Appended run metric to local ledger: {p}")


def create_metrics_auditor(spark=None, monitoring_catalog: str = "lakeforge_dev") -> PipelineMetricsAuditor:
    """Factory function for PipelineMetricsAuditor."""
    return PipelineMetricsAuditor(spark=spark, monitoring_catalog=monitoring_catalog)
