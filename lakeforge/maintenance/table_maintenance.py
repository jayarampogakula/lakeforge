"""
LakeForge Automated Delta Table Maintenance.
Handles OPTIMIZE, Z-ORDER, VACUUM (stale log/file purging), and ANALYZE TABLE statistics computation.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

from lakeforge.metadata.config_parser import ConfigParser

logger = logging.getLogger(__name__)


@dataclass
class MaintenanceResult:
    """Stores the execution output of a table maintenance operation."""
    table_name: str
    operations_executed: List[str] = field(default_factory=list)
    status: str = "SUCCESS"  # SUCCESS, FAILED, SKIPPED
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


class TableMaintenanceManager:
    """
    Automates Delta Lake maintenance operations across Bronze, Silver, and Gold tables.
    """

    def __init__(self, spark=None):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def build_fully_qualified_name(self, catalog: Optional[str], schema: Optional[str], table: str) -> str:
        """Constructs a fully qualified table identifier."""
        parts = [p for p in [catalog, schema, table] if p]
        return ".".join(parts)

    def optimize_table(
        self,
        table_name: str,
        zorder_by: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute Delta Lake OPTIMIZE with optional Z-ORDER clustering.
        """
        sql = f"OPTIMIZE {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if zorder_by:
            cols = ", ".join(zorder_by)
            sql += f" ZORDER BY ({cols})"

        self.logger.info(f"Running OPTIMIZE: {sql}")
        if dry_run or self.spark is None:
            return {"command": sql, "dry_run": True, "status": "PLANNED"}

        try:
            res_df = self.spark.sql(sql)
            metrics = res_df.collect()[0].asDict() if res_df.count() > 0 else {}
            return {"command": sql, "metrics": metrics, "status": "SUCCESS"}
        except Exception as e:
            msg = f"OPTIMIZE failed on {table_name}: {str(e)}"
            self.logger.warning(msg)
            return {"command": sql, "error": msg, "status": "FAILED"}

    def vacuum_table(
        self,
        table_name: str,
        retain_hours: int = 168,  # Default 7 days
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute Delta Lake VACUUM to remove files no longer referenced by the latest snapshot.
        """
        sql = f"VACUUM {table_name} RETAIN {retain_hours} HOURS"
        if dry_run:
            sql += " DRY RUN"

        self.logger.info(f"Running VACUUM: {sql}")
        if self.spark is None:
            return {"command": sql, "dry_run": dry_run, "status": "PLANNED"}

        try:
            res_df = self.spark.sql(sql)
            deleted_files = [r[0] for r in res_df.collect()] if res_df.count() > 0 else []
            return {
                "command": sql,
                "deleted_files_count": len(deleted_files),
                "deleted_files_sample": deleted_files[:10],
                "status": "SUCCESS"
            }
        except Exception as e:
            msg = f"VACUUM failed on {table_name}: {str(e)}"
            self.logger.warning(msg)
            return {"command": sql, "error": msg, "status": "FAILED"}

    def analyze_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute ANALYZE TABLE COMPUTE STATISTICS for query optimizer plan improvement.
        """
        sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"
        if columns:
            cols = ", ".join(columns)
            sql += f" FOR COLUMNS {cols}"

        self.logger.info(f"Running ANALYZE TABLE: {sql}")
        if dry_run or self.spark is None:
            return {"command": sql, "dry_run": True, "status": "PLANNED"}

        try:
            self.spark.sql(sql)
            return {"command": sql, "status": "SUCCESS"}
        except Exception as e:
            msg = f"ANALYZE failed on {table_name}: {str(e)}"
            self.logger.warning(msg)
            return {"command": sql, "error": msg, "status": "FAILED"}

    def run_table_maintenance(
        self,
        table_name: str,
        zorder_by: Optional[List[str]] = None,
        vacuum_retain_hours: int = 168,
        analyze_columns: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> MaintenanceResult:
        """
        Run a full maintenance suite (OPTIMIZE + Z-ORDER, VACUUM, and ANALYZE) on a single table.
        """
        result = MaintenanceResult(table_name=table_name)

        # 1. OPTIMIZE & Z-ORDER
        opt_res = self.optimize_table(table_name, zorder_by=zorder_by, dry_run=dry_run)
        result.operations_executed.append("OPTIMIZE")
        result.details["optimize"] = opt_res
        if opt_res.get("status") == "FAILED":
            result.errors.append(opt_res.get("error", "OPTIMIZE failed"))

        # 2. VACUUM
        vac_res = self.vacuum_table(table_name, retain_hours=vacuum_retain_hours, dry_run=dry_run)
        result.operations_executed.append("VACUUM")
        result.details["vacuum"] = vac_res
        if vac_res.get("status") == "FAILED":
            result.errors.append(vac_res.get("error", "VACUUM failed"))

        # 3. ANALYZE STATISTICS
        ana_res = self.analyze_table(table_name, columns=analyze_columns, dry_run=dry_run)
        result.operations_executed.append("ANALYZE")
        result.details["analyze"] = ana_res
        if ana_res.get("status") == "FAILED":
            result.errors.append(ana_res.get("error", "ANALYZE failed"))

        if result.errors:
            result.status = "PARTIAL" if len(result.errors) < 3 else "FAILED"
        return result

    def run_layer_maintenance(
        self,
        config_dir: str,
        stage: str = "dev",
        layer: str = "all",  # bronze, silver, gold, all
        client_id: Optional[str] = None,
        vacuum_retain_hours: int = 168,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Discover tables for the given stage and layer from configurations,
        and execute maintenance on all identified tables.
        """
        env_config = ConfigParser.get_environment_config(config_dir, stage)
        cat = env_config["catalog"]
        tables_to_maintain: List[Dict[str, Any]] = []

        path = Path(config_dir)

        # 1. Bronze tables
        if layer in ["all", "bronze"]:
            ingest_dir = path / "ingestion"
            if ingest_dir.is_dir():
                for f in ingest_dir.glob("*.json"):
                    try:
                        cfg = ConfigParser.get_ingestion_config(config_dir, f.stem, stage, client_id=client_id)
                        full_name = f"{cfg.target_catalog}.{cfg.target_schema}.{cfg.target_table}"
                        tables_to_maintain.append({
                            "name": full_name,
                            "layer": "bronze",
                            "zorder": cfg.business_key
                        })
                    except Exception:
                        pass

        # 2. Silver tables
        if layer in ["all", "silver"]:
            trans_dir = path / "transformations"
            if trans_dir.is_dir():
                for f in trans_dir.glob("*.json"):
                    try:
                        cfg = ConfigParser.get_silver_transformation_config(config_dir, f.stem, stage, client_id=client_id)
                        target = cfg.get("target_table", f.stem)
                        full_name = f"{cat}.{env_config['silver_schema']}.{target}"
                        dedup = cfg.get("deduplication", {})
                        zorder = dedup.get("key_columns") if isinstance(dedup, dict) else None
                        tables_to_maintain.append({
                            "name": full_name,
                            "layer": "silver",
                            "zorder": zorder
                        })
                    except Exception:
                        pass

        # 3. Gold tables
        if layer in ["all", "gold"]:
            agg_dir = path / "aggregations"
            if agg_dir.is_dir():
                for f in agg_dir.glob("*.json"):
                    try:
                        cfg = ConfigParser.get_gold_aggregation_config(config_dir, f.stem, stage, client_id=client_id)
                        full_name = f"{cat}.{env_config['gold_schema']}.{f.stem}"
                        zorder = cfg.get("business_key") or cfg.get("group_by")
                        tables_to_maintain.append({
                            "name": full_name,
                            "layer": "gold",
                            "zorder": zorder if isinstance(zorder, list) else None
                        })
                    except Exception:
                        pass

        results = []
        for t in tables_to_maintain:
            m_res = self.run_table_maintenance(
                table_name=t["name"],
                zorder_by=t.get("zorder"),
                vacuum_retain_hours=vacuum_retain_hours,
                dry_run=dry_run
            )
            results.append(m_res)

        return {
            "stage": stage,
            "layer": layer,
            "dry_run": dry_run,
            "tables_processed": len(tables_to_maintain),
            "results": [r.__dict__ for r in results]
        }


def create_maintenance_manager(spark=None) -> TableMaintenanceManager:
    """Factory function for TableMaintenanceManager."""
    return TableMaintenanceManager(spark=spark)
