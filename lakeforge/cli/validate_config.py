"""
LakeForge CLI - Config Validator
Validates LakeForge modular or monolithic JSON configuration files against schemas and rules.
Supports multi-tenant client configs and centralized governance access control.
"""
import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional

from lakeforge.metadata.config_parser import (
    ConfigParser,
    IngestionConfig,
    DQConfig,
    DQRule,
    ViewConfig,
)
from lakeforge.governance.governance_engine import GovernanceEngine


def validate_all_configs(
    config_dir_or_file: str,
    environment: str = "dev",
    client_id: Optional[str] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate all configurations in a directory or file.

    Returns:
        (is_valid: bool, summary_dict: Dict[str, Any])
    """
    path = Path(config_dir_or_file)
    if not path.exists():
        return False, {"error": f"Path not found: {config_dir_or_file}"}

    summary = {
        "path": str(path.resolve()),
        "environment": environment,
        "client_id": client_id,
        "valid_count": 0,
        "error_count": 0,
        "warning_count": 0,
        "categories": {}
    }

    def record(category: str, item_name: str, errors: List[str], warnings: List[str] = None):
        if category not in summary["categories"]:
            summary["categories"][category] = []
        is_ok = len(errors) == 0
        if is_ok:
            summary["valid_count"] += 1
        else:
            summary["error_count"] += len(errors)
        if warnings:
            summary["warning_count"] += len(warnings)
        summary["categories"][category].append({
            "name": item_name,
            "status": "PASS" if is_ok else "FAIL",
            "errors": errors,
            "warnings": warnings or []
        })

    # 1. Environments
    try:
        env_conf = ConfigParser.get_environment_config(str(path), environment)
        env_errs = []
        for req in ["catalog", "bronze_schema", "silver_schema", "gold_schema"]:
            if req not in env_conf:
                env_errs.append(f"Missing required key '{req}' in environment '{environment}'")
        record("environments", environment, env_errs)
    except Exception as e:
        record("environments", environment, [str(e)])

    # 2. Ingestion
    ingest_dirs = []
    if path.is_dir():
        if client_id:
            if (path / "ingestion" / client_id).is_dir():
                ingest_dirs.append((f"ingestion/{client_id}", path / "ingestion" / client_id, client_id))
            elif (path / "clients" / client_id / "ingestion").is_dir():
                ingest_dirs.append((f"clients/{client_id}/ingestion", path / "clients" / client_id / "ingestion", client_id))
        else:
            if (path / "ingestion").is_dir():
                for sub in sorted(p for p in (path / "ingestion").iterdir() if p.is_dir()):
                    ingest_dirs.append((f"ingestion/{sub.name}", sub, sub.name))
            if (path / "clients").is_dir():
                for sub in sorted(p for p in (path / "clients").iterdir() if p.is_dir()):
                    if (sub / "ingestion").is_dir():
                        ingest_dirs.append((f"clients/{sub.name}/ingestion", sub / "ingestion", sub.name))
        if (path / "ingestion").is_dir():
            ingest_dirs.append(("ingestion", path / "ingestion", None))

    for cat_label, idir, cid in ingest_dirs:
        for f in idir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if not data:
                    continue  # Skip empty/neutralized stub files
                sources = data if (isinstance(data, dict) and not any(k in data for k in ["source_type", "file_path", "bronze_table"])) else {f.stem: data}
                for src_name, src_spec in sources.items():
                    try:
                        cfg = ConfigParser.get_ingestion_config(str(path), src_name, environment, client_id=cid)
                        errs = ConfigParser.validate_ingestion_config(cfg)
                        item_id = f"[{cat_label}] {f.name}::{src_name}" if cid else f"{f.name}::{src_name}"
                        record("ingestion", item_id, errs)
                    except Exception as ex:
                        record("ingestion", f"{f.name}::{src_name}", [str(ex)])
            except Exception as e:
                record("ingestion", f.name, [f"Invalid JSON: {e}"])

    # 3. Data Quality
    dq_dirs = []
    if path.is_dir():
        if client_id:
            if (path / "dq" / client_id).is_dir():
                dq_dirs.append((f"dq/{client_id}", path / "dq" / client_id, client_id))
            elif (path / "clients" / client_id / "dq").is_dir():
                dq_dirs.append((f"clients/{client_id}/dq", path / "clients" / client_id / "dq", client_id))
        else:
            if (path / "dq").is_dir():
                for sub in sorted(p for p in (path / "dq").iterdir() if p.is_dir()):
                    dq_dirs.append((f"dq/{sub.name}", sub, sub.name))
            if (path / "clients").is_dir():
                for sub in sorted(p for p in (path / "clients").iterdir() if p.is_dir()):
                    if (sub / "dq").is_dir():
                        dq_dirs.append((f"clients/{sub.name}/dq", sub / "dq", sub.name))
        if (path / "dq").is_dir():
            dq_dirs.append(("dq", path / "dq", None))

    for cat_label, ddir, cid in dq_dirs:
        for f in ddir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if not data:
                    continue  # Skip empty/neutralized stub files
                dq_cfg = ConfigParser.get_dq_config(str(path), f.stem, environment, client_id=cid)
                errs = ConfigParser.validate_dq_config(dq_cfg)
                item_id = f"[{cat_label}] {f.name}" if cid else f.name
                record("dq", item_id, errs)
            except Exception as e:
                record("dq", f.name, [str(e)])

    # 4. Silver Transformations
    trans_dirs = []
    if path.is_dir():
        if client_id:
            if (path / "transformations" / client_id).is_dir():
                trans_dirs.append((f"transformations/{client_id}", path / "transformations" / client_id))
            elif (path / "clients" / client_id / "transformations").is_dir():
                trans_dirs.append((f"clients/{client_id}/transformations", path / "clients" / client_id / "transformations"))
        else:
            if (path / "transformations").is_dir():
                for sub in sorted(p for p in (path / "transformations").iterdir() if p.is_dir()):
                    trans_dirs.append((f"transformations/{sub.name}", sub))
            if (path / "clients").is_dir():
                for sub in sorted(p for p in (path / "clients").iterdir() if p.is_dir()):
                    if (sub / "transformations").is_dir():
                        trans_dirs.append((f"clients/{sub.name}/transformations", sub / "transformations"))
        if (path / "transformations").is_dir():
            trans_dirs.append(("transformations", path / "transformations"))

    for cat_label, tdir in trans_dirs:
        for f in tdir.glob("*.json"):
            errs = []
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if not data:
                    continue  # Skip empty/neutralized stub files
                if not data.get("source_table"):
                    errs.append("Missing 'source_table'")
                transformations = data.get("transformations")
                columns = data.get("columns")
                dedup = data.get("deduplication")
                custom_fn = data.get("custom_function") or data.get("custom_module")
                sql_file = data.get("sql_file")
                query = data.get("query")
                if transformations is None and columns is None and dedup is None and custom_fn is None and sql_file is None and query is None:
                    errs.append("Configuration must specify 'transformations', 'columns', 'deduplication', or 'custom_function'")
                if sql_file:
                    resolved_sql = ConfigParser.resolve_sql_query(str(path), "transformations", data, environment)
                    if not resolved_sql:
                        errs.append(f"Referenced sql_file '{sql_file}' could not be resolved or is empty")
                if transformations is not None and not isinstance(transformations, list):
                    errs.append("'transformations' must be a list")
                elif transformations is not None:
                    for idx, t in enumerate(transformations):
                        if not isinstance(t, dict) or "type" not in t:
                            errs.append(f"Step {idx}: missing 'type'")
                item_id = f"[{cat_label}] {f.name}" if ("clients" in cat_label or "/" in cat_label) else f.name
                record("transformations", item_id, errs)
            except Exception as e:
                record("transformations", f.name, [f"Invalid JSON: {e}"])

    # 5. Gold Aggregations
    agg_dir = path / "aggregations" if path.is_dir() else None
    if agg_dir and agg_dir.exists():
        for f in agg_dir.glob("*.json"):
            errs = []
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if not data:
                    continue  # Skip empty/neutralized stub files
                has_aggs = bool(data.get("aggregations"))
                has_custom = bool(data.get("custom_function") or data.get("custom_module"))
                has_query = bool(data.get("query") or data.get("sql_file"))
                if not data.get("source_tables") and not has_custom and not has_query:
                    errs.append("Missing 'source_tables'")
                if not has_aggs and not has_custom and not has_query:
                    errs.append("Configuration must specify 'aggregations' or 'custom_function'")
                if data.get("sql_file"):
                    resolved_sql = ConfigParser.resolve_sql_query(str(path), "aggregations", data, environment)
                    if not resolved_sql:
                        errs.append(f"Referenced sql_file '{data.get('sql_file')}' could not be resolved or is empty")
                record("aggregations", f.name, errs)
            except Exception as e:
                record("aggregations", f.name, [f"Invalid JSON: {e}"])

    # 6. Views
    try:
        views = ConfigParser.get_view_configs(str(path), environment, client_id=client_id)
        for v in views:
            errs = ConfigParser.validate_view_config(v)
            record("views", v.view_name, errs)
    except Exception as e:
        # Views are optional
        pass

    # 7. Centralized Governance
    try:
        gov_config = GovernanceEngine.load_governance_config(str(path), client_id=client_id)
        if gov_config.roles or gov_config.grants:
            gov_errs = []
            if not gov_config.grants:
                gov_errs.append("No valid grants could be resolved from defined roles")
            record("governance", "access_control.json", gov_errs)
    except Exception as e:
        record("governance", "access_control.json", [str(e)])

    is_overall_valid = summary["error_count"] == 0
    return is_overall_valid, summary


def print_summary(summary: Dict[str, Any]):
    """Print ASCII-safe validation report."""
    print("=" * 70)
    print(" LAKEFORGE CONFIGURATION VALIDATION REPORT")
    print("=" * 70)
    print(f"Path:        {summary.get('path')}")
    print(f"Environment: {summary.get('environment')}")
    if summary.get("client_id"):
        print(f"Client ID:   {summary.get('client_id')}")
    print(f"Passed:      {summary.get('valid_count')}")
    print(f"Errors:      {summary.get('error_count')}")
    print(f"Warnings:    {summary.get('warning_count')}")
    print("-" * 70)

    for cat, items in summary.get("categories", {}).items():
        print(f"\n[{cat.upper()}]")
        for it in items:
            mark = "[PASS]" if it["status"] == "PASS" else "[FAIL]"
            print(f"  {mark} {it['name']}")
            for err in it.get("errors", []):
                print(f"      * ERROR: {err}")
            for warn in it.get("warnings", []):
                print(f"      * WARN:  {warn}")

    print("=" * 70)
    if summary.get("error_count", 0) == 0:
        print("ALL CONFIGURATIONS VALID")
    else:
        print(f"VALIDATION FAILED: {summary['error_count']} error(s) found.")
    print("=" * 70)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    import argparse
    parser = argparse.ArgumentParser(description="Validate LakeForge pipeline configurations.")
    parser.add_argument("config_path", nargs="?", default="configs", help="Path to config directory or file (default: configs)")
    parser.add_argument("--env", default="dev", help="Environment to validate (default: dev)")
    parser.add_argument("--client", default=None, help="Client ID / Tenant (optional)")
    args = parser.parse_args(argv)

    is_valid, summary = validate_all_configs(args.config_path, args.env, client_id=args.client)
    print_summary(summary)
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
