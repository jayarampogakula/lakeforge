"""
LakeForge CLI - Pipeline Runner
Dispatches and runs LakeForge pipeline stages (Bronze, Silver, Gold, Views, Governance).
Supports multi-tenant client scoping and on-demand governance/views execution.
"""
import sys
from pathlib import Path
from typing import Optional

from lakeforge.metadata.config_parser import ConfigParser, load_view_configs
from lakeforge.governance.governance_engine import GovernanceEngine


def run_bronze(config_dir: str, pipeline_name: str, environment: str = "dev", client_id: Optional[str] = None):
    """Execute Bronze ingestion."""
    print(f"Loading Bronze configuration for: {pipeline_name} (client={client_id or 'global'}, env={environment})...")
    ing_cfg = ConfigParser.get_ingestion_config(config_dir, pipeline_name, environment, client_id=client_id)
    dq_cfg = ConfigParser.get_dq_config(config_dir, pipeline_name, environment, client_id=client_id)

    print(f"Target: {ing_cfg.target_catalog}.{ing_cfg.target_schema}.{ing_cfg.target_table}")
    print(f"Format: {ing_cfg.file_format} | Mode: {ing_cfg.mode}")
    print(f"DQ Rules: {len(dq_cfg.rules)} rules configured")

    try:
        from pyspark.sql import SparkSession
        from lakeforge.ingestion import create_universal_loader
        from lakeforge.dq import create_dq_engine
        from lakeforge.bronze import create_bronze_writer

        spark = SparkSession.builder.getOrCreate()
        print("Reading source data via UniversalLoader...")
        loader = create_universal_loader(spark)
        df_raw = loader.load_from_config(ing_cfg)
        print(f"Loaded {df_raw.count()} raw records.")

        dq_engine = create_dq_engine(spark)
        rules_list = [r.__dict__ for r in dq_cfg.rules]
        dq_results = dq_engine.validate_dataframe(df_raw, rules_list)
        print(f"DQ Passed: {dq_results['rules_passed']}/{dq_results['rules_executed']}")

        clean_df = df_raw.join(dq_results["quarantine_df"], on=ing_cfg.business_key, how="left_anti") if (dq_results.get("quarantine_count", 0) > 0 and ing_cfg.business_key) else df_raw
        writer = create_bronze_writer(spark)
        writer.write_to_bronze(
            df=clean_df,
            target_table=ing_cfg.target_table,
            catalog=ing_cfg.target_catalog,
            schema=ing_cfg.target_schema,
            mode=ing_cfg.mode,
            merge_keys=ing_cfg.business_key,
            add_audit_columns=True
        )
        print("Bronze ingestion complete!")
    except ImportError:
        print("[NOTE] Local PySpark runtime not available. In Databricks, run via the Bronze_Generic_Pipeline notebook.")


def run_silver(config_dir: str, target_table: str, source_table: str = None, environment: str = "dev", scd2: bool = False, client_id: Optional[str] = None):
    """Execute Silver transformation."""
    source_tbl = source_table or target_table.replace("_clean", "")
    print(f"Loading Silver configuration for: {target_table} (client={client_id or 'global'}, source={source_tbl}, env={environment})...")
    trans_cfg = ConfigParser.get_silver_transformation_config(config_dir, target_table, environment, client_id=client_id)
    print(f"Steps: {len(trans_cfg.get('transformations', []))} transformation(s)")

    try:
        from pyspark.sql import SparkSession
        from lakeforge.silver import SilverTransformer, SilverMergeEngine, create_scd_type2_handler

        spark = SparkSession.builder.getOrCreate()
        env_cfg = ConfigParser.get_environment_config(config_dir, environment)
        full_src = f"{env_cfg['catalog']}.{env_cfg['bronze_schema']}.{source_tbl}"
        full_tgt = f"{env_cfg['catalog']}.{env_cfg['silver_schema']}.{target_table}"

        df_src = spark.table(full_src)
        df_transformed = SilverTransformer.transform(df_src, trans_cfg.get("transformations", []))

        ing_cfg = ConfigParser.get_ingestion_config(config_dir, source_tbl, environment, client_id=client_id)
        b_keys = ing_cfg.business_key

        if scd2:
            scd = create_scd_type2_handler(spark)
            scd.merge_scd_type2(df_transformed, target_table, env_cfg["catalog"], env_cfg["silver_schema"], b_keys)
        else:
            mode_val = "merge" if ing_cfg.merge_strategy == "upsert" else ing_cfg.merge_strategy
            SilverMergeEngine.merge(spark, df_transformed, full_tgt, b_keys, mode=mode_val)
        print("Silver transformation complete!")
    except ImportError:
        print("[NOTE] Local PySpark runtime not available. In Databricks, run via the SILVER_Transformation_Pipeline notebook.")


def run_gold(config_dir: str, target_table: str, environment: str = "dev", client_id: Optional[str] = None):
    """Execute Gold aggregation."""
    print(f"Loading Gold aggregation configuration for: {target_table} (client={client_id or 'global'}, env={environment})...")
    agg_cfg = ConfigParser.get_gold_aggregation_config(config_dir, target_table, environment, client_id=client_id)
    print(f"Sources: {agg_cfg.get('source_tables')}")
    print(f"Aggregations: {len(agg_cfg.get('aggregations', []))}")

    try:
        from pyspark.sql import SparkSession
        from lakeforge.gold import GoldAggregator
        from lakeforge.silver import SilverMergeEngine

        spark = SparkSession.builder.getOrCreate()
        env_cfg = ConfigParser.get_environment_config(config_dir, environment)
        silver_dfs = {alias: spark.table(f"{env_cfg['catalog']}.{env_cfg['silver_schema']}.{tbl.split('.')[-1]}") for alias, tbl in agg_cfg["source_tables"].items()}

        result_df = GoldAggregator.aggregate(spark, silver_dfs, agg_cfg)
        full_gold = f"{env_cfg['catalog']}.{env_cfg['gold_schema']}.{target_table}"

        merge_strat = agg_cfg.get("merge_strategy", "overwrite")
        b_keys = agg_cfg.get("business_key", [])

        if merge_strat in ["upsert", "merge"] and b_keys:
            SilverMergeEngine.merge(spark, result_df, full_gold, b_keys, mode="merge")
        elif merge_strat == "append":
            SilverMergeEngine.merge(spark, result_df, full_gold, [], mode="append")
        else:
            result_df.write.format("delta").mode("overwrite").saveAsTable(full_gold)
        print("Gold aggregation complete!")
    except ImportError:
        print("[NOTE] Local PySpark runtime not available. In Databricks, run via the GOLD_Aggregation_Pipeline notebook.")


def run_views(config_dir: str, view_name: Optional[str] = None, environment: str = "dev", client_id: Optional[str] = None):
    """Execute on-demand View creation."""
    print(f"Loading Views configuration (client={client_id or 'global'}, env={environment})...")
    env_cfg = ConfigParser.get_environment_config(config_dir, environment)
    view_configs = ConfigParser.get_view_configs(config_dir, environment, client_id=client_id)
    if view_name:
        view_configs = [vc for vc in view_configs if vc.view_name == view_name]

    print(f"Loaded {len(view_configs)} view definition(s):")
    for vc in view_configs:
        print(f"  * {vc.catalog}.{vc.schema}.{vc.view_name} (type: {vc.definition_type})")

    try:
        from pyspark.sql import SparkSession
        from lakeforge.views.view_manager import create_view_manager

        spark = SparkSession.builder.getOrCreate()
        manager = create_view_manager(spark, env_cfg)
        manager.create_views(view_configs)
        print("Views created successfully!")
    except ImportError:
        print("[NOTE] Local PySpark runtime not available. In Databricks, run via the VIEWS_Creation_Pipeline notebook.")


def run_governance(config_dir: str, environment: str = "dev", dry_run: bool = False, client_id: Optional[str] = None, target_catalog: Optional[str] = None):
    """Execute on-demand Governance access control reconciliation."""
    print(f"Loading Governance configuration (client={client_id or 'global'}, env={environment}, dry_run={dry_run})...")
    engine = GovernanceEngine()
    result = engine.reconcile_permissions(
        config_path_or_dir=config_dir,
        environment=environment,
        dry_run=dry_run,
        target_catalog=target_catalog,
        client_id=client_id
    )
    print(f"Reconciliation Summary: {result['executed']} executed / {result['total_statements']} total grants")
    for stmt in result["statements"]:
        print(f"  * {stmt}")


def run_maintenance(config_dir: str, environment: str = "dev", target_layer: str = "all", table_name: Optional[str] = None, client_id: Optional[str] = None, vacuum_hours: int = 168, dry_run: bool = False):
    """Execute on-demand Delta table maintenance."""
    print(f"Loading Table Maintenance (layer={target_layer}, client={client_id or 'global'}, env={environment}, dry_run={dry_run})...")
    from lakeforge.maintenance.table_maintenance import TableMaintenanceManager
    mgr = TableMaintenanceManager()
    if table_name:
        res = mgr.run_table_maintenance(table_name=table_name, vacuum_retain_hours=vacuum_hours, dry_run=dry_run)
        print(f"Table Maintenance on {table_name}: {res.status}")
    else:
        summary = mgr.run_layer_maintenance(config_dir=config_dir, stage=environment, layer=target_layer, client_id=client_id, vacuum_retain_hours=vacuum_hours, dry_run=dry_run)
        print(f"Layer Maintenance Summary: {summary['tables_processed']} tables processed for layer {target_layer} in stage {environment}")


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    import argparse
    parser = argparse.ArgumentParser(description="Run a LakeForge pipeline stage.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "views", "governance", "maintenance"], required=True, help="Medallion layer or on-demand stage")
    parser.add_argument("--name", default=None, help="Source name (Bronze) or target table (Silver/Gold) or view name (Views)")
    parser.add_argument("--source", default=None, help="Source table name for Silver layer (optional)")
    parser.add_argument("--env", default="dev", help="Environment (default: dev)")
    parser.add_argument("--client", default=None, help="Client ID / Tenant (optional)")
    parser.add_argument("--config-dir", default="configs", help="Configuration directory (default: configs)")
    parser.add_argument("--scd2", action="store_true", help="Use SCD Type 2 for Silver layer")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode for governance/audit")
    parser.add_argument("--vacuum-hours", type=int, default=168, help="Vacuum retention hours (default: 168)")
    args = parser.parse_args(argv)

    if args.layer == "bronze":
        if not args.name:
            parser.error("--name is required for bronze layer")
        run_bronze(args.config_dir, args.name, args.env, client_id=args.client)
    elif args.layer == "silver":
        if not args.name:
            parser.error("--name is required for silver layer")
        run_silver(args.config_dir, args.name, args.source, args.env, args.scd2, client_id=args.client)
    elif args.layer == "gold":
        if not args.name:
            parser.error("--name is required for gold layer")
        run_gold(args.config_dir, args.name, args.env, client_id=args.client)
    elif args.layer == "views":
        run_views(args.config_dir, args.name, args.env, client_id=args.client)
    elif args.layer == "governance":
        run_governance(args.config_dir, args.env, dry_run=args.dry_run, client_id=args.client)
    elif args.layer == "maintenance":
        run_maintenance(args.config_dir, environment=args.env, target_layer=args.name or "all", client_id=args.client, vacuum_hours=args.vacuum_hours, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
