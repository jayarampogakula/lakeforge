"""
LakeForge CLI - Pipeline Runner
Dispatches and runs LakeForge pipeline stages (Bronze, Silver, Gold).
"""
import sys
from pathlib import Path
from typing import Optional

from lakeforge.metadata.config_parser import ConfigParser


def run_bronze(config_dir: str, pipeline_name: str, environment: str = "dev"):
    """Execute Bronze ingestion."""
    print(f"Loading Bronze configuration for: {pipeline_name} (env={environment})...")
    ing_cfg = ConfigParser.get_ingestion_config(config_dir, pipeline_name, environment)
    dq_cfg = ConfigParser.get_dq_config(config_dir, pipeline_name, environment)

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


def run_silver(config_dir: str, target_table: str, source_table: str = None, environment: str = "dev", scd2: bool = False):
    """Execute Silver transformation."""
    source_tbl = source_table or target_table.replace("_clean", "")
    print(f"Loading Silver configuration for: {target_table} (source={source_tbl}, env={environment})...")
    trans_cfg = ConfigParser.get_silver_transformation_config(config_dir, target_table, environment)
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

        ing_cfg = ConfigParser.get_ingestion_config(config_dir, source_tbl, environment)
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


def run_gold(config_dir: str, target_table: str, environment: str = "dev"):
    """Execute Gold aggregation."""
    print(f"Loading Gold aggregation configuration for: {target_table} (env={environment})...")
    agg_cfg = ConfigParser.get_gold_aggregation_config(config_dir, target_table, environment)
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


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    import argparse
    parser = argparse.ArgumentParser(description="Run a LakeForge pipeline stage.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True, help="Medallion layer to run")
    parser.add_argument("--name", required=True, help="Source name (Bronze) or target table name (Silver/Gold)")
    parser.add_argument("--source", default=None, help="Source table name for Silver layer (optional)")
    parser.add_argument("--env", default="dev", help="Environment (default: dev)")
    parser.add_argument("--config-dir", default="configs", help="Configuration directory (default: configs)")
    parser.add_argument("--scd2", action="store_true", help="Use SCD Type 2 for Silver layer")
    args = parser.parse_args(argv)

    if args.layer == "bronze":
        run_bronze(args.config_dir, args.name, args.env)
    elif args.layer == "silver":
        run_silver(args.config_dir, args.name, args.source, args.env, args.scd2)
    elif args.layer == "gold":
        run_gold(args.config_dir, args.name, args.env)


if __name__ == "__main__":
    main()
