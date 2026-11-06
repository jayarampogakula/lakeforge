"""
LakeForge Command Line Interface (CLI)
Provides command-line utilities for LakeForge project setup, validation, and execution.
"""
import sys
import argparse


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog="lakeforge",
        description="LakeForge - Enterprise Databricks Medallion ETL & Data Quality Framework"
    )
    parser.add_argument("--version", "-v", action="version", version="lakeforge 1.0.0")

    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # validate
    validate_parser = subparsers.add_parser("validate", help="Validate LakeForge pipeline configurations")
    validate_parser.add_argument("config_path", nargs="?", default="configs", help="Config directory or file (default: configs)")
    validate_parser.add_argument("--env", default="dev", help="Environment to validate against (default: dev)")

    # init
    init_parser = subparsers.add_parser("init", help="Initialize project directory structure and starter configs")
    init_parser.add_argument("target_dir", nargs="?", default=".", help="Target directory (default: .)")
    init_parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing starter files")

    # run
    run_parser = subparsers.add_parser("run", help="Execute or inspect a pipeline stage")
    run_parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True, help="Layer (bronze, silver, gold)")
    run_parser.add_argument("--name", required=True, help="Source name (Bronze) or target table (Silver/Gold)")
    run_parser.add_argument("--source", default=None, help="Source table name for Silver layer")
    run_parser.add_argument("--env", default="dev", help="Environment (default: dev)")
    run_parser.add_argument("--config-dir", default="configs", help="Config directory (default: configs)")
    run_parser.add_argument("--scd2", action="store_true", help="Enable SCD Type 2 merge for Silver")

    args = parser.parse_args(argv)

    if args.subcommand == "validate":
        from .validate_config import main as validate_main
        validate_main([args.config_path, "--env", args.env])
    elif args.subcommand == "init":
        from .init_project import main as init_main
        extra = ["-f"] if args.force else []
        init_main([args.target_dir] + extra)
    elif args.subcommand == "run":
        from .run_pipeline import main as run_main
        run_args = ["--layer", args.layer, "--name", args.name, "--env", args.env, "--config-dir", args.config_dir]
        if args.source:
            run_args.extend(["--source", args.source])
        if args.scd2:
            run_args.append("--scd2")
        run_main(run_args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
