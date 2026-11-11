"""
LakeForge Command Line Interface (CLI)
Provides command-line utilities for LakeForge project setup, validation, execution, governance, and deployment.
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
    validate_parser.add_argument("--client", default=None, help="Client ID / Tenant (optional)")

    # init
    init_parser = subparsers.add_parser("init", help="Initialize project directory structure and starter configs")
    init_parser.add_argument("target_dir", nargs="?", default=".", help="Target directory (default: .)")
    init_parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing starter files")

    # run
    run_parser = subparsers.add_parser("run", help="Execute or inspect a pipeline stage")
    run_parser.add_argument("--layer", choices=["bronze", "silver", "gold", "views", "governance"], required=True, help="Layer or stage")
    run_parser.add_argument("--name", default=None, help="Source name (Bronze) or target table (Silver/Gold) or view name (Views)")
    run_parser.add_argument("--source", default=None, help="Source table name for Silver layer")
    run_parser.add_argument("--env", default="dev", help="Environment (default: dev)")
    run_parser.add_argument("--client", default=None, help="Client ID / Tenant scope")
    run_parser.add_argument("--config-dir", default="configs", help="Config directory (default: configs)")
    run_parser.add_argument("--scd2", action="store_true", help="Enable SCD Type 2 merge for Silver")
    run_parser.add_argument("--dry-run", action="store_true", help="Dry run mode for governance/audit")

    # deploy
    deploy_parser = subparsers.add_parser("deploy", help="Deploy LakeForge to a target stage (dev, test, staging, prod)")
    deploy_parser.add_argument("--stage", "-s", required=True, choices=["dev", "test", "staging", "prod"], help="Target stage")
    deploy_parser.add_argument("--client", "-c", default=None, help="Client ID / Tenant scope (optional)")
    deploy_parser.add_argument("--component", default="all", choices=["all", "pipelines", "views", "governance", "package"], help="Component to deploy")
    deploy_parser.add_argument("--config-dir", default="configs", help="Config directory (default: configs)")
    deploy_parser.add_argument("--target-workspace", default=None, help="Target Databricks workspace path")
    deploy_parser.add_argument("--dry-run", action="store_true", help="Dry run mode (preview without applying)")

    # pr-review
    pr_parser = subparsers.add_parser("pr-review", help="Automated Pull Request Review & Duplicate Function Detection")
    pr_parser.add_argument("--base-ref", default="origin/main", help="Base git ref to compare against (default: origin/main)")
    pr_parser.add_argument("--repo-root", default=".", help="Root of repository")
    pr_parser.add_argument("--post-comment", action="store_true", help="Post comment to GitHub PR")
    pr_parser.add_argument("--fail-on-duplicate", action="store_true", help="Exit code 1 if duplicates found")

    args = parser.parse_args(argv)

    if args.subcommand == "validate":
        from .validate_config import main as validate_main
        val_args = [args.config_path, "--env", args.env]
        if args.client:
            val_args.extend(["--client", args.client])
        validate_main(val_args)
    elif args.subcommand == "init":
        from .init_project import main as init_main
        extra = ["-f"] if args.force else []
        init_main([args.target_dir] + extra)
    elif args.subcommand == "run":
        from .run_pipeline import main as run_main
        run_args = ["--layer", args.layer, "--env", args.env, "--config-dir", args.config_dir]
        if args.name:
            run_args.extend(["--name", args.name])
        if args.source:
            run_args.extend(["--source", args.source])
        if args.client:
            run_args.extend(["--client", args.client])
        if args.scd2:
            run_args.append("--scd2")
        if args.dry_run:
            run_args.append("--dry-run")
        run_main(run_args)
    elif args.subcommand == "deploy":
        from .deploy import main as deploy_main
        dep_args = ["--stage", args.stage, "--component", args.component, "--config-dir", args.config_dir]
        if args.client:
            dep_args.extend(["--client", args.client])
        if args.target_workspace:
            dep_args.extend(["--target-workspace", args.target_workspace])
        if args.dry_run:
            dep_args.append("--dry-run")
        deploy_main(dep_args)
    elif args.subcommand == "pr-review":
        from .pr_reviewer import main as pr_main
        sys_args = ["--base-ref", args.base_ref, "--repo-root", args.repo_root]
        if args.post_comment:
            sys_args.append("--post-comment")
        if args.fail_on_duplicate:
            sys_args.append("--fail-on-duplicate")
        old_argv = sys.argv
        try:
            sys.argv = ["lakeforge pr-review"] + sys_args
            pr_main()
        finally:
            sys.argv = old_argv
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
