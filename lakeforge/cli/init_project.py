"""
LakeForge CLI - Project Initializer
Scaffolds the recommended enterprise medallion directory structure and starter configs.
"""
import sys
import json
from pathlib import Path


SAMPLE_ENV = {
    "dev": {
        "catalog": "lakehouse_dev",
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "gold_schema": "gold"
    },
    "prod": {
        "catalog": "lakehouse_prod",
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "gold_schema": "gold"
    }
}

SAMPLE_INGESTION = {
    "sample_sales": {
        "source_type": "csv",
        "file_path": "/Volumes/lakehouse/raw/data/sales.csv",
        "file_format": "csv",
        "options": {
            "header": "true",
            "inferSchema": "true"
        },
        "catalog": "lakehouse_dev",
        "schema": "bronze",
        "bronze_table": "sales_raw",
        "business_key": ["order_id"],
        "merge_strategy": "upsert"
    }
}

SAMPLE_DQ = [
    {
        "rule_id": "r_01",
        "rule_name": "order_id_not_null",
        "column": "order_id",
        "validation_type": "not_null",
        "severity": "critical",
        "action": "quarantine"
    },
    {
        "rule_id": "r_02",
        "rule_name": "order_id_unique",
        "column": "order_id",
        "validation_type": "unique",
        "severity": "critical",
        "action": "quarantine"
    }
]

SAMPLE_TRANSFORMATION = {
    "source_table": "sales_raw",
    "transformations": [
        {
            "type": "deduplicate",
            "columns": ["order_id"],
            "strategy": "keep_latest",
            "order_by": "_ingestion_timestamp"
        },
        {
            "type": "filter",
            "condition": "order_id IS NOT NULL"
        }
    ]
}

SAMPLE_AGGREGATION = {
    "description": "Daily sales summary mart",
    "merge_strategy": "upsert",
    "business_key": ["sale_date"],
    "source_tables": {
        "sales": "silver.sales_clean"
    },
    "group_by": ["sale_date"],
    "aggregations": [
        {
            "name": "total_revenue",
            "expression": "SUM(amount)"
        },
        {
            "name": "total_orders",
            "expression": "COUNT(*)"
        }
    ]
}

SAMPLE_VIEWS = {
    "v_active_sales": {
        "definition_type": "table",
        "schema_type": "silver",
        "source_table": "sales_clean",
        "select_columns": ["order_id", "sale_date", "amount"],
        "filter_condition": "amount > 0"
    }
}


def init_project(target_dir: str = ".", force: bool = False) -> Path:
    """Initialize a LakeForge project directory structure."""
    base = Path(target_dir).resolve()
    cfg_base = base / "configs"

    subfolders = [
        ("environments", "env_settings.json", SAMPLE_ENV),
        ("ingestion", "sample_sales.json", SAMPLE_INGESTION),
        ("dq", "sample_sales.json", SAMPLE_DQ),
        ("transformations", "sales_clean.json", SAMPLE_TRANSFORMATION),
        ("aggregations", "sales_summary.json", SAMPLE_AGGREGATION),
        ("views", "reporting_views.json", SAMPLE_VIEWS)
    ]

    print(f"Initializing LakeForge project in: {base}")
    cfg_base.mkdir(parents=True, exist_ok=True)

    for folder_name, file_name, sample_data in subfolders:
        folder = cfg_base / folder_name
        folder.mkdir(parents=True, exist_ok=True)
        target_file = folder / file_name

        if target_file.exists() and not force:
            print(f"  [EXISTS] {target_file.relative_to(base)}")
        else:
            with open(target_file, "w", encoding="utf-8") as f:
                json.dump(sample_data, f, indent=2)
            print(f"  [CREATE] {target_file.relative_to(base)}")

    print("\nProject initialized successfully!")
    print(f"Configuration root: {cfg_base}")
    print("Next step: run 'lakeforge validate' to check your configurations.")
    return base


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    import argparse
    parser = argparse.ArgumentParser(description="Initialize LakeForge project structure and starter configs.")
    parser.add_argument("target_dir", nargs="?", default=".", help="Directory to initialize (default: current directory)")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing starter files")
    args = parser.parse_args(argv)

    init_project(args.target_dir, args.force)


if __name__ == "__main__":
    main()
