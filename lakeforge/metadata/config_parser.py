"""
LakeForge Config Parser Module
Handles JSON configuration parsing for ingestion, DQ rules, and pipeline configs.
"""
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field, asdict


@dataclass
class IngestionConfig:
    """Configuration for data ingestion."""
    source_name: str
    source_type: str  # csv, excel, json, api, sql
    source_path: str
    target_table: str
    target_catalog: str
    target_schema: str
    file_format: Optional[str] = None
    
    # CSV/Excel specific
    header: bool = True
    delimiter: Optional[str] = ","
    encoding: Optional[str] = "utf-8"
    sheet_name: Optional[str] = None
    
    # Schema options
    infer_schema: bool = True
    schema_path: Optional[str] = None
    
    # Load options
    mode: str = "append"  # append, overwrite, merge
    partition_by: Optional[List[str]] = None
    
    # Audit columns
    add_audit_columns: bool = True
    
    # Key properties
    business_key: List[str] = field(default_factory=list)
    merge_strategy: str = "append"
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return asdict(self)
    
    def to_json(self, file_path: str, indent: int = 2):
        """Save config to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=indent)


@dataclass
class DQRule:
    """Data Quality rule configuration."""
    rule_name: str
    rule_id: Optional[str] = None
    rule_type: Optional[str] = None  # null_check, duplicate_check, regex_check, range_check, custom_sql, datatype_check
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    validation_type: Optional[str] = None  # Map validation_type from configs
    
    # Rule-specific parameters
    threshold: Optional[float] = None
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    custom_sql: Optional[str] = None
    
    # Referential integrity / Datatype / Threshold parameters
    source_column: Optional[str] = None
    reference_table: Optional[str] = None
    reference_column: Optional[str] = None
    expected_type: Optional[str] = None
    baseline_count: Optional[int] = None
    threshold_percent: Optional[float] = None
    
    # Action on failure
    action: str = "quarantine"  # quarantine, fail, warn
    severity: str = "error"  # error, warning, info

    def __post_init__(self):
        if not self.rule_type and self.validation_type:
            val_map = {
                "not_null": "null_check",
                "unique": "duplicate_check",
                "regex": "regex_check",
                "range": "range_check",
                "custom_sql": "custom_sql",
                "datatype": "datatype_check"
            }
            self.rule_type = val_map.get(self.validation_type, self.validation_type)


@dataclass
class DQConfig:
    """Configuration for data quality checks."""
    table_name: str
    catalog: str
    schema: str
    rules: List[DQRule]
    quarantine_table: Optional[str] = None
    enable_scorecard: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        config_dict = {
            "table_name": self.table_name,
            "catalog": self.catalog,
            "schema": self.schema,
            "quarantine_table": self.quarantine_table,
            "enable_scorecard": self.enable_scorecard,
            "rules": [asdict(rule) for rule in self.rules]
        }
        return config_dict
    
    def to_json(self, file_path: str, indent: int = 2):
        """Save config to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=indent)


@dataclass
class ViewConfig:
    """Configuration for creating a Spark SQL view."""
    view_name: str
    catalog: str
    schema: str
    definition_type: str  # table, query
    view_type: str = "persistent"  # persistent, temp
    source_table: Optional[str] = None
    select_columns: Optional[List[str]] = None
    filter_condition: Optional[str] = None
    query: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return asdict(self)


class ConfigParser:
    """Parser for LakeForge JSON configurations."""
    
    @staticmethod
    def load_merged_json(path_or_dir: str) -> Dict[str, Any]:
        """
        Load JSON configuration from a file or load and merge all JSON files in a directory.
        """
        path = Path(path_or_dir)
        if not path.exists():
            raise FileNotFoundError(f"Configuration path does not exist: {path_or_dir}")
            
        if path.is_file():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        # If it is a directory, load and merge all JSON files
        merged = {}
        unwrapped_keys = {
            "source_type", "file_path", "file_format", "bronze_table", "silver_table", 
            "merge_strategy", "business_key", "schema", "options", "rules", "table_name", 
            "source_table", "transformations", "source_tables", "group_by", "aggregations", 
            "definition_type", "schema_type", "query", "filter_condition", "select_columns"
        }
        for json_file in path.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    content = json.load(f)
            except Exception as e:
                # Log or raise error if a JSON file is corrupt
                raise ValueError(f"Failed to parse JSON file {json_file}: {str(e)}")
                
            if isinstance(content, dict):
                # Check if it is an unwrapped config file
                is_unwrapped = any(k in unwrapped_keys for k in content.keys())
                if is_unwrapped:
                    stem = json_file.stem
                    merged[stem] = content
                else:
                    # First level deep merge
                    for k, v in content.items():
                        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                            merged[k].update(v)
                        else:
                            merged[k] = v
            else:
                stem = json_file.stem
                merged[stem] = content
        return merged

    @staticmethod
    def get_environment_config(
        config_dir_or_file: str,
        environment: str
    ) -> Dict[str, Any]:
        """
        Extract and construct environment configuration settings.
        """
        path = Path(config_dir_or_file)
        env_data = None
        
        if path.is_dir():
            env_dir = path / "environments"
            if env_dir.exists() and env_dir.is_dir():
                env_file = env_dir / "env_settings.json"
                if not env_file.exists():
                    env_file = env_dir / f"{environment}.json"
                if env_file.exists():
                    with open(env_file, 'r', encoding='utf-8') as f:
                        env_data = json.load(f)
                else:
                    env_data = ConfigParser.load_merged_json(str(env_dir))
            else:
                env_file = path / "env_settings.json"
                if env_file.exists():
                    with open(env_file, 'r', encoding='utf-8') as f:
                        env_data = json.load(f)
                else:
                    env_data = ConfigParser.load_merged_json(config_dir_or_file)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            env_data = monolithic.get("environments", {})
            
        if not env_data:
            raise ValueError(f"Environment configurations not found in {config_dir_or_file}")
            
        if environment in env_data:
            return env_data[environment]
            
        if "catalog" in env_data:
            return env_data
            
        raise ValueError(f"Config for environment '{environment}' not found in env_settings")

    @staticmethod
    def get_ingestion_config(
        config_dir_or_file: str,
        source_name: str,
        environment: str
    ) -> IngestionConfig:
        """
        Extract and construct an IngestionConfig for a source from the configs.
        """
        env_config = ConfigParser.get_environment_config(config_dir_or_file, environment)
        path = Path(config_dir_or_file)
        source_data = None
        
        if path.is_dir():
            ingest_dir = path / "ingestion"
            if ingest_dir.exists() and ingest_dir.is_dir():
                src_file = ingest_dir / f"{source_name}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        source_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(str(ingest_dir))
                    source_data = merged.get(source_name)
            else:
                src_file = path / f"{source_name}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        source_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(config_dir_or_file)
                    source_data = merged.get(source_name)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            source_data = monolithic.get("sources", {}).get(source_name)
            
        if not source_data:
            raise ValueError(f"Ingestion configuration for source '{source_name}' not found in {config_dir_or_file}")
            
        if source_name in source_data and isinstance(source_data[source_name], dict):
            source_data = source_data[source_name]
            
        options = source_data.get("options", {})
        header_val = str(options.get("header", "true")).lower() == "true"
        infer_val = str(options.get("inferSchema", "true")).lower() == "true"
        
        mode_val = "append"
        merge_strat = source_data.get("merge_strategy", "append")
        if merge_strat == "upsert":
            mode_val = "merge"
        elif merge_strat == "overwrite":
            mode_val = "overwrite"
            
        return IngestionConfig(
            source_name=source_name,
            source_type=source_data.get("source_type", "csv"),
            source_path=source_data.get("file_path"),
            target_table=source_data.get("bronze_table"),
            target_catalog=env_config["catalog"],
            target_schema=env_config["bronze_schema"],
            file_format=source_data.get("file_format", "csv"),
            header=header_val,
            delimiter=options.get("delimiter", ","),
            encoding=options.get("encoding", "utf-8"),
            infer_schema=infer_val,
            mode=mode_val,
            business_key=source_data.get("business_key", []),
            merge_strategy=merge_strat
        )

    @staticmethod
    def get_dq_config(
        config_dir_or_file: str,
        source_name: str,
        environment: str
    ) -> DQConfig:
        """
        Extract and construct a DQConfig for a source from configs.
        """
        env_config = ConfigParser.get_environment_config(config_dir_or_file, environment)
        
        try:
            ing_config = ConfigParser.get_ingestion_config(config_dir_or_file, source_name, environment)
            target_tbl = ing_config.target_table
        except Exception:
            target_tbl = source_name
            
        path = Path(config_dir_or_file)
        rules_list = None
        
        if path.is_dir():
            dq_dir = path / "dq"
            if dq_dir.exists() and dq_dir.is_dir():
                src_file = dq_dir / f"{source_name}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        rules_list = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(str(dq_dir))
                    rules_list = merged.get(source_name)
            else:
                src_file = path / f"{source_name}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        rules_list = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(config_dir_or_file)
                    rules_list = merged.get(source_name)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            rules_list = monolithic.get("dq_rules", {}).get(source_name, [])
            
        if rules_list is None:
            rules_list = []
            
        if isinstance(rules_list, dict):
            rules_list = rules_list.get("rules", [])
            
        parsed_rules = []
        for rule_dict in rules_list:
            rule_args = dict(rule_dict)
            if "rule_type" not in rule_args and "validation_type" in rule_args:
                rule_args["rule_type"] = rule_args["validation_type"]
            parsed_rules.append(DQRule(**rule_args))
            
        quarantine_tbl = f"{target_tbl}_quarantine"
        
        return DQConfig(
            table_name=target_tbl,
            catalog=env_config["catalog"],
            schema=env_config["bronze_schema"],
            rules=parsed_rules,
            quarantine_table=quarantine_tbl,
            enable_scorecard=True
        )

    @staticmethod
    def get_silver_transformation_config(
        config_dir_or_file: str,
        target_table: str,
        environment: str
    ) -> Dict[str, Any]:
        """
        Extract and construct a Silver transformations config for a table from configs.
        """
        path = Path(config_dir_or_file)
        trans_data = None
        
        if path.is_dir():
            trans_dir = path / "transformations"
            if trans_dir.exists() and trans_dir.is_dir():
                src_file = trans_dir / f"{target_table}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        trans_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(str(trans_dir))
                    trans_data = merged.get(target_table)
            else:
                src_file = path / f"{target_table}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        trans_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(config_dir_or_file)
                    trans_data = merged.get(target_table)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            trans_data = monolithic.get("silver_transformations", {}).get(target_table)
            
        if not trans_data:
            raise ValueError(f"Silver transformations configuration for '{target_table}' not found in {config_dir_or_file}")
            
        if target_table in trans_data and isinstance(trans_data[target_table], dict):
            trans_data = trans_data[target_table]
            
        return trans_data

    @staticmethod
    def get_gold_aggregation_config(
        config_dir_or_file: str,
        target_table: str,
        environment: str
    ) -> Dict[str, Any]:
        """
        Extract and construct a Gold aggregation config for a table from configs.
        """
        path = Path(config_dir_or_file)
        agg_data = None
        
        if path.is_dir():
            agg_dir = path / "aggregations"
            if agg_dir.exists() and agg_dir.is_dir():
                src_file = agg_dir / f"{target_table}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        agg_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(str(agg_dir))
                    agg_data = merged.get(target_table)
            else:
                src_file = path / f"{target_table}.json"
                if src_file.exists():
                    with open(src_file, 'r', encoding='utf-8') as f:
                        agg_data = json.load(f)
                else:
                    merged = ConfigParser.load_merged_json(config_dir_or_file)
                    agg_data = merged.get(target_table)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            agg_data = monolithic.get("gold_aggregations", {}).get(target_table)
            
        if not agg_data:
            raise ValueError(f"Gold aggregation configuration for '{target_table}' not found in {config_dir_or_file}")
            
        if target_table in agg_data and isinstance(agg_data[target_table], dict):
            agg_data = agg_data[target_table]
            
        return agg_data

    @staticmethod
    def get_view_configs(
        config_dir_or_file: str,
        environment: str
    ) -> List[ViewConfig]:
        """
        Extract and construct a list of ViewConfigs from configs.
        """
        env_config = ConfigParser.get_environment_config(config_dir_or_file, environment)
        path = Path(config_dir_or_file)
        views_dict = None
        
        if path.is_dir():
            views_dir = path / "views"
            if views_dir.exists() and views_dir.is_dir():
                views_dict = ConfigParser.load_merged_json(str(views_dir))
            else:
                views_dict = ConfigParser.load_merged_json(config_dir_or_file)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                monolithic = json.load(f)
            views_dict = monolithic.get("views", {})
            
        if not views_dict:
            if isinstance(views_dict, dict) and any(k in ["definition_type", "view_type"] for k in views_dict.values()):
                pass
            else:
                views_dict = {}
                
        parsed_configs = []
        for view_name, view_info in views_dict.items():
            if not isinstance(view_info, dict):
                continue
            schema_type = view_info.get("schema_type", "silver")
            
            if schema_type == "bronze":
                schema_val = env_config["bronze_schema"]
            elif schema_type == "gold":
                schema_val = env_config["gold_schema"]
            else:
                schema_val = env_config["silver_schema"]
                
            parsed_configs.append(
                ViewConfig(
                    view_name=view_name,
                    catalog=env_config["catalog"],
                    schema=schema_val,
                    definition_type=view_info.get("definition_type", "table"),
                    view_type=view_info.get("view_type", "persistent"),
                    source_table=view_info.get("source_table"),
                    select_columns=view_info.get("select_columns"),
                    filter_condition=view_info.get("filter_condition"),
                    query=view_info.get("query")
                )
            )
            
        return parsed_configs

    @staticmethod
    def validate_ingestion_config(config: IngestionConfig) -> List[str]:
        """
        Validate ingestion configuration.
        """
        errors = []
        if not config.source_name:
            errors.append("source_name is required")
        if not config.source_type:
            errors.append("source_type is required")
        if config.source_type not in ['csv', 'excel', 'json', 'api', 'sql', 'parquet']:
            errors.append(f"Invalid source_type: {config.source_type}")
        if not config.source_path:
            errors.append("source_path is required")
        if not config.target_table:
            errors.append("target_table is required")
        if not config.target_catalog:
            errors.append("target_catalog is required")
        if not config.target_schema:
            errors.append("target_schema is required")
        if config.mode not in ['append', 'overwrite', 'merge']:
            errors.append(f"Invalid mode: {config.mode}")
        return errors

    @staticmethod
    def validate_dq_config(config: DQConfig) -> List[str]:
        """
        Validate data quality configuration.
        """
        errors = []
        if not config.table_name:
            errors.append("table_name is required")
        if not config.rules:
            errors.append("At least one rule is required")
            
        for idx, rule in enumerate(config.rules):
            if not rule.rule_name:
                errors.append(f"Rule {idx}: rule_name is required")
            if not rule.rule_type:
                errors.append(f"Rule {idx}: rule_type is required")
                
            valid_rule_types = ['null_check', 'duplicate_check', 'regex_check', 
                               'range_check', 'datatype_check', 'custom_sql', 'referential_integrity', 
                               'row_count_threshold', 'null_rate_threshold']
            if rule.rule_type not in valid_rule_types:
                errors.append(f"Rule {idx}: Invalid rule_type: {rule.rule_type}")
            if rule.action not in ['quarantine', 'fail', 'warn', 'flag', 'alert']:
                errors.append(f"Rule {idx}: Invalid action: {rule.action}")
        return errors

    @staticmethod
    def validate_view_config(config: ViewConfig) -> List[str]:
        """
        Validate view configuration.
        """
        errors = []
        if not config.view_name:
            errors.append("view_name is required")
        if not config.catalog:
            errors.append("catalog is required")
        if not config.schema:
            errors.append("schema is required")
        if config.definition_type not in ["table", "query"]:
            errors.append(f"Invalid definition_type: {config.definition_type}")
        if config.view_type not in ["persistent", "temp"]:
            errors.append(f"Invalid view_type: {config.view_type}")
        if config.definition_type == "table" and not config.source_table:
            errors.append("source_table is required when definition_type is 'table'")
        if config.definition_type == "query" and not config.query:
            errors.append("query is required when definition_type is 'query'")
        return errors


def load_ingestion_config(config_path: str, environment: str = "dev") -> IngestionConfig:
    """
    Load and validate ingestion configuration for a single config file layout.
    """
    # Try directory/file path parser
    try:
        # If config_path is a specific JSON file, get its stem name as source_name
        p = Path(config_path)
        source_name = p.stem
        config = ConfigParser.get_ingestion_config(str(p.parent), source_name, environment)
    except Exception:
        # Fallback to direct parsing
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        config = IngestionConfig(**config_dict)
        
    errors = ConfigParser.validate_ingestion_config(config)
    if errors:
        raise ValueError(f"Invalid ingestion config: {', '.join(errors)}")
    return config


def load_dq_config(config_path: str, environment: str = "dev") -> DQConfig:
    """
    Load and validate DQ configuration for a single config file layout.
    """
    try:
        p = Path(config_path)
        source_name = p.stem
        config = ConfigParser.get_dq_config(str(p.parent), source_name, environment)
    except Exception:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        rules = [DQRule(**r) for r in config_dict.get('rules', [])]
        config_dict['rules'] = rules
        config = DQConfig(**config_dict)
        
    errors = ConfigParser.validate_dq_config(config)
    if errors:
        raise ValueError(f"Invalid DQ config: {', '.join(errors)}")
    return config


def load_view_configs(config_path: str, environment: str) -> List[ViewConfig]:
    """
    Load and validate view configurations.
    """
    configs = ConfigParser.get_view_configs(config_path, environment)
    for config in configs:
        errors = ConfigParser.validate_view_config(config)
        if errors:
            raise ValueError(f"Invalid view config for '{config.view_name}': {', '.join(errors)}")
    return configs
