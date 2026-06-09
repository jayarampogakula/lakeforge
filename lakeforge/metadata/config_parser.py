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


class ConfigParser:
    """Parser for LakeForge JSON configurations."""
    
    @staticmethod
    def parse_ingestion_config(config_path: str) -> IngestionConfig:
        """
        Parse ingestion configuration from JSON file.
        
        Args:
            config_path: Path to JSON config file
            
        Returns:
            IngestionConfig object
        """
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        
        return IngestionConfig(**config_dict)
    
    @staticmethod
    def parse_dq_config(config_path: str) -> DQConfig:
        """
        Parse data quality configuration from JSON file.
        
        Args:
            config_path: Path to JSON config file
            
        Returns:
            DQConfig object
        """
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        
        # Parse rules
        rules = []
        for rule_dict in config_dict.get('rules', []):
            rules.append(DQRule(**rule_dict))
        
        config_dict['rules'] = rules
        return DQConfig(**config_dict)
    
    @staticmethod
    def parse_json(config_path: str) -> Dict[str, Any]:
        """
        Parse generic JSON configuration file.
        
        Args:
            config_path: Path to JSON config file
            
        Returns:
            Dictionary containing configuration
        """
        with open(config_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def parse_monolithic_pipeline_config(config_path: str) -> Dict[str, Any]:
        """
        Parse the monolithic pipeline_config.json configuration.
        """
        with open(config_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_ingestion_config(
        monolithic_config: Dict[str, Any], 
        source_name: str, 
        environment: str
    ) -> IngestionConfig:
        """
        Extract and construct an IngestionConfig for a source from monolithic config.
        """
        env_config = monolithic_config["environments"][environment]
        source_config = monolithic_config["sources"][source_name]
        
        # Options conversion
        options = source_config.get("options", {})
        header_val = options.get("header", "true").lower() == "true"
        infer_val = options.get("inferSchema", "true").lower() == "true"
        
        mode_val = "append"
        merge_strat = source_config.get("merge_strategy", "append")
        if merge_strat == "upsert":
            mode_val = "merge"
        elif merge_strat == "overwrite":
            mode_val = "overwrite"
            
        return IngestionConfig(
            source_name=source_name,
            source_type=source_config.get("source_type", "csv"),
            source_path=source_config.get("file_path"),
            target_table=source_config.get("bronze_table"),
            target_catalog=env_config["catalog"],
            target_schema=env_config["bronze_schema"],
            file_format=source_config.get("file_format", "csv"),
            header=header_val,
            delimiter=options.get("delimiter", ","),
            encoding=options.get("encoding", "utf-8"),
            infer_schema=infer_val,
            mode=mode_val
        )

    @staticmethod
    def get_dq_config(
        monolithic_config: Dict[str, Any],
        source_name: str,
        environment: str
    ) -> DQConfig:
        """
        Extract and construct a DQConfig for a source from monolithic config.
        """
        env_config = monolithic_config["environments"][environment]
        source_config = monolithic_config["sources"][source_name]
        rules_list = monolithic_config.get("dq_rules", {}).get(source_name, [])
        
        parsed_rules = []
        for rule_dict in rules_list:
            # Map validation_type to rule_type if needed
            rule_args = dict(rule_dict)
            if "rule_type" not in rule_args and "validation_type" in rule_args:
                rule_args["rule_type"] = rule_args["validation_type"]
            parsed_rules.append(DQRule(**rule_args))
            
        target_tbl = source_config.get("bronze_table")
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
    def validate_ingestion_config(config: IngestionConfig) -> List[str]:
        """
        Validate ingestion configuration.
        
        Args:
            config: IngestionConfig to validate
            
        Returns:
            List of validation errors (empty if valid)
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
        
        Args:
            config: DQConfig to validate
            
        Returns:
            List of validation errors (empty if valid)
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
                               'range_check', 'datatype_check', 'custom_sql', 'referential_integrity', 'row_count_threshold', 'null_rate_threshold']
            if rule.rule_type not in valid_rule_types:
                errors.append(f"Rule {idx}: Invalid rule_type: {rule.rule_type}")
            
            if rule.action not in ['quarantine', 'fail', 'warn', 'flag', 'alert']:
                errors.append(f"Rule {idx}: Invalid action: {rule.action}")
        
        return errors


def load_ingestion_config(config_path: str) -> IngestionConfig:
    """
    Load and validate ingestion configuration.
    
    Args:
        config_path: Path to JSON config file
        
    Returns:
        Validated IngestionConfig object
        
    Raises:
        ValueError: If configuration is invalid
    """
    config = ConfigParser.parse_ingestion_config(config_path)
    errors = ConfigParser.validate_ingestion_config(config)
    
    if errors:
        raise ValueError(f"Invalid ingestion config: {', '.join(errors)}")
    
    return config


def load_dq_config(config_path: str) -> DQConfig:
    """
    Load and validate DQ configuration.
    
    Args:
        config_path: Path to JSON config file
        
    Returns:
        Validated DQConfig object
        
    Raises:
        ValueError: If configuration is invalid
    """
    config = ConfigParser.parse_dq_config(config_path)
    errors = ConfigParser.validate_dq_config(config)
    
    if errors:
        raise ValueError(f"Invalid DQ config: {', '.join(errors)}")
    
    return config
