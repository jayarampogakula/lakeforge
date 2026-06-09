"""LakeForge - Enterprise Data Engineering Framework"""

__version__ = "1.0.0"

from .ingestion.files.csv_loader import CSVLoader, create_csv_loader
from .ingestion.files.excel_loader import ExcelLoader, create_excel_loader
from .ingestion.files.json_loader import JSONLoader, create_json_loader
from .ingestion.utilities.schema_detector import SchemaDetector, create_schema_detector
from .bronze.bronze_writer import BronzeWriter, create_bronze_writer
from .dq.dq_engine import DQEngine, create_dq_engine
from .trust_engine.trust_engine import TrustEngine, create_trust_engine
from .silver.scd_type2 import SCDType2Handler, create_scd_type2_handler
from .silver.transformer import SilverTransformer
from .silver.deduplication import Deduplicator
from .silver.merge_engine import SilverMergeEngine
from .gold.aggregations import GoldAggregator
from .metadata.config_parser import ConfigParser, load_ingestion_config, load_dq_config
from .metadata.schema_drift_detector import SchemaDriftDetector, create_schema_drift_detector
from .reporting.reporting import ReportGenerator, create_report_generator
from .observability.logger import LakeForgeLogger

__all__ = [
    "CSVLoader", "ExcelLoader", "JSONLoader", "SchemaDetector", "BronzeWriter", "DQEngine", "TrustEngine",
    "SCDType2Handler", "SilverTransformer", "Deduplicator", "SilverMergeEngine",
    "GoldAggregator", "ConfigParser", "SchemaDriftDetector", "ReportGenerator",
    "LakeForgeLogger"
]
