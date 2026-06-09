"""LakeForge - Enterprise Data Engineering Framework"""

__version__ = "1.0.0"

from .ingestion.csv_loader import CSVLoader, create_csv_loader
from .ingestion.excel_loader import ExcelLoader, create_excel_loader
from .bronze.bronze_writer import BronzeWriter, create_bronze_writer
from .dq.dq_engine import DQEngine, create_dq_engine
from .trust_engine.trust_engine import TrustEngine, create_trust_engine
from .silver.scd_type2 import SCDType2Handler, create_scd_type2_handler
from .metadata.config_parser import ConfigParser, load_ingestion_config, load_dq_config
from .metadata.schema_drift_detector import SchemaDriftDetector, create_schema_drift_detector
from .reporting.reporting import ReportGenerator, create_report_generator
from .observability.logger import LakeForgeLogger

__all__ = [
    "CSVLoader", "ExcelLoader", "BronzeWriter", "DQEngine", "TrustEngine",
    "SCDType2Handler", "ConfigParser", "SchemaDriftDetector", "ReportGenerator",
    "LakeForgeLogger"
]
