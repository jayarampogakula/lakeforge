"""LakeForge - Enterprise Data Engineering Framework"""

__version__ = "1.0.0"

# Metadata, observability, governance, and reporting are available without PySpark
from .metadata.config_parser import ConfigParser, load_ingestion_config, load_dq_config, load_view_configs
from .reporting.reporting import ReportGenerator, create_report_generator
from .observability.logger import LakeForgeLogger
from .governance.governance_engine import (
    GovernanceEngine,
    create_governance_engine,
    AccessControlConfig,
    PrincipalPrivilege
)

__all__ = [
    "ConfigParser",
    "load_ingestion_config",
    "load_dq_config",
    "load_view_configs",
    "ReportGenerator",
    "create_report_generator",
    "LakeForgeLogger",
    "GovernanceEngine",
    "create_governance_engine",
    "AccessControlConfig",
    "PrincipalPrivilege",
]

# Spark-dependent components
try:
    from .ingestion.files.csv_loader import CSVLoader, create_csv_loader
    from .ingestion.files.excel_loader import ExcelLoader, create_excel_loader
    from .ingestion.files.json_loader import JSONLoader, create_json_loader
    from .ingestion.files.xml_loader import XMLLoader, create_xml_loader
    from .ingestion.files.avro_loader import AvroLoader, create_avro_loader
    from .ingestion.files.orc_loader import ORCLoader, create_orc_loader
    from .ingestion.files.delta_loader import DeltaLoader, create_delta_loader
    from .ingestion.files.text_loader import TextLoader, create_text_loader
    from .ingestion.files.universal_loader import UniversalLoader, create_universal_loader
    from .ingestion.utilities.schema_detector import SchemaDetector, create_schema_detector
    from .bronze.bronze_writer import BronzeWriter, create_bronze_writer
    from .dq.dq_engine import DQEngine, create_dq_engine
    from .trust_engine.trust_engine import TrustEngine, create_trust_engine
    from .silver.scd_type2 import SCDType2Handler, create_scd_type2_handler
    from .silver.transformer import SilverTransformer
    from .silver.deduplication import Deduplicator
    from .silver.merge_engine import SilverMergeEngine
    from .gold.aggregations import GoldAggregator
    from .views.view_manager import ViewManager, create_view_manager

    __all__.extend([
        "CSVLoader", "create_csv_loader",
        "ExcelLoader", "create_excel_loader",
        "JSONLoader", "create_json_loader",
        "XMLLoader", "create_xml_loader",
        "AvroLoader", "create_avro_loader",
        "ORCLoader", "create_orc_loader",
        "DeltaLoader", "create_delta_loader",
        "TextLoader", "create_text_loader",
        "UniversalLoader", "create_universal_loader",
        "SchemaDetector", "create_schema_detector",
        "BronzeWriter", "create_bronze_writer",
        "DQEngine", "create_dq_engine",
        "TrustEngine", "create_trust_engine",
        "SCDType2Handler", "create_scd_type2_handler",
        "SilverTransformer",
        "Deduplicator",
        "SilverMergeEngine",
        "GoldAggregator",
        "ViewManager", "create_view_manager",
    ])
except ImportError:
    # PySpark/Delta not present in the runtime environment (e.g. lightweight CLI/runner)
    pass
