"""LakeForge - Enterprise Data Engineering Framework"""

__version__ = "1.0.0"

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
from .metadata.config_parser import ConfigParser, load_ingestion_config, load_dq_config
from .reporting.reporting import ReportGenerator, create_report_generator
from .observability.logger import LakeForgeLogger
from .views.view_manager import ViewManager, create_view_manager

__all__ = [
    "CSVLoader", "ExcelLoader", "JSONLoader", "SchemaDetector", "BronzeWriter", "DQEngine", "TrustEngine",
    "SCDType2Handler", "SilverTransformer", "Deduplicator", "SilverMergeEngine",
    "GoldAggregator", "ConfigParser", "ReportGenerator",
    "LakeForgeLogger", "ViewManager", "create_view_manager",
    "XMLLoader", "AvroLoader", "ORCLoader", "DeltaLoader", "TextLoader",
    "UniversalLoader", "create_universal_loader", "create_xml_loader", "create_avro_loader",
    "create_orc_loader", "create_delta_loader", "create_text_loader"
]
