"""
LakeForge Ingestion Module
Data loaders for various file formats
"""

from lakeforge.ingestion.csv_loader import CSVLoader, create_csv_loader
from lakeforge.ingestion.excel_loader import ExcelLoader, create_excel_loader
from lakeforge.ingestion.json_loader import JSONLoader, create_json_loader
from lakeforge.ingestion.schema_detector import SchemaDetector, create_schema_detector

__all__ = [
    'CSVLoader',
    'create_csv_loader',
    'ExcelLoader',
    'create_excel_loader',
    'JSONLoader',
    'create_json_loader',
    'SchemaDetector',
    'create_schema_detector'
]
