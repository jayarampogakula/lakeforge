"""
LakeForge - Unified Data Engineering Framework
Phase 1: Core Ingestion, Bronze, DQ, and SCD Type 2
"""

__version__ = "0.1.0"
__author__ = "LakeForge Team"

# Import key components for easy access
from lakeforge.observability.logger import get_logger
from lakeforge.metadata.config_parser import load_ingestion_config, load_dq_config

__all__ = [
    'get_logger',
    'load_ingestion_config',
    'load_dq_config'
]
