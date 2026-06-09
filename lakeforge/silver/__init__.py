"""
LakeForge Silver Layer Module
Data transformation and SCD logic
"""

from lakeforge.silver.scd_type2 import SCDType2Handler, create_scd_type2_handler
from lakeforge.silver.transformer import SilverTransformer
from lakeforge.silver.deduplication import Deduplicator
from lakeforge.silver.merge_engine import SilverMergeEngine

__all__ = [
    'SCDType2Handler',
    'create_scd_type2_handler',
    'SilverTransformer',
    'Deduplicator',
    'SilverMergeEngine'
]
