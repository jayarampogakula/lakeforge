"""
LakeForge Automated Table Maintenance Module.
Provides optimization, Z-Ordering, VACUUM retention cleanup, and statistics computation for Delta tables.
"""
from lakeforge.maintenance.table_maintenance import (
    TableMaintenanceManager,
    MaintenanceResult,
    create_maintenance_manager
)

__all__ = [
    "TableMaintenanceManager",
    "MaintenanceResult",
    "create_maintenance_manager"
]
