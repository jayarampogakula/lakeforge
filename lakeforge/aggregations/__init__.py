"""
LakeForge Aggregations Library
Provides generic and custom PySpark aggregation functions for Gold layer analytical marts.
"""
from lakeforge.aggregations.custom import (
    compute_customer_sales_mart,
    compute_daily_revenue_kpis
)

__all__ = [
    "compute_customer_sales_mart",
    "compute_daily_revenue_kpis"
]
