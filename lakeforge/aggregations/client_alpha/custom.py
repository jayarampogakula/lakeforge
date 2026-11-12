"""
Client Alpha Custom PySpark Gold Layer Aggregations
Tenant-specific aggregation functions for client_alpha Gold marts.
"""
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def compute_client_alpha_sales_summary(
    source_dfs: Dict[str, DataFrame],
    spark: Optional[SparkSession] = None,
    min_volume: int = 1,
    **kwargs
) -> DataFrame:
    """
    Client Alpha Custom Gold Sales Summary:
      - Aggregates client_alpha sales by region and customer
      - Computes net sales, total transactions, average transaction value
      - Computes enterprise deals volume
    """
    # Look for sales DataFrame by key or take primary
    sales_df = source_dfs.get("sales") or source_dfs.get("orders")
    if sales_df is None and source_dfs:
        sales_df = list(source_dfs.values())[0]

    if sales_df is None:
        raise ValueError("No sales DataFrame provided in source_dfs for client_alpha sales summary")

    amount_col = "amount" if "amount" in sales_df.columns else "transaction_amount"
    region_col = "region" if "region" in sales_df.columns else "region_code"
    cust_col = "customer_id"

    mart_df = (
        sales_df.groupBy(cust_col, region_col)
        .agg(
            F.round(F.sum(amount_col), 2).alias("total_sales_amount"),
            F.count("*").alias("transaction_count"),
            F.round(F.avg(amount_col), 2).alias("avg_sale_amount"),
            F.max(amount_col).alias("max_single_sale"),
            F.sum(
                F.when(F.col(amount_col) >= 5000.0, F.lit(1)).otherwise(F.lit(0))
            ).alias("enterprise_deal_count")
        )
    )

    if min_volume > 1:
        mart_df = mart_df.filter(F.col("transaction_count") >= min_volume)

    return mart_df.orderBy(F.col("total_sales_amount").desc())


def compute_client_alpha_regional_mart(
    df: DataFrame,
    **kwargs
) -> DataFrame:
    """
    Client Alpha Regional Performance Mart:
      - Groups by region
      - Aggregates gross revenue, client count, order volume
    """
    amount_col = "amount" if "amount" in df.columns else "transaction_amount"
    region_col = "region" if "region" in df.columns else "region_code"

    return (
        df.groupBy(region_col)
        .agg(
            F.round(F.sum(amount_col), 2).alias("regional_revenue"),
            F.countDistinct("customer_id").alias("active_clients"),
            F.count("*").alias("total_transactions")
        )
        .orderBy(F.col("regional_revenue").desc())
    )
