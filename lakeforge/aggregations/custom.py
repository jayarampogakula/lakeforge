"""
Custom PySpark Gold Layer Aggregations Library
Provides reusable user-defined aggregation functions for Gold analytical marts.
Users can define any custom PySpark logic here or in their own modules,
and call them directly from their Gold aggregation configs.
"""
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def compute_customer_sales_mart(
    source_dfs: Dict[str, DataFrame],
    spark: Optional[SparkSession] = None,
    min_order_count: int = 1,
    **kwargs
) -> DataFrame:
    """
    Custom Gold Mart Aggregation:
      - Joins Silver 'orders' and 'customers' DataFrames
      - Groups by customer dimensions and year/month
      - Computes revenue KPIs (total_revenue, order_count, avg_order_value, high_value_orders)
      - Filters out low volume records based on min_order_count
    
    Args:
        source_dfs: Dictionary of alias -> DataFrame (e.g. {'orders': df_orders, 'customers': df_cust})
        spark: Active SparkSession
        min_order_count: Minimum orders required to be included in the mart
        **kwargs: Extra parameters passed from config
    """
    orders_df = source_dfs.get("orders")
    customers_df = source_dfs.get("customers")

    if orders_df is None or customers_df is None:
        # Fallback to single df if passed
        primary_df = list(source_dfs.values())[0]
        return primary_df

    # Alias columns to prevent ambiguity
    o = orders_df.alias("o")
    c = customers_df.alias("c")

    # Join orders and customers
    joined = o.join(c, F.col("o.customer_id") == F.col("c.customer_id"), "inner")

    # Filter active transactions
    filtered = joined.filter(
        (F.col("o.transaction_amount") > 0) & F.col("c.customer_id").isNotNull()
    )

    # Group by customer and time dimensions
    mart_df = filtered.groupBy(
        F.col("c.customer_id"),
        F.col("c.customer_name"),
        F.coalesce(F.col("c.region"), F.lit("GLOBAL")).alias("region"),
        F.year(F.col("o.transaction_timestamp")).alias("order_year"),
        F.month(F.col("o.transaction_timestamp")).alias("order_month")
    ).agg(
        F.round(F.sum("o.transaction_amount"), 2).alias("total_revenue"),
        F.count("o.record_id").alias("total_orders"),
        F.round(F.avg("o.transaction_amount"), 2).alias("avg_order_value"),
        F.max("o.transaction_amount").alias("max_order_amount"),
        F.min("o.transaction_amount").alias("min_order_amount"),
        F.sum(
            F.when(F.col("o.is_high_value") == True, F.lit(1)).otherwise(F.lit(0))  # noqa: E712
        ).alias("high_value_order_count"),
        F.min("o.transaction_timestamp").alias("first_transaction_time"),
        F.max("o.transaction_timestamp").alias("last_transaction_time")
    )

    if min_order_count > 1:
        mart_df = mart_df.filter(F.col("total_orders") >= min_order_count)

    return mart_df.orderBy(F.col("total_revenue").desc(), F.col("customer_id").asc())


def compute_daily_revenue_kpis(
    df: DataFrame,
    **kwargs
) -> DataFrame:
    """
    Computes daily revenue KPI summary table:
      - Groups by transaction date
      - Computes daily revenue, order volume, unique customers
    """
    res = df.withColumn("transaction_date", F.to_date("transaction_timestamp"))
    return (
        res.groupBy("transaction_date")
        .agg(
            F.round(F.sum("transaction_amount"), 2).alias("daily_revenue"),
            F.count("record_id").alias("total_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("transaction_amount"), 2).alias("avg_transaction_amount")
        )
        .orderBy("transaction_date")
    )
