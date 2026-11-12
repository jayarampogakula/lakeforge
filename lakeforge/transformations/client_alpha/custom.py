"""
Client Alpha Custom PySpark Transformations
Tenant-specific column and DataFrame transformations for client_alpha.
"""
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


# ==============================================================================
# 1. CLIENT ALPHA COLUMN-LEVEL TRANSFORMATION FUNCTIONS
# ==============================================================================

def clean_client_alpha_sales_id(col: Column) -> Column:
    """Format and standardize client_alpha sales IDs: trim, uppercase, prefix."""
    trimmed = F.trim(col)
    return F.when(
        trimmed.startswith("ALPHA-"),
        trimmed
    ).otherwise(
        F.concat(F.lit("ALPHA-"), F.upper(trimmed))
    )


def mask_client_alpha_pii(col: Column) -> Column:
    """Custom PII masking for client_alpha sensitive identifiers."""
    return F.concat(
        F.substring(col, 1, 2),
        F.lit("***-MASKED-***"),
        F.substring(col, -2, 2)
    )


def standardize_client_alpha_currency(amount_col: Column, tax_rate: float = 0.08) -> Column:
    """Calculate client_alpha gross transaction amount including tax."""
    return F.round(amount_col * (1.0 + tax_rate), 2)


# ==============================================================================
# 2. CLIENT ALPHA DATAFRAME-LEVEL TRANSFORMATION FUNCTIONS
# ==============================================================================

def transform_client_alpha_sales(df: DataFrame, min_amount: float = 0.0, **kwargs) -> DataFrame:
    """
    Custom transformation pipeline specifically for client_alpha sales records:
      - Standardizes sale_id with client_alpha prefix
      - Upper-cases region and coalesces NULLs to 'ALPHA_HQ'
      - Flags high-value enterprise sales
      - Calculates gross amount with tax
    """
    res = df

    if "sale_id" in res.columns:
        res = res.withColumn("sale_id", clean_client_alpha_sales_id(F.col("sale_id")))

    if "region" in res.columns:
        res = res.withColumn("region", F.upper(F.coalesce(F.col("region"), F.lit("ALPHA_HQ"))))

    if "amount" in res.columns:
        res = res.withColumn("gross_amount", standardize_client_alpha_currency(F.col("amount")))
        res = res.withColumn(
            "is_enterprise_sale",
            F.when(F.col("amount") >= 5000.0, F.lit(True)).otherwise(F.lit(False))
        )
        if min_amount > 0:
            res = res.filter(F.col("amount") >= min_amount)

    return res


def clean_customer_dataframe(df: DataFrame, **kwargs) -> DataFrame:
    """
    Client-specific customer cleansing for client_alpha:
      - Standardizes customer names
      - Cleans customer codes
    """
    res = df
    if "customer_name" in res.columns:
        res = res.withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    if "customer_id" in res.columns:
        res = res.withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
    return res
