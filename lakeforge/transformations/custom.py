"""
Custom PySpark Transformations Library
Example and reusable user-defined Python / PySpark functions for column and DataFrame transformations.
Users can add their own functions here or in custom modules and call them from config JSON.
"""
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


# ==============================================================================
# 1. COLUMN-LEVEL TRANSFORMATION FUNCTIONS
#    Takes a Column (and optional kwargs) and returns a transformed Column
# ==============================================================================

def clean_phone_number(col: Column) -> Column:
    """Standardize phone numbers by stripping non-numeric characters and padding."""
    digits = F.regexp_replace(col, r"[^\d]", "")
    return F.when(F.length(digits) == 10, F.concat(F.lit("+1-"), digits)).otherwise(digits)


def mask_email_column(col: Column) -> Column:
    """Mask email addresses for privacy: 'john.doe@example.com' -> 'j***e@example.com'."""
    user = F.split(col, "@").getItem(0)
    domain = F.split(col, "@").getItem(1)
    masked_user = F.concat(
        F.substring(user, 1, 1),
        F.lit("***"),
        F.substring(user, -1, 1)
    )
    return F.when(col.contains("@"), F.concat(masked_user, F.lit("@"), domain)).otherwise(col)


def categorize_transaction_value(amount_col: Column, threshold: float = 1000.0) -> Column:
    """Categorize transactions into 'HIGH_VALUE', 'MEDIUM_VALUE', or 'STANDARD'."""
    return (
        F.when(amount_col >= threshold, F.lit("HIGH_VALUE"))
        .when(amount_col >= (threshold / 2), F.lit("MEDIUM_VALUE"))
        .otherwise(F.lit("STANDARD"))
    )


# ==============================================================================
# 2. DATAFRAME-LEVEL TRANSFORMATION FUNCTIONS
#    Takes a DataFrame (and optional kwargs) and returns a transformed DataFrame
# ==============================================================================

def clean_customer_dataframe(df: DataFrame, min_spend: float = 0.0, **kwargs) -> DataFrame:
    """
    Example custom DataFrame transformation:
      - Trims string columns
      - Standardizes email and phone
      - Derives high-value customer flag
      - Filters invalid records
    """
    res = df
    if "customer_name" in res.columns:
        res = res.withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))

    if "contact_email" in res.columns:
        res = res.withColumn("contact_email", F.lower(F.trim(F.col("contact_email"))))

    if "phone" in res.columns:
        res = res.withColumn("phone", clean_phone_number(F.col("phone")))

    if "transaction_amount" in res.columns:
        res = res.withColumn(
            "is_high_value",
            F.when(F.col("transaction_amount") >= 1000.0, F.lit(True)).otherwise(F.lit(False))
        )
        if min_spend > 0:
            res = res.filter(F.col("transaction_amount") >= min_spend)

    return res


def enrich_sales_dataframe(df: DataFrame, default_region: str = "GLOBAL", **kwargs) -> DataFrame:
    """
    Example custom DataFrame enrichment:
      - Fills missing region with default_region
      - Formats currency amounts
      - Extracts transaction year and month
    """
    res = df
    if "region" in res.columns:
        res = res.withColumn("region", F.upper(F.coalesce(F.col("region"), F.lit(default_region))))

    if "transaction_timestamp" in res.columns:
        res = res.withColumn("order_year", F.year(F.col("transaction_timestamp")))
        res = res.withColumn("order_month", F.month(F.col("transaction_timestamp")))

    return res
