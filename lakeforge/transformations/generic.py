"""
Generic PySpark DataFrame and Column Transformations
Provides modular, reusable transformation functions for PySpark pipelines.
"""
from typing import List, Dict, Any, Optional, Callable, Union
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def filter_rows(df: DataFrame, condition: Union[str, Column, Callable[[DataFrame], Column]]) -> DataFrame:
    """Filter rows matching a SQL condition, PySpark Column expression, or callable predicate."""
    if callable(condition):
        return df.filter(condition(df))
    elif isinstance(condition, str):
        return df.filter(F.expr(condition))
    else:
        return df.filter(condition)


def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Drop specified columns from the DataFrame if they exist."""
    existing_cols = [c for c in columns if c in df.columns]
    return df.drop(*existing_cols) if existing_cols else df


def drop_duplicates(
    df: DataFrame,
    keys: List[str],
    strategy: str = "keep_latest",
    order_by: str = "_ingestion_timestamp",
    order_desc: bool = True
) -> DataFrame:
    """
    Deduplicate DataFrame records on composite key columns.
    
    Strategies:
      - 'keep_latest': Keeps row with highest/lowest order_by value
      - 'simple': Standard Spark dropDuplicates on keys
    """
    valid_keys = [k for k in keys if k in df.columns]
    if not valid_keys:
        return df

    if strategy == "simple" or order_by not in df.columns:
        return df.dropDuplicates(valid_keys)

    order_expr = F.col(order_by).desc() if order_desc else F.col(order_by).asc()
    window_spec = Window.partitionBy(*valid_keys).orderBy(order_expr)
    return (
        df.withColumn("_lakeforge_row_num", F.row_number().over(window_spec))
        .filter(F.col("_lakeforge_row_num") == 1)
        .drop("_lakeforge_row_num")
    )


def join_tables(
    left_df: DataFrame,
    right_df: DataFrame,
    join_keys: Union[str, List[str]],
    join_type: str = "inner"
) -> DataFrame:
    """Join two DataFrames on key columns with specified join type."""
    return left_df.join(right_df, on=join_keys, how=join_type)


def rename_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Rename columns according to an old_name -> new_name dictionary mapping."""
    res = df
    for old_col, new_col in mapping.items():
        if old_col in res.columns:
            res = res.withColumnRenamed(old_col, new_col)
    return res


def cast_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Cast specified columns to target data types."""
    res = df
    for col_name, target_type in mapping.items():
        if col_name in res.columns:
            res = res.withColumn(col_name, F.col(col_name).cast(target_type))
    return res


def standardize_column(df: DataFrame, column: str, operation: str) -> DataFrame:
    """
    Standardize string column values.
    Operations: 'trim', 'lowercase', 'uppercase', 'initcap', 'remove_non_numeric', 'remove_spaces'
    """
    if column not in df.columns:
        return df

    op = operation.lower().strip()
    c = F.col(column)
    if op == "trim":
        return df.withColumn(column, F.trim(c))
    elif op in ["lowercase", "lower"]:
        return df.withColumn(column, F.lower(F.trim(c)))
    elif op in ["uppercase", "upper"]:
        return df.withColumn(column, F.upper(F.trim(c)))
    elif op in ["initcap", "capitalize"]:
        return df.withColumn(column, F.initcap(F.trim(c)))
    elif op == "remove_non_numeric":
        return df.withColumn(column, F.regexp_replace(c, r"[^0-9]", ""))
    elif op == "remove_spaces":
        return df.withColumn(column, F.regexp_replace(c, r"\s+", ""))
    return df


def add_derived_column(
    df: DataFrame,
    name: str,
    expression: Union[str, Column, Callable[[DataFrame], Column]]
) -> DataFrame:
    """Add or replace a derived column using a SQL expression, Column object, or callable."""
    if callable(expression):
        return df.withColumn(name, expression(df))
    elif isinstance(expression, str):
        return df.withColumn(name, F.expr(expression))
    else:
        return df.withColumn(name, expression)


def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Project and reorder DataFrame columns to the specified list."""
    existing = [c for c in columns if c in df.columns]
    return df.select(*existing) if existing else df


def select_and_cast(df: DataFrame, columns_and_types: Dict[str, str]) -> DataFrame:
    """Project and cast columns in a single operation."""
    select_exprs = [
        F.col(c).cast(t).alias(c)
        for c, t in columns_and_types.items()
        if c in df.columns
    ]
    return df.select(*select_exprs) if select_exprs else df


def enforce_schema(df: DataFrame, schema_def: Any) -> DataFrame:
    """
    Enforce explicit schema (DDL string or dict of col -> type) on DataFrame.
    """
    if not schema_def:
        return df

    if isinstance(schema_def, dict):
        return select_and_cast(df, schema_def)

    if isinstance(schema_def, str):
        try:
            spark_sess = getattr(df, "sparkSession", None)
            if spark_sess:
                parsed = spark_sess.createDataFrame([], schema=schema_def).schema
                select_exprs = [
                    F.col(f.name).cast(f.dataType).alias(f.name)
                    for f in parsed.fields if f.name in df.columns
                ]
                if select_exprs:
                    return df.select(*select_exprs)
        except Exception:
            pass

    return df


def apply_column_function(
    df: DataFrame,
    column: str,
    func: Callable[[Column], Column],
    target_column: Optional[str] = None,
    **kwargs
) -> DataFrame:
    """
    Apply a Python / PySpark function directly to a column.
    
    Args:
        df: Input DataFrame
        column: Source column name
        func: Python function taking a Column and returning a Column
        target_column: Destination column name (defaults to source column)
    """
    if column not in df.columns:
        return df
    dest_col = target_column or column
    return df.withColumn(dest_col, func(F.col(column), **kwargs))


def apply_dataframe_function(
    df: DataFrame,
    func: Callable[..., DataFrame],
    **kwargs
) -> DataFrame:
    """
    Apply a custom Python / PySpark DataFrame transformation function.
    
    Args:
        df: Input DataFrame
        func: Python function taking DataFrame and returning DataFrame
        **kwargs: Optional parameters passed into func
    """
    return func(df, **kwargs)
