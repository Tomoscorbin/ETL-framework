"""
SQL Executor: thin helpers that build a query (via _sql.py) and execute it.

- Functions are intentionally small and explicit.
- They *do not* catch exceptions; readers decide how to turn failures into warnings.
"""

from __future__ import annotations
from typing import List
from pyspark.sql import Row, SparkSession

from ._sql import (
    sql_select_primary_key_for_table,
    sql_select_column_comments_for_table,
    sql_select_table_comment_for_table,
)


def select_primary_key_rows_for_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> List[Row]:
    """
    Execute the information_schema query that returns one row per PK column
    (or zero rows if no PK) for the given table.
    """
    query = sql_select_primary_key_for_table(catalog=catalog, schema=schema, table=table)
    return spark.sql(query).collect()


def select_column_comment_rows_for_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> List[Row]:
    """
    Execute the information_schema query that returns column_name + comment
    rows for the given table (zero rows if table is absent).
    """
    query = sql_select_column_comments_for_table(catalog=catalog, schema=schema, table=table)
    return spark.sql(query).collect()

def select_table_comment_rows_for_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> List[Row]:
    """
    Execute the information_schema query that returns the table-level comment
    (at most one row) for the given table.
    """
    query = sql_select_table_comment_for_table(catalog=catalog, schema=schema, table=table)
    return spark.sql(query).collect()