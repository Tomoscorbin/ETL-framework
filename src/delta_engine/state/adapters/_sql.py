from __future__ import annotations

from src.delta_engine.utils import escape_sql_literal

def sql_select_primary_key_for_table(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    Return rows for the primary key of a **single** table within `catalog`.
    One output row per PK column (if any). Zero rows if no PK or table not present.
    """
    catalog_lit = escape_sql_literal(catalog)
    schema_lit = escape_sql_literal(schema)
    table_lit = escape_sql_literal(table)

    # Simple LEFT JOIN: if there is no PK, the WHERE on tc removes all rows => empty result set.
    return f"""
    SELECT
      tc.constraint_name    AS constraint_name,
      kcu.column_name       AS column_name,
      kcu.ordinal_position  AS ordinal_position
    FROM {catalog}.information_schema.table_constraints AS tc
    LEFT JOIN {catalog}.information_schema.key_column_usage AS kcu
      ON kcu.table_catalog   = '{catalog_lit}'
     AND kcu.table_schema    = '{schema_lit}'
     AND kcu.table_name      = '{table_lit}'
     AND kcu.constraint_name = tc.constraint_name
    WHERE tc.table_catalog   = '{catalog_lit}'
      AND tc.table_schema    = '{schema_lit}'
      AND tc.table_name      = '{table_lit}'
      AND tc.constraint_type = 'PRIMARY KEY'
    """

def sql_select_column_comments_for_table(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    Return column_name and comment for a **single** table within `catalog`.
    Zero rows if the table does not exist or has no columns visible in metadata.
    """
    catalog_lit = escape_sql_literal(catalog)
    schema_lit = escape_sql_literal(schema)
    table_lit = escape_sql_literal(table)

    return f"""
    SELECT
      column_name,
      comment
    FROM {catalog}.information_schema.columns
    WHERE table_catalog = '{catalog_lit}'
      AND table_schema  = '{schema_lit}'
      AND table_name    = '{table_lit}'
    """

def sql_select_table_comment_for_table(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    Return a single row (at most) with the `comment` for a specific table within `catalog`.
    Zero rows if the table does not exist or is not visible in metadata.
    """
    catalog_lit = escape_sql_literal(catalog)
    schema_lit = escape_sql_literal(schema)
    table_lit = escape_sql_literal(table)

    return f"""
    SELECT
      comment
    FROM {catalog}.information_schema.tables
    WHERE table_catalog = '{catalog_lit}'
      AND table_schema  = '{schema_lit}'
      AND table_name    = '{table_lit}'
    """