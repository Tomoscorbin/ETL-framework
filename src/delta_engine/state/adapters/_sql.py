"""Information Schema SQL builders.

These helpers generate parameterized SQL strings for a single table:
- primary key rows
- column comments
- table-level comment

Conventions:
- Use `catalog_name`, `schema_name`, `table_name` param names.
- Quote **identifiers** (catalog/schema/table objects) with `quote_qualified_name`.
- Escape **literals** in WHERE clauses with `escape_sql_literal`.
"""

from __future__ import annotations

from src.delta_engine.identifiers import quote_qualified_name
from src.delta_engine.utils import escape_sql_literal


def sql_select_primary_key_for_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    """
    Return rows for the PRIMARY KEY of a single table within `catalog_name`.
    One output row per PK column (if any). Zero rows if no PK or table not present.
    """
    # Object paths (identifiers)
    tc_table = _info_schema_table(catalog_name, "table_constraints")
    kcu_table = _info_schema_table(catalog_name, "key_column_usage")

    # String literals for WHERE
    catalog_lit = escape_sql_literal(catalog_name)
    schema_lit = escape_sql_literal(schema_name)
    table_lit = escape_sql_literal(table_name)

    # LEFT JOIN: if there is no PK, the WHERE on `tc` eliminates rows -> empty result set.
    return f"""
    SELECT
      tc.constraint_name    AS constraint_name,
      kcu.column_name       AS column_name,
      kcu.ordinal_position  AS ordinal_position
    FROM {tc_table} AS tc
    LEFT JOIN {kcu_table} AS kcu
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
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    """
    Return (column_name, comment) for a single table within `catalog_name`.
    Zero rows if the table does not exist or has no columns visible in metadata.
    """
    columns_table = _info_schema_table(catalog_name, "columns")

    catalog_lit = escape_sql_literal(catalog_name)
    schema_lit = escape_sql_literal(schema_name)
    table_lit = escape_sql_literal(table_name)

    return f"""
    SELECT
      column_name,
      comment
    FROM {columns_table}
    WHERE table_catalog = '{catalog_lit}'
      AND table_schema  = '{schema_lit}'
      AND table_name    = '{table_lit}'
    """


def sql_select_table_comment_for_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    """
    Return at most one row with the `comment` for a specific table within `catalog_name`.
    Zero rows if the table does not exist or is not visible in metadata.
    """
    tables_table = _info_schema_table(catalog_name, "tables")

    catalog_lit = escape_sql_literal(catalog_name)
    schema_lit = escape_sql_literal(schema_name)
    table_lit = escape_sql_literal(table_name)

    return f"""
    SELECT
      comment
    FROM {tables_table}
    WHERE table_catalog = '{catalog_lit}'
      AND table_schema  = '{schema_lit}'
      AND table_name    = '{table_lit}'
    """


# ---------- helpers ----------

def _info_schema_table(catalog_name: str, info_schema_table: str) -> str:
    """
    Build a fully qualified information_schema object path with proper identifier quoting.
    Example: `` `catalog`.`information_schema`.`table_constraints` ``.
    """
    return quote_qualified_name(catalog_name, "information_schema", info_schema_table)
