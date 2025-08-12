"""
SQL string builders for Delta/Unity Catalog operations.

All functions return fully-formed SQL strings (or None for no-ops) and assume
the caller passes a fully-qualified table identifier string that will be
backtick-escaped via `quote_qualified_name_from_full`.

Design guarantees
- Deterministic, side-effect free string generation.
- Proper identifier quoting and SQL literal escaping.
- No business rules: higher layers (planner/validator) decide policy.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pyspark.sql.types as T

from src.delta_engine.types import ThreePartTableName
from src.delta_engine.utils import (
    escape_sql_literal,
    format_tblproperties,
    quote_ident,
    quote_qualified_name_from_full,
)


def sql_set_table_properties(qualified_table_name: str, props: Mapping[str, str]) -> str | None:
    """ALTER TABLE ... SET TBLPROPERTIES (...). Returns None if no props."""
    if not props:
        return None
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(qualified_table_name)} "
        f"SET TBLPROPERTIES ({format_tblproperties(props)})"
    )


def sql_add_column(
    qualified_table_name: str,
    column_name: str,
    dtype: T.DataType,
    comment: str = "",
) -> str:
    """ALTER TABLE ... ADD COLUMNS (`col` type [COMMENT '...'])."""
    full = quote_qualified_name_from_full(qualified_table_name)
    column = quote_ident(column_name)
    dt = dtype.simpleString()
    comment_sql = f" COMMENT '{escape_sql_literal(comment)}'" if comment else ""
    return f"ALTER TABLE {full} ADD COLUMNS ({column} {dt}{comment_sql})"


def sql_drop_columns(qualified_table_name: str, column_names: Iterable[str]) -> str | None:
    """ALTER TABLE ... DROP COLUMNS (...). Returns None if no names."""
    columns = [quote_ident(n) for n in column_names]
    if not columns:
        return None
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(qualified_table_name)}"
        f" DROP COLUMNS ({', '.join(columns)})"
    )


def sql_set_column_nullability(
    qualified_table_name: str, column_name: str, make_nullable: bool
) -> str:
    """ALTER TABLE ... ALTER COLUMN `col` DROP/SET NOT NULL."""
    op = "DROP NOT NULL" if make_nullable else "SET NOT NULL"
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(qualified_table_name)} "
        f"ALTER COLUMN {quote_ident(column_name)} {op}"
    )


def sql_set_column_comment(qualified_table_name: str, column_name: str, comment: str) -> str:
    """COMMENT ON COLUMN ..."""
    return (
        f"COMMENT ON COLUMN {quote_qualified_name_from_full(qualified_table_name)}."
        f"{quote_ident(column_name)} IS '{escape_sql_literal(comment or '')}'"
    )


def sql_set_table_comment(qualified_table_name: str, comment: str) -> str:
    """COMMENT ON TABLE ..."""
    return (
        f"COMMENT ON TABLE {quote_qualified_name_from_full(qualified_table_name)} "
        f"IS '{escape_sql_literal(comment or '')}'"
    )


def sql_add_primary_key(
    qualified_table_name: str, constraint_name: str, column_names: Iterable[str]
) -> str:
    """ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)."""
    cols = [quote_ident(c) for c in column_names]
    if not cols:
        raise ValueError("Primary key requires at least one column.")
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(qualified_table_name)} "
        f"ADD CONSTRAINT {quote_ident(constraint_name)} PRIMARY KEY ({', '.join(cols)})"
    )


def sql_drop_primary_key(qualified_table_name: str, constraint_name: str) -> str:
    """ALTER TABLE ... DROP CONSTRAINT ..."""
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(qualified_table_name)} "
        f"DROP CONSTRAINT {quote_ident(constraint_name)}"
    )


def sql_select_primary_key_name_for_table(three_part_name: ThreePartTableName) -> str:
    """SQL to fetch the PRIMARY KEY constraint name for a table (Unity Catalog)."""
    catalog, schema, table = map(escape_sql_literal, three_part_name)
    return f"""
      SELECT constraint_name AS name
      FROM information_schema.table_constraints
      WHERE constraint_type = 'PRIMARY KEY'
        AND table_catalog = '{catalog}'
        AND table_schema  = '{schema}'
        AND table_name    = '{table}'
      LIMIT 1
    """


def sql_select_primary_key_columns_for_table(three_part_name: ThreePartTableName) -> str:
    """
    Return SQL to list PRIMARY KEY columns (with ordinal_position) for a given table.
    Columns are ordered by their position in the PK.
    """
    catalog, schema, table = map(escape_sql_literal, three_part_name)
    return f"""
      SELECT
        kcu.column_name        AS column_name,
        kcu.ordinal_position   AS ordinal_position
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON  tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema  = kcu.constraint_schema
        AND tc.constraint_name    = kcu.constraint_name
        AND tc.table_catalog      = kcu.table_catalog
        AND tc.table_schema       = kcu.table_schema
        AND tc.table_name         = kcu.table_name
      WHERE tc.table_catalog   = '{catalog}'
        AND tc.table_schema    = '{schema}'
        AND tc.table_name      = '{table}'
        AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position
      """
