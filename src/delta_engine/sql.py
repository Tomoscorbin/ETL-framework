from __future__ import annotations

from collections.abc import Iterable, Mapping

import pyspark.sql.types as T

from src.delta_engine.types import ThreePartTableName
from src.delta_engine.utils import (
    escape,
    escape_sql_literal,
    format_tblproperties,
    quote_ident,
    quote_qualified_name_from_full,
)


def sql_set_table_properties(full_name: str, props: Mapping[str, str]) -> str | None:
    """ALTER TABLE ... SET TBLPROPERTIES (...). Returns None if no props."""
    if not props:
        return None
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(full_name)} "
        f"SET TBLPROPERTIES ({format_tblproperties(props)})"
    )


def sql_add_column(full_name: str, name: str, dtype: T.DataType, comment: str = "") -> str:
    """ALTER TABLE ... ADD COLUMNS (`col` type [COMMENT '...'])."""
    full = quote_qualified_name_from_full(full_name)
    col = quote_ident(name)
    dt = dtype.simpleString()
    comment_sql = f" COMMENT '{escape_sql_literal(comment)}'" if comment else ""
    return f"ALTER TABLE {full} ADD COLUMNS ({col} {dt}{comment_sql})"


def sql_drop_columns(full_name: str, names: Iterable[str]) -> str | None:
    """ALTER TABLE ... DROP COLUMNS (...). Returns None if no names."""
    cols = [quote_ident(n) for n in names]
    if not cols:
        return None
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(full_name)} DROP COLUMNS ({', '.join(cols)})"
    )


def sql_set_column_nullability(full_name: str, name: str, make_nullable: bool) -> str:
    """ALTER TABLE ... ALTER COLUMN `col` DROP/SET NOT NULL."""
    op = "DROP NOT NULL" if make_nullable else "SET NOT NULL"
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(full_name)} "
        f"ALTER COLUMN {quote_ident(name)} {op}"
    )


def sql_set_column_comment(full_name: str, name: str, comment: str) -> str:
    """COMMENT ON COLUMN ..."""
    return (
        f"COMMENT ON COLUMN {quote_qualified_name_from_full(full_name)}."
        f"{quote_ident(name)} IS '{escape_sql_literal(comment or '')}'"
    )


def sql_set_table_comment(full_name: str, comment: str) -> str:
    """COMMENT ON TABLE ..."""
    return (
        f"COMMENT ON TABLE {quote_qualified_name_from_full(full_name)} "
        f"IS '{escape_sql_literal(comment or '')}'"
    )


def sql_add_primary_key(full_name: str, constraint_name: str, columns: Iterable[str]) -> str:
    """ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)."""
    cols = [quote_ident(c) for c in columns]
    if not cols:
        raise ValueError("Primary key requires at least one column.")
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(full_name)} "
        f"ADD CONSTRAINT {quote_ident(constraint_name)} PRIMARY KEY ({', '.join(cols)})"
    )


def sql_drop_primary_key(full_name: str, constraint_name: str) -> str:
    """ALTER TABLE ... DROP CONSTRAINT ..."""
    return (
        f"ALTER TABLE {quote_qualified_name_from_full(full_name)} "
        f"DROP CONSTRAINT {quote_ident(constraint_name)}"
    )


def sql_select_primary_key_name_for_table(t: ThreePartTableName) -> str:
    """SQL to fetch the PRIMARY KEY constraint name for a table (Unity Catalog)."""
    c, s, n = map(escape, t)
    return f"""
      SELECT constraint_name AS name
      FROM information_schema.table_constraints
      WHERE constraint_type = 'PRIMARY KEY'
        AND table_catalog = '{c}'
        AND table_schema  = '{s}'
        AND table_name    = '{n}'
      LIMIT 1
    """


def sql_select_primary_key_columns_for_table(three_part: ThreePartTableName) -> str:
    """
    Return SQL to list PRIMARY KEY columns (with ordinal_position) for a given table.
    Columns are ordered by their position in the PK.
    """
    catalog, schema, table = three_part

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
      WHERE tc.table_catalog   = '{escape(catalog)}'
        AND tc.table_schema    = '{escape(schema)}'
        AND tc.table_name      = '{escape(table)}'
        AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position
      """
