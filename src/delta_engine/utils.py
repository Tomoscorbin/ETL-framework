"""Utility helpers for identifier quoting, SQL literal escaping, and name handling."""

from __future__ import annotations

from collections.abc import Mapping

from src.delta_engine.types import HasTableIdentity


def escape_sql_literal(value: str) -> str:
    """
    Escape a Python string for use as a single-quoted SQL literal.

    Doubles single quotes per SQL rules. Empty/None → empty string.
    """
    return (value or "").replace("'", "''")


def qualify_table_name(obj: HasTableIdentity) -> str:
    """Return unescaped 'catalog.schema.table' for an object with table identity."""
    return f"{obj.catalog_name}.{obj.schema_name}.{obj.table_name}"


def quote_ident(identifier: str) -> str:
    """
    Quote an identifier for Databricks/Delta SQL using backticks, escaping embedded backticks.

    Example: foo`bar → `foo``bar`
    """
    if identifier is None:  # defensive: callers sometimes pass None at runtime
        raise ValueError("Identifier cannot be None")
    return f"`{identifier.replace('`', '``')}`"


def split_three_part(full_name: str) -> tuple[str, str, str]:
    """
    Split 'catalog.schema.table' into its three parts; raise if malformed.

    Rejects missing or empty parts.
    """
    parts = full_name.split(".")
    if len(parts) != 3 or any(p == "" for p in parts):
        raise ValueError(f"Expected three-part name 'catalog.schema.table', got: {full_name!r}")
    return parts[0], parts[1], parts[2]


def quote_qualified_name(catalog: str, schema: str, table: str) -> str:
    """Quote a three-part identifier as `catalog`.`schema`.`table`."""
    return ".".join(quote_ident(p) for p in (catalog, schema, table))


def quote_qualified_name_from_full(full_name: str) -> str:
    """Quote a 'catalog.schema.table' string as `catalog`.`schema`.`table`."""
    c, s, t = split_three_part(full_name)
    return quote_qualified_name(c, s, t)


def format_tblproperties(props: Mapping[str, str]) -> str:
    """
    Format TBLPROPERTIES assignments: `'key' = 'value', 'k2' = 'v2'`.

    Notes:
    -----
    - Keys and values are SQL **string literals** (NOT identifiers).
    This matches Spark/Databricks syntax,
    e.g., ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name').
    - Keys are sorted for deterministic output.
    """
    return ", ".join(
        f"'{escape_sql_literal(k)}' = '{escape_sql_literal(v)}'"
        for k, v in sorted(props.items(), key=lambda kv: kv[0])
    )
