from __future__ import annotations

from collections.abc import Mapping

from src.delta_engine.types import HasTableIdentity


def escape(s: str) -> str:
    """Escape single quotes for safe SQL string literals."""
    return s.replace("'", "''")


def qualify_table_name(obj: HasTableIdentity) -> str:
    """Return the fully qualified table name 'catalog.schema.table' for a table object."""
    return f"{obj.catalog_name}.{obj.schema_name}.{obj.table_name}"


def escape_sql_literal(value: str) -> str:
    """Escape single quotes in a SQL string literal by doubling them."""
    return (value or "").replace("'", "''")


def quote_ident(identifier: str) -> str:
    """Quote an identifier for Databricks/Delta SQL using backticks, escaping embedded backticks."""
    if identifier is None:
        raise ValueError("Identifier cannot be None")
    return f"`{identifier.replace('`', '``')}`"


def split_three_part(full_name: str) -> tuple[str, str, str]:
    """Split 'catalog.schema.table' into its three parts; raise if malformed."""
    parts = full_name.split(".")
    if len(parts) != 3:
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
    """Format TBLPROPERTIES assignments like `key = 'value', key2 = 'value2'`."""
    return ", ".join(f"{quote_ident(k)} = '{escape_sql_literal(v)}'" for k, v in props.items())
