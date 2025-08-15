# src/delta_engine/desired/builders.py
"""
Adapters: user-facing Table → engine-facing Desired*

Why this exists
---------------
Users declare tables with `models.Table`. The engine plans and executes using
`Desired*` types that encode tri-state semantics:

- table_comment:
    None  → unmanaged (engine does not touch it)
    ""    → clear the comment
    "x"   → set to "x"

- table_properties:
    None  → unmanaged (engine does not touch properties)
    dict  → enforce exactly these (executor may still whitelist)

- primary_key_columns:
    None           → unmanaged (do not add or drop)
    ()             → ensure no primary key (drop if present)
    ("id", ...)    → ensure that primary key with the given ordered columns

This module centralises the mapping so the rest of the engine never has to guess.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Column, Table


# ---------- mapping options (edge policy) ----------

@dataclass(frozen=True)
class TableMappingOptions:
    """
    Controls how user-facing Table fields map to tri-state Desired* fields.

    enforce_properties:
        True  → enforce defaults + overrides (use Table.effective_properties)
        False → leave properties unmanaged (DesiredTable.table_properties = None)

    empty_comment_means_clear:
        True  → "" means "clear the comment"
        False → "" is treated as unmanaged (DesiredTable.table_comment = None)
    """
    enforce_properties: bool = True
    empty_comment_means_clear: bool = True


# ---------- tiny helpers ----------

def _build_full_table_name_from_table(table: Table) -> FullyQualifiedTableName:
    """Build a FullyQualifiedTableName from a user Table (catalog, schema, table)."""
    return FullyQualifiedTableName(
        catalog=table.catalog_name,
        schema=table.schema_name,
        table=table.table_name,
    )


def _map_columns(table: Table) -> tuple[Column, ...]:
    """Columns flow straight through; convert to an immutable tuple for determinism."""
    return tuple(table.columns)


def _map_primary_key_columns(table: Table) -> tuple[str, ...] | None:
    """
    Tri-state mapping:
      - None  → unmanaged
      - []    → explicitly no primary key (empty tuple)
      - ["id", ...] → enforce with the given order
    """
    if table.primary_key is None:
        return None
    return tuple(table.primary_key)  # [] -> (), preserves order otherwise


def _map_table_comment(table: Table, options: TableMappingOptions) -> str | None:
    """
    Map comment with tri-state semantics controlled by options.
      - If empty_comment_means_clear=True: pass through as-is (None/""/"x").
      - Else: treat "" as unmanaged (None).
    """
    if options.empty_comment_means_clear:
        return table.comment
    return table.comment or None  # treat empty string as unmanaged


def _map_table_properties(table: Table, options: TableMappingOptions) -> Mapping[str, str] | None:
    """
    Map properties with tri-state semantics.
      - enforce_properties=False → unmanaged (None)
      - True → enforce Table.effective_properties (defaults + overrides)
    """
    if not options.enforce_properties:
        return None
    # Make a plain dict to decouple from MappingProxyType
    return dict(table.effective_properties)


# ---------- public builders ----------

def build_desired_table(
    table: Table,
    options: TableMappingOptions | None = None,
) -> DesiredTable:
    """Convert a single user Table into a DesiredTable with explicit tri-state fields."""
    mapping_options = options or TableMappingOptions()

    full_table_name = _build_full_table_name_from_table(table)
    columns = _map_columns(table)
    primary_key_columns = _map_primary_key_columns(table)
    table_comment = _map_table_comment(table, mapping_options)
    table_properties = _map_table_properties(table, mapping_options)
    primary_key_name_override = None  # engine derives a stable name unless policy changes

    return DesiredTable(
        fully_qualified_table_name=full_table_name,
        columns=columns,
        primary_key_columns=primary_key_columns,
        primary_key_name_override=primary_key_name_override,
        table_comment=table_comment,
        table_properties=table_properties,
    )


def build_desired_catalog(
    tables: Iterable[Table],
    options: TableMappingOptions | None = None,
) -> DesiredCatalog:
    """Convert an iterable of user Tables into a DesiredCatalog the engine can plan on."""
    desired_list = [build_desired_table(table, options) for table in tables]
    desired_tables = tuple(desired_list)
    return DesiredCatalog(tables=desired_tables)
