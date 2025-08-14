# src/delta_engine/desired/builders.py
"""
Adapters: user-facing Table -> engine-facing Desired*.

Why this exists
---------------
Users declare tables with `models.Table`. The engine plans and executes using
`Desired*` types that encode tri-state semantics:

- table_comment:
    None  => unmanaged (engine does not touch it)
    ""    => clear the comment
    "x"   => set to "x"

- table_properties:
    None  => unmanaged (engine does not touch properties)
    dict  => enforce exactly these (executor may still whitelist)

- primary_key_columns:
    None  => unmanaged (do not add or drop)
    ()    => enforce NO primary key (drop if present)
    ("id", ...) => enforce that primary key (engine derives name unless overridden)

This module centralises the mapping so the rest of the engine never has to guess.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Tuple

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Table, Column
from src.delta_engine.desired.models import DesiredCatalog, DesiredTable


# ---------- mapping options (edge policy) ----------

@dataclass(frozen=True)
class TableMappingOptions:
    """
    Controls how user-facing Table fields map to tri-state Desired* fields.

    enforce_properties:
        True  -> enforce defaults + overrides (use Table.effective_table_properties)
        False -> leave properties unmanaged (DesiredTable.table_properties = None)

    empty_comment_means_clear:
        True  -> "" means "clear the comment"
        False -> "" is treated as unmanaged (DesiredTable.table_comment = None)
    """
    enforce_properties: bool = True
    empty_comment_means_clear: bool = True


# ---------- tiny helpers (single-purpose, explicit) ----------

def _to_fully_qualified_name(table: Table) -> FullyQualifiedTableName:
    """Map a user Table into a FullyQualifiedTableName (catalog, schema, table)."""
    return FullyQualifiedTableName(
        catalog=table.catalog_name,
        schema=table.schema_name,
        table=table.table_name,
    )


def _map_columns(table: Table) -> Tuple[Column, ...]:
    """
    Columns flow straight through; convert to an immutable tuple
    for planner determinism.
    """
    return tuple(table.columns)


def _map_primary_key_columns(table: Table) -> Tuple[str, ...] | None:
    """
    Tri-state mapping:
      - None  => unmanaged
      - []    => explicitly no PK (empty tuple)
      - ["id", ...] => enforce
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
    # Treat empty string as unmanaged
    return table.comment or None


def _map_table_properties(table: Table, options: TableMappingOptions) -> Mapping[str, str] | None:
    """
    Map properties with tri-state semantics.
      - enforce_properties=False => unmanaged (None)
      - True => enforce Table.effective_table_properties (defaults + overrides)
    """
    if not options.enforce_properties:
        return None
    # Make a plain dict to decouple from MappingProxyType
    return dict(table.effective_table_properties)


# ---------- public builders ----------

def build_desired_table(
    table: Table,
    options: TableMappingOptions | None = None,
) -> DesiredTable:
    """
    Convert a single user Table into a DesiredTable with explicit tri-state fields.
    """
    mapping = options or TableMappingOptions()
    return DesiredTable(
        fully_qualified_table_name=_to_fully_qualified_name(table),
        columns=_map_columns(table),
        primary_key_columns=_map_primary_key_columns(table),
        primary_key_name_override=None,  # let the engine derive a stable name unless you add a policy here
        table_comment=_map_table_comment(table, mapping),
        table_properties=_map_table_properties(table, mapping),
    )


def build_desired_catalog(
    tables: Iterable[Table],
    options: TableMappingOptions | None = None,
) -> DesiredCatalog:
    """
    Convert an iterable of user Tables into a DesiredCatalog the engine can plan on.
    """
    desired_tables = tuple(build_desired_table(t, options) for t in tables)
    return DesiredCatalog(tables=desired_tables)
