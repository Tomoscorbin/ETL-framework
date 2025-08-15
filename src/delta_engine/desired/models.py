"""
Desired catalog specification models.

These dataclasses define the *intended* state of tables. The diff engine compares
them to the observed state and produces actions.

Conventions and semantics
-------------------------
- `fully_qualified_table_name`: structured identity (catalog, schema, table).
- `columns`: full desired logical schema in order.
- `primary_key_columns` (tri-state):
    None  → unmanaged (no PK actions)
    ()    → ensure no PRIMARY KEY (drop if it exists)
    ("c1","c2",...) → ensure PRIMARY KEY with these ordered columns
- `primary_key_name_override`: when set, use this name; otherwise derive a deterministic one.
- `table_comment` (tri-state):
    None  → unmanaged
    ""    → clear comment
    "..." → set to the given text
- `table_properties` (tri-state):
    None  → unmanaged
    {}    → manage and ensure empty set (executor may whitelist)
    {k:v} → manage and ensure exactly these key/value pairs
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Column


@dataclass(frozen=True)
class DesiredTable:
    """Desired state for a single table."""
    fully_qualified_table_name: FullyQualifiedTableName
    columns: tuple[Column, ...]
    primary_key_columns: tuple[str, ...] | None = None
    primary_key_name_override: str | None = None
    table_comment: str | None = None
    table_properties: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DesiredCatalog:
    """A set of desired tables to manage."""
    tables: tuple[DesiredTable, ...]
