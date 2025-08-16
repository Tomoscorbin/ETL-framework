"""
Plan actions: immutable, declarative operations targeting a single table.

Conventions
-----------
- Every executable action is tied to one `full_table_name` (FullyQualifiedTableName).
- Verbs:
  - Add*/Drop* for structural changes
  - Set* for idempotent attributes (comments, properties, nullability)
- Empty string clears a comment; empty mappings mean "no changes" unless otherwise noted.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Column

# ---------- base (executable) ----------


@dataclass(frozen=True)
class Action:
    """Base executable action tied to a single fully qualified table."""

    full_table_name: FullyQualifiedTableName


# ---------- sub-actions (payloads) ----------


@dataclass(frozen=True)
class AddColumns:
    """Add one or more columns."""

    columns: tuple[Column, ...]


@dataclass(frozen=True)
class DropColumns:
    """Drop one or more columns by exact column name."""

    columns: tuple[str, ...]


@dataclass(frozen=True)
class SetColumnNullability:
    """Set the nullability flag for a single column (True => NULL, False => NOT NULL)."""

    column_name: str
    make_nullable: bool


@dataclass(frozen=True)
class SetColumnComments:
    """Set per-column comments; empty string clears. Keys are column names (case as declared)."""

    comments: Mapping[str, str]  # {column_name: comment}


@dataclass(frozen=True)
class SetTableComment:
    """Set or clear the table comment; empty string clears."""

    comment: str  # "" => clear


@dataclass(frozen=True)
class SetTableProperties:
    """
    Replace table properties with the provided mapping.
    Executors may whitelist supported keys.
    """

    properties: Mapping[str, str]


@dataclass(frozen=True)
class AddPrimaryKey:
    """Create a PRIMARY KEY with a deterministic or explicit name."""

    name: str
    columns: tuple[str, ...]  # ordered


@dataclass(frozen=True)
class DropPrimaryKey:
    """Drop the existing PRIMARY KEY by name."""

    name: str


# ---------- executable actions (compose sub-actions) ----------


@dataclass(frozen=True)
class CreateTable(Action):
    """
    Create a Delta table in one shot.

    Compose with sub-actions:
      - add_columns: required schema for CREATE
      - set_table_comment: optional COMMENT (None => omit)
      - set_table_properties: optional properties (empty allowed)
      - add_primary_key: optional PK on create
    """

    add_columns: AddColumns
    set_table_comment: SetTableComment | None = None
    set_table_properties: SetTableProperties | None = None
    add_primary_key: AddPrimaryKey | None = None


@dataclass(frozen=True)
class AlignTable(Action):
    """Coalesced, per-table alignment. Any field None/empty => no-op for that slice."""

    add_columns: AddColumns | None = None
    drop_columns: DropColumns | None = None
    set_nullability: tuple[SetColumnNullability, ...] = ()
    set_column_comments: SetColumnComments | None = None
    set_table_comment: SetTableComment | None = None
    set_table_properties: SetTableProperties | None = None
    add_primary_key: AddPrimaryKey | None = None
    drop_primary_key: DropPrimaryKey | None = None
