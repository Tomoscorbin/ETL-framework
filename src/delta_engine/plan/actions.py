from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Tuple, Optional

import pyspark.sql.types as T
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Column


# ---------- base (executable) ----------

@dataclass(frozen=True)
class Action:
    """Base executable action tied to a single fully-qualified table."""
    table: FullyQualifiedTableName


# ---------- sub-actions (payloads) ----------

@dataclass(frozen=True)
class AddColumns:
    """Add one or more columns."""
    columns: Tuple[Column, ...]


@dataclass(frozen=True)
class AlterColumnNullability:
    """Flip NOT NULL flag for a single column."""
    column_name: str
    make_nullable: bool


@dataclass(frozen=True)
class SetColumnComments:
    """Set comments per column; empty string clears."""
    comments: Mapping[str, str]  # {column_name: comment}


@dataclass(frozen=True)
class SetTableComment:
    """Set/clear table comment; empty string clears."""
    comment: str  # "" => clear


@dataclass(frozen=True)
class SetTableProperties:
    """Replace table properties with the provided mapping (executor may whitelist)."""
    properties: Mapping[str, str]


@dataclass(frozen=True)
class AddPrimaryKey:
    """Create a PRIMARY KEY with a deterministic/explicit name."""
    name: str
    columns: Tuple[str, ...]  # ordered


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
    set_table_comment: Optional[SetTableComment] = None
    set_table_properties: Optional[SetTableProperties] = None
    add_primary_key: Optional[AddPrimaryKey] = None


@dataclass(frozen=True)
class AlignTable(Action):
    """
    Coalesced, per-table alignment. Any field None/empty => no-op for that slice.
    """
    add_columns: Optional[AddColumns] = None
    alter_nullability: Tuple[AlterColumnNullability, ...] = ()
    set_column_comments: Optional[SetColumnComments] = None
    set_table_comment: Optional[SetTableComment] = None
    set_table_properties: Optional[SetTableProperties] = None
    add_primary_key: Optional[AddPrimaryKey] = None
    drop_primary_key: Optional[DropPrimaryKey] = None
