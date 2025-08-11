"""
This module defines dataclasses representing schema changes
and table operations, such as creating tables, adding or dropping columns,
changing nullability, and aligning table properties. These actions are
used to describe a table migration plan before execution.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import pyspark.sql.types as T

# ---------- Common payloads ----------


@dataclass(frozen=True)
class PrimaryKeyDefinition:
    """Definition of a PRIMARY KEY (used within actions)."""

    name: str
    columns: tuple[str, ...]  # ordered


# ---------- Table Operations ----------


@dataclass(frozen=True)
class ColumnAdd:
    """Represents an ADD COLUMN operation."""

    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str = ""


@dataclass(frozen=True)
class ColumnDrop:
    """Represents a DROP COLUMN operation."""

    name: str


@dataclass(frozen=True)
class ColumnNullabilityChange:
    """
    Represents a change to a column's nullability.

    make_nullable=True  -> DROP NOT NULL
    make_nullable=False -> SET NOT NULL
    """

    name: str
    make_nullable: bool


@dataclass(frozen=True)
class SetColumnComments:
    """Represents setting comments on one or more columns."""

    comments: Mapping[str, str]


@dataclass(frozen=True)
class SetTableComment:
    """Represents setting the table comment."""

    comment: str


@dataclass(frozen=True)
class SetTableProperties:
    """Represents setting one or more table properties."""

    properties: Mapping[str, str]


@dataclass(frozen=True)
class PrimaryKeyAdd:
    """ADD PRIMARY KEY constraint on a table."""

    definition: PrimaryKeyDefinition


@dataclass(frozen=True)
class PrimaryKeyDrop:
    """DROP PRIMARY KEY constraint from a table."""

    name: str


@dataclass(frozen=True)
class CreateTable:
    """CREATE TABLE with schema, metadata, and optional PRIMARY KEY."""

    catalog_name: str
    schema_name: str
    table_name: str
    schema_struct: T.StructType
    table_comment: str
    table_properties: Mapping[str, str]
    column_comments: Mapping[str, str]
    primary_key: PrimaryKeyDefinition | None = None


@dataclass(frozen=True)
class AlignTable:
    """
    ALTER TABLE to align an existing table to the desired state.
    Includes column edits, metadata tweaks, and PK add/drop for this table.
    """

    catalog_name: str
    schema_name: str
    table_name: str

    # Column edits
    add_columns: tuple[ColumnAdd, ...] = field(default_factory=tuple)
    drop_columns: tuple[ColumnDrop, ...] = field(default_factory=tuple)
    change_nullability: tuple[ColumnNullabilityChange, ...] = field(default_factory=tuple)

    # Metadata
    set_column_comments: SetColumnComments | None = None
    set_table_comment: SetTableComment | None = None
    set_table_properties: SetTableProperties | None = None

    # PK edits (can be both in one action to “recreate”)
    drop_primary_key: PrimaryKeyDrop | None = None
    add_primary_key: PrimaryKeyAdd | None = None


# ---------- Plans ----------


@dataclass(frozen=True)
class TablePlan:
    """
    Represents a table change plan.

    Contains CREATE TABLE operations and ALTER TABLE alignment operations.
    """

    create_tables: tuple[CreateTable, ...]
    align_tables: tuple[AlignTable, ...]
