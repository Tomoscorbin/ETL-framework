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

from src.delta_engine.common_types import ThreePartTableName

# ---------- Table Operations ----------

@dataclass(frozen=True)
class CreateTable:
    """Represents a CREATE TABLE operation."""

    catalog_name: str
    schema_name: str
    table_name: str
    schema_struct: T.StructType
    table_comment: str
    table_properties: Mapping[str, str]
    column_comments: Mapping[str, str]


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
class AlignTable:
    """
    Represents an ALTER TABLE operation to align an existing
    table to its desired state.
    """

    catalog_name: str
    schema_name: str
    table_name: str
    set_column_comments: SetColumnComments | None = None
    set_table_comment: SetTableComment | None = None
    set_table_properties: SetTableProperties | None = None

    add_columns: list[ColumnAdd] = field(default_factory=list)
    change_nullability: list[ColumnNullabilityChange] = field(default_factory=list)
    drop_columns: list[ColumnDrop] = field(default_factory=list)


# ---------- Constraints ----------

@dataclass(frozen=True)
class CreatePrimaryKey:
    """Add a PRIMARY KEY constraint to a table."""
    three_part_table_name: ThreePartTableName
    name: str
    columns: tuple[str, ...]

@dataclass(frozen=True)
class CreateForeignKey:
    """Add a FOREIGN KEY constraint from source to target table."""
    source_three_part_table_name: ThreePartTableName
    name: str
    source_columns: tuple[str, ...]
    target_three_part_table_name: ThreePartTableName
    target_columns: tuple[str, ...]

@dataclass(frozen=True)
class DropPrimaryKey:
    """Drop the PRIMARY KEY constraint from a table by name."""
    three_part_table_name: ThreePartTableName
    name: str

@dataclass(frozen=True)
class DropForeignKey:
    """Drop a FOREIGN KEY constraint from the source table by name."""
    source_three_part_table_name: ThreePartTableName
    name: str


# ---------- Plans ----------

@dataclass(frozen=True)
class Plan:
    """
    Represents a table change plan.

    Contains CREATE TABLE operations and ALTER TABLE alignment operations.
    """

    create_tables: list[CreateTable]
    align_tables: list[AlignTable]


@dataclass(frozen=True)
class ConstraintPlan:
    """
    Ordered constraint actions.
    Primary keys MUST be created before foreign keys.
    """
    create_primary_keys: tuple[CreatePrimaryKey, ...]
    create_foreign_keys: tuple[CreateForeignKey, ...]