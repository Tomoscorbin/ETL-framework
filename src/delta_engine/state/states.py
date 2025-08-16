"""
Observed catalog state dataclasses.

These types capture what exists in the Catalog *right now*:
- Columns (name, type, nullability, comment)
- Table-level metadata (comment, properties)
- Primary key (name + ordered columns)
- A catalog-wide snapshot keyed by FullyQualifiedTableName

Notes:
- Dataclasses are frozen and use tuples/read-only mappings for nested data.
- Construction sites (e.g., CatalogReader) should convert lists/dicts to
  tuples / MappingProxyType before creating these objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Self

import pyspark.sql.types as T

from src.delta_engine.identifiers import (
    FullyQualifiedTableName,
    format_fully_qualified_table_name_from_parts,
)

# -----------------------------
# Column / PK states
# -----------------------------


@dataclass(frozen=True, slots=True)
class ColumnState:
    """Observed column state: name, Spark SQL data type, nullability, and optional comment."""

    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str = ""


@dataclass(frozen=True, slots=True)
class PrimaryKeyState:
    """Observed PRIMARY KEY constraint: constraint name and ordered column list."""

    name: str
    columns: tuple[str, ...]


# -----------------------------
# Table and catalog snapshots
# -----------------------------


@dataclass(frozen=True, slots=True)
class TableState:
    """
    Observed table state: identity, existence, schema, comment, properties, and PK.

    Fields
    ------
    catalog_name, schema_name, table_name : str
        Table identity (unescaped).
    exists : bool
        Whether the table exists in the catalog.
    columns : tuple[ColumnState, ...]
        Physical columns in order.
    comment : str
        Empty string means "no comment".
    properties : Mapping[str, str]
        Read-only view of table properties.
    primary_key : PrimaryKeyState | None
        Present when a PRIMARY KEY exists.
    """

    catalog_name: str
    schema_name: str
    table_name: str
    exists: bool
    columns: tuple[ColumnState, ...] = field(default_factory=tuple)
    comment: str = ""
    properties: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    primary_key: PrimaryKeyState | None = None

    @property
    def full_name(self) -> str:
        """Unquoted full name: 'catalog.schema.table'."""
        return format_fully_qualified_table_name_from_parts(
            self.catalog_name, self.schema_name, self.table_name
        )

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Ordered PK columns (empty tuple if no PK)."""
        return self.primary_key.columns if self.primary_key else ()

    @classmethod
    def empty(cls, catalog_name: str, schema_name: str, table_name: str) -> Self:
        """Factory for a non-existent table snapshot with empty metadata."""
        return cls(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            exists=False,
        )


@dataclass(frozen=True, slots=True)
class CatalogState:
    """Point-in-time snapshot of multiple tables, keyed by FullyQualifiedTableName."""

    tables: Mapping[FullyQualifiedTableName, TableState]

    def get(self, full_table_name: FullyQualifiedTableName) -> TableState | None:
        """Return the TableState for `full_table_name`, or None if absent."""
        return self.tables.get(full_table_name)

    def contains(self, full_table_name: FullyQualifiedTableName) -> bool:
        """True if `full_table_name` is present in this snapshot."""
        return full_table_name in self.tables
