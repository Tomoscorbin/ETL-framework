"""
Observed catalog state dataclasses.

These types capture what exists in Unity Catalog / Delta *right now*:
- Columns (name, type, nullability, comment)
- Table-level metadata (comment, properties)
- Primary key (name + ordered columns)
- A catalog-wide snapshot keyed by unescaped 'catalog.schema.table'

Notes:
-----
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

from src.delta_engine.identifiers import FullyQualifiedTableName


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


@dataclass(frozen=True, slots=True)
class TableState:
    """
    Observed table state: identity, existence, schema, comments, properties, and PK.

    Fields
    ------
    catalog_name, schema_name, table_name : str
        Table identity (unescaped).
    exists : bool
        Whether the table exists in the catalog.
    columns : tuple[ColumnState, ...]
        Physical columns in order.
    table_comment : str
        Empty string means "no comment".
    table_properties : Mapping[str, str]
        Read-only view of table properties.
    primary_key : PrimaryKeyState | None
        Present when a PRIMARY KEY exists.
    """

    catalog_name: str
    schema_name: str
    table_name: str
    exists: bool
    columns: tuple[ColumnState, ...] = field(default_factory=tuple)
    table_comment: str = ""
    table_properties: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    primary_key: PrimaryKeyState | None = None

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
    """
    Point-in-time snapshot of multiple tables, keyed by unescaped full name.

    Example key: "catalog.schema.table"
    """

    tables: Mapping[FullyQualifiedTableName, TableState]

    def get(self, identity: FullyQualifiedTableName) -> TableState | None:
        return self.tables.get(identity)

    def contains(self, identity: FullyQualifiedTableName) -> bool:
        return identity in self.tables
