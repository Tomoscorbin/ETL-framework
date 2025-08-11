"""
Represents the observed catalog state. This module defines immutable
dataclasses for describing observed column, table, and catalog state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Self

import pyspark.sql.types as T


@dataclass(frozen=True)
class ColumnState:
    """Observed column state, including schema and metadata."""

    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str = ""


@dataclass(frozen=True)
class PrimaryKeyState:
    """Observed PRIMARY KEY on a table."""

    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class TableState:
    """Observed table state, including columns, comments, and properties."""

    catalog_name: str
    schema_name: str
    table_name: str
    exists: bool
    columns: list[ColumnState] = field(default_factory=list)
    table_comment: str = ""
    table_properties: dict[str, str] = field(default_factory=dict)
    primary_key: PrimaryKeyState | None = None

    @property
    def full_name(self) -> str:
        """Return the fully qualified table name (catalog.schema.table)."""
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

    @classmethod
    def empty(cls, catalog_name: str, schema_name: str, table_name: str) -> Self:
        """Factory for a non-existent table snapshot."""
        return cls(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            exists=False,
            columns=[],
            table_comment="",
            table_properties={},
            primary_key=None,
        )


@dataclass(frozen=True)
class CatalogState:
    """Snapshot of multiple tables, keyed by full name."""

    tables: dict[str, TableState]

    def get(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> TableState | None:
        """Retrieve the TableState for a specific table."""
        return self.tables.get(f"{catalog_name}.{schema_name}.{table_name}")
