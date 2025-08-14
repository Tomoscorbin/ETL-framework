"""Domain models for declaring Delta tables (logical schema + properties)."""

from __future__ import annotations

from enum import StrEnum
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import ClassVar

import pyspark.sql.types as T


class TableProperty(StrEnum):
    ENABLE_DELETION_VECTORS = "delta.enableDeletionVectors"
    ENABLE_TYPE_WIDENING    = "delta.enableTypeWidening"
    COLUMN_MAPPING_MODE     = "delta.columnMapping.mode"


@dataclass(frozen=True)
class Column:
    """Declarative Delta table column definition."""

    name: str
    data_type: T.DataType
    comment: str = ""
    is_nullable: bool = True


@dataclass(frozen=True)
class Table:
    """Declarative Delta table definition."""

    DEFAULT_PROPERTIES: ClassVar[dict[str, str]] = {
        TableProperty.COLUMN_MAPPING_MODE: "name",
    }

    catalog_name: str
    schema_name: str
    table_name: str
    columns: list[Column]
    comment: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] | None = None


    @property
    def full_name(self) -> str:
        """Full table name in catalgog.schema.table format"""
        return render_fully_qualified_name_from_parts(
            self.catalog_name, self.schema_name, self.table_name
        )

    @property
    def column_names(self) -> list[str]:
        """List of colomn names."""
        return [column.name for column in self.columns]
    
    @property
    def primary_key_name(self) -> str | None:
        """Compute the default PK name (None if no PK columns provided)."""
        if not self.primary_key_columns:
            return None
        return generate_primary_key_name(self.identity, self.primary_key_columns)
    
    @property
    def effective_table_properties(self) -> Mapping[str, str]:
        """
        Default table properties merged with user overrides (read-only view).

        Returns:
        -------
        Mapping[str, str]
            Read-only mapping combining DEFAULT_TABLE_PROPERTIES and properties.
        """
        merged = {**self.DEFAULT_PROPERTIES, **self.properties}
        return MappingProxyType(merged)
