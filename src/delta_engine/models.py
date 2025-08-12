"""Domain models for declaring Delta tables (logical schema + properties)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import ClassVar

import pyspark.sql.types as T

from src.enums import DeltaTableProperty


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

    DEFAULT_TABLE_PROPERTIES: ClassVar[dict[str, str]] = {
        DeltaTableProperty.COLUMN_MAPPING_MODE: "name",
    }

    catalog_name: str
    schema_name: str
    table_name: str
    columns: list[Column]
    comment: str = ""
    table_properties: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] | None = None

    @property
    def full_name(self) -> str:
        """Fully qualified name in `catalog.schema.table` format."""
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

    @property
    def effective_table_properties(self) -> Mapping[str, str]:
        """
        Default table properties merged with user overrides (read-only view).

        Returns:
        -------
        Mapping[str, str]
            Read-only mapping combining DEFAULT_TABLE_PROPERTIES and table_properties.
        """
        merged = {**self.DEFAULT_TABLE_PROPERTIES, **self.table_properties}
        return MappingProxyType(merged)

    @property
    def column_names(self) -> list[str]:
        """List of colomn names."""
        return [column.name for column in self.columns]
