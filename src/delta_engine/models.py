"""Domain models for declaring Delta tables (logical schema + properties)."""

from __future__ import annotations

from dataclasses import dataclass, field
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

    @property
    def full_name(self) -> str:
        """Fully qualified name in `catalog.schema.table` format."""
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

    @property
    def effective_table_properties(self) -> dict[str, str]:
        """Default table properties + user overrides."""
        return {**self.DEFAULT_TABLE_PROPERTIES, **self.table_properties}

    @property
    def column_names(self) -> list[str]:
        """List of coloumn names."""
        return [column.name for column in self.columns]
