"""Domain models for declaring Delta tables (logical schema + properties)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar

import pyspark.sql.types as T

from src.delta_engine.identifiers import (
    build_primary_key_name,
    format_fully_qualified_table_name_from_parts,
)


class TableProperty(StrEnum):
    ENABLE_DELETION_VECTORS = "delta.enableDeletionVectors"
    ENABLE_TYPE_WIDENING = "delta.enableTypeWidening"
    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"


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

    # Defaults use string keys (Enum.value); exposed read-only.
    DEFAULT_PROPERTIES: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            TableProperty.COLUMN_MAPPING_MODE.value: "name",
        }
    )

    catalog_name: str
    schema_name: str
    table_name: str
    columns: Sequence[Column]
    comment: str = ""
    properties: Mapping[str, str] = field(default_factory=dict)
    primary_key: Sequence[str] | None = None

    # --------- Convenience properties ---------

    @property
    def full_name(self) -> str:
        """Unquoted full name: 'catalog.schema.table'."""
        return format_fully_qualified_table_name_from_parts(
            self.catalog_name, self.schema_name, self.table_name
        )

    @property
    def column_names(self) -> tuple[str, ...]:
        """Column names in declared order."""
        return tuple(column.name for column in self.columns)

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Primary-key column names (empty tuple if none)."""
        return tuple(self.primary_key) if self.primary_key else tuple()

    @property
    def primary_key_name(self) -> str | None:
        """Deterministic PK constraint name, or None if no primary key is declared."""
        pk_columns = self.primary_key_columns
        if not pk_columns:
            return None

        return build_primary_key_name(
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            columns=pk_columns,
        )

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Default properties merged with user-provided properties (read-only)."""
        user = _normalize_properties(self.properties)
        merged = {**dict(self.DEFAULT_PROPERTIES), **user}
        return MappingProxyType(merged)


# -----------------
# Helpers
# -----------------


def _normalize_properties(props: Mapping[Any, Any]) -> dict[str, str]:
    """Coerce mapping keys/values to strings; supports TableProperty keys."""
    normalized: dict[str, str] = {}
    for k, v in dict(props).items():
        key = k.value if isinstance(k, TableProperty) else str(k)
        normalized[key] = str(v)
    return normalized
