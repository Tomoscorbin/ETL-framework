from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence, Any
import pyspark.sql.types as T


@dataclass(frozen=True)
class ForeignKey:
    """
    A single-column foreign key to another table.

    If reference_catalog_name or reference_schema_name are not provided,
    they default to the source table's catalog and schema during planning.
    """
    reference_table_name: str
    reference_column_name: str
    reference_catalog_name: str | None = None
    reference_schema_name: str | None = None


@dataclass(frozen=True)
class Column:
    """
    A column in a Delta table.
    """
    name: str
    data_type: T.DataType
    comment: str = ""
    is_nullable: bool = True
    is_primary_key: bool = False


@dataclass(frozen=True)
class Table:
    """
    A declarative description of a Delta table.
    """
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
