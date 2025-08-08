"""Data structures representing Delta table columns and quality rules."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

import pyspark.sql.types as T

from src.enums import DQCriticality
from src.models.utils import build_fk_name_minimal

if TYPE_CHECKING:
    from src.models.table import DeltaTable


@dataclass(frozen=True)
class ForeignKey:
    target_table: "DeltaTable"
    target_column: str

    def constraint_name(self, source_table: "DeltaTable", salt: str | None = None) -> str:
        target_catalog = self.target_table.catalog_name
        target_table = self.target_table.table_name

        return build_fk_name_minimal(
            source_catalog=source_table.catalog_name,
            source_table=source_table.table_name,
            target_catalog=target_catalog,
            target_table=target_table,
            salt=salt,
        )



@dataclass(frozen=True)
class QualityRule:
    """Data quality rule for one column."""

    criticality: str = DQCriticality.ERROR
    allowed_values: Sequence[Any] | None = None
    min_value: int | float | None = None
    max_value: int | float | None = None


@dataclass(frozen=True)
class DeltaColumn:
    """Represents a Delta Table column."""

    name: str
    data_type: T.DataType
    comment: str = ""
    is_primary_key: bool = False
    is_nullable: bool = True
    foreign_key: ForeignKey | None = None
    quality_rule: QualityRule | None = None

    @property
    def struct_field(self) -> T.StructField:
        """PySpark `StructField` representation of a column."""
        return T.StructField(self.name, self.data_type, self.is_nullable)
