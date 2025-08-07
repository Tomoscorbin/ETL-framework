"""Data structures representing Delta table columns and quality rules."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyspark.sql.types as T

from src.enums import DQCriticality
from src.models.utils import split_qualified_name


@dataclass(frozen=True)
class ForeignKey:
    """Represents a foreign key constraint."""

    reference_table_full_name: str
    reference_column_name: str

    def constraint_name(self, source_table_name: str) -> str:
        """
        Naming convention:
          fk_<catalog>_<source_table>_<reference_table>_<reference_column>
        """
        catalog_name, _, reference_table_name = split_qualified_name(self.reference_table_full_name)
        return (
            f"fk_{catalog_name}"
            f"_{source_table_name}"
            f"_{reference_table_name}"
            f"_{self.reference_column_name}"
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
