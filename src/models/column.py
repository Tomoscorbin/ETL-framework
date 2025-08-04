from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyspark.sql.types as T

from src.enums import DQCriticality


@dataclass(frozen=True)
class ForeignKey:
    """Represents a foreign key constraint."""

    table_name: str
    column_name: str


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
