"""Data structures representing Delta table columns and quality rules."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyspark.sql.types as T

from src.enums import DQCriticality
from src.models.utils import build_foreign_key_name


@dataclass(frozen=True)
class ForeignKey:
    """Represents a foreign key reference to another table and column."""

    reference_table_name: str
    reference_column_name: str

    def to_spec(
        self,
        source_catalog: str,
        source_schema: str,
        source_table: str,
        source_column: str,
    ) -> dict[str, str]:
        """Return a dict specification for this foreign key using the given source context."""
        constraint_name = build_foreign_key_name(
            source_catalog=source_catalog,
            source_table=source_table,
            source_schema=source_schema,
            source_column=source_column,
            target_table=self.reference_table_name,
            target_column=self.reference_column_name,
        )
        return {
            "constraint_name": constraint_name,
            "source_column": source_column,
            "reference_table": self.reference_table_name,
            "reference_column": self.reference_column_name,
        }


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
