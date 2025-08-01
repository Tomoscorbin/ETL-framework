from dataclasses import dataclass

import pyspark.sql.types as T


@dataclass(frozen=True)
class ForeignKey:
    """Represents a foreign key constraint."""

    table_name: str
    column_name: str


@dataclass(frozen=True)
class DeltaColumn:
    """Represents a Delta Table column."""

    name: str
    data_type: T.DataType
    comment: str = ""
    is_primary_key: bool = False
    is_nullable: bool = True
    foreign_key: ForeignKey | None = None

    @property
    def struct_field(self) -> T.StructField:
        """PySpark `StructField` representation of a column."""
        return T.StructField(self.name, self.data_type, self.is_nullable)
