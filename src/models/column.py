from dataclasses import dataclass

import pyspark.sql.types as T


@dataclass(frozen=True)
class DeltaColumn:
    """Represents a Delta Table column."""

    name: str
    data_type: T.DataType
    comment: str = ""
    is_primary_key: bool = False
    is_nullable: bool = True

    @property
    def struct_field(self) -> T.StructField:
        """PySpark `StructField` representation of a column."""
        # Ensure the StructField accurately reflects the column's nullability.
        # Previously this always set ``nullable`` to ``False`` which meant
        # nullable columns were incorrectly represented as non-nullable in the
        # resulting schema.
        return T.StructField(self.name, self.data_type, self.is_nullable)
