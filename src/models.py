from dataclasses import dataclass, field

import pyspark.sql.types as T
from pyspark.sql import DataFrame


@dataclass(frozen=True)
class Column:
    """Represents a Delta Table column."""

    name: str
    data_type: T.DataType
    comment: str = ""
    is_primary_key: bool = False
    is_nullable: bool = True

    @property
    def struct_field(self) -> T.StructField:
        """PySpark `StructField` representation of a column."""
        return T.StructField(self.name, self.data_type, True)


@dataclass(frozen=True)
class DeltaTable:
    """Represents a Delta Table."""

    table_name: str
    schema_name: str
    catalog_name: str
    columns: list[Column]
    comment: str = ""
    delta_properties: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        """Full table name in the format `catalog.schema.table`."""
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

    @property
    def column_names(self) -> list[str]:
        """List of coloumn names."""
        return [column.name for column in self.columns]

    @property
    def schema(self) -> T.StructType:
        """PySpark `StructType` representation of the table schema."""
        return T.StructType(fields=[column.struct_field for column in self.columns])

    def overwrite(self, df: DataFrame) -> None:
        """
        Overwrites the Delta Table with the given DataFrame.

        Args:
            df (DataFrame): DataFrame to overwrite the Delta Table with.

        Returns: None
        """
        df.select(self.column_names).write.saveAsTable(
            name=self.full_name, format="delta", mode="overwrite"
        )
