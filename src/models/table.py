from dataclasses import dataclass, field
from typing import ClassVar

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.rule import DQRule  # type: ignore
from src.enums import DeltaTableProperty
from src.models.column import DeltaColumn
from src.models.table_manager import DeltaTableManager
from src.models.writer import DeltaWriter


@dataclass(frozen=True)
class DeltaTable:
    """Represents a Delta Table."""

    default_delta_properties: ClassVar[dict[str, str]] = {
        DeltaTableProperty.COLUMN_MAPPING_MODE: "name",
    }

    table_name: str
    schema_name: str
    catalog_name: str
    columns: list[DeltaColumn]
    comment: str = ""
    delta_properties: dict[str, str] = field(default_factory=dict)
    rules: list[DQRule] = field(default_factory=list)

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

    @property
    def expected_delta_properties(self) -> dict[str, str]:
        """The expected Delta Table properties.
        Additional properties override default properties.
        """
        return {**self.default_delta_properties, **self.delta_properties}

    @property
    def primary_key_column_names(self) -> list[str]:
        """Names of primary keys."""
        return [column.name for column in self.columns if column.is_primary_key]

    def ensure(self, spark: SparkSession) -> None:
        """Ensure the table exists with the correct features."""
        DeltaTableManager(delta_table=self).ensure(spark)

    def check_exists(self, spark: SparkSession) -> bool:
        """Checks if the table already exists."""
        return spark.catalog.tableExists(self.full_name)

    def overwrite(self, dataframe: DataFrame) -> None:
        """Overwrite the table with the given dataframe."""
        DeltaWriter(delta_table=self, dataframe=dataframe).overwrite()
