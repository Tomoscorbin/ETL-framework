"""Representation of a Delta table with helper methods."""

from dataclasses import dataclass, field
from typing import ClassVar

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.rule import DQRule  # type: ignore
from src.enums import DeltaTableProperty
from src.models.column import DeltaColumn, ForeignKey
from src.models.table_builder import DeltaTableBuilder
from src.models.utils import short_hash
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
    foreign_keys: list[ForeignKey] = field(default_factory=list)
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

    @property
    def foreign_key_constraints(self) -> list[dict[str, str]]:
        """
        Build FK constraints from column FKs.
        Returns dicts: constraint_name, source_column, reference_table, reference_column
        """
        constraints: list[dict[str, str]] = []
        used_names: set[str] = set()

        for col in self.columns:
            fk = col.foreign_key
            if not fk:
                continue

            name = fk.constraint_name(self)

            if name in used_names:
                # Disambiguate: stable salt includes table + column info (not visible in base name)
                salt = short_hash(
                    self.full_name,
                    col.name,
                    fk.target_table.full_name,
                    fk.target_column,
                )
                name = fk.constraint_name(self, salt=salt)

            used_names.add(name)

            constraints.append(
                {
                    "constraint_name": name,
                    "source_column": col.name,
                    "reference_table": fk.target_table.full_name,
                    "reference_column": fk.target_column,
                }
            )

        constraints.sort(key=lambda c: (c["constraint_name"], c["reference_table"]))
        return constraints

    # I/O helpers
    def ensure(self, spark: SparkSession) -> None:
        """Ensure the table exists with the correct features."""
        DeltaTableBuilder(delta_table=self).ensure(spark)

    def check_exists(self, spark: SparkSession) -> bool:
        """Checks if the table already exists."""
        return spark.catalog.tableExists(self.full_name)

    def read(self, spark: SparkSession) -> DataFrame:
        """Read the DeltaTable as a Spark DataFrame."""
        return spark.table(self.full_name)

    def overwrite(self, dataframe: DataFrame) -> None:
        """Overwrite the table with the given dataframe."""
        DeltaWriter(delta_table=self, dataframe=dataframe).overwrite()

    def merge(self, dataframe: DataFrame) -> None:
        """Merge the given dataframe into table."""
        DeltaWriter(delta_table=self, dataframe=dataframe).merge()
