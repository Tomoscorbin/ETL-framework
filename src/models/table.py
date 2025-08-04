from dataclasses import dataclass, field
from typing import Callable, Iterable, List, ClassVar, TypeAlias

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.rule import DQRule  # type: ignore
from src.enums import DeltaTableProperty
from src.models.column import DeltaColumn
from src.models.table_manager import DeltaTableManager
from src.models.writer import DeltaWriter
from src.logger import LOGGER


RuleBuilder: TypeAlias = Callable[[DeltaTable], Iterable[DQRule]]


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

    def read(self, spark: SparkSession) -> DataFrame:
        """Read the DeltaTable as a Spark DataFrame."""
        return spark.table(self.full_name)

    def overwrite(self, dataframe: DataFrame) -> None:
        """Overwrite the table with the given dataframe."""
        DeltaWriter(delta_table=self, dataframe=dataframe).overwrite()

    def merge(self, dataframe: DataFrame) -> None:
        """Merge the given dataframe into table."""
        DeltaWriter(delta_table=self, dataframe=dataframe).merge()



@dataclass(frozen=True)
class QualityAwareDeltaTable(DeltaTable):
    """
    DeltaTable that auto-derives DQx rules from column metadata.
    Callers can still pass `rules=` explicitly; duplicates are ignored.
    """

    # registry of plug-in builder functions
    _builders: ClassVar[List[RuleBuilder]] = []

    @classmethod
    def register_builder(cls, fn: RuleBuilder) -> RuleBuilder:
        """Decorator: `@QualityAwareDeltaTable.register_builder`."""
        cls._builders.append(fn)
        return fn

    def __post_init__(self) -> None:
        # we’re frozen, so we use object.__setattr__
        auto_rules = []
        for build in self._builders:
            try:
                auto_rules.extend(build(self))
            except Exception as exc:     # bad builder must NOT kill the table
                LOGGER.error(f"Rule builder {build.__name__} failed: {exc}")

        # de-dupe: caller-supplied rules win
        combined = self.rules + [
            r for r in auto_rules if r not in self.rules
        ]
        object.__setattr__(self, "rules", combined)