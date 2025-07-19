from dataclasses import dataclass, field
from typing import ClassVar, TYPE_CHECKING

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src.enums import DeltaTableProperty

if TYPE_CHECKING:
    from src.models.table_manager import DeltaTableManager


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
        return T.StructField(self.name, self.data_type, True)


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
        DeltaTableManager(delta_table=self).ensure(spark)
    


    # def overwrite(self, df: DataFrame) -> None:
    #     """
    #     Overwrites the Delta Table with the given DataFrame.

    #     Args:
    #         df (DataFrame): DataFrame to overwrite the Delta Table with.

    #     Returns: None
    #     """
    #     df.select(self.column_names).write.saveAsTable(
    #         name=self.full_name, format="delta", mode="overwrite"
    #     )
