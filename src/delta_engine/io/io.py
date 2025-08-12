"""TableIO module for reading and writing Delta tables."""

from pyspark.sql import DataFrame, SparkSession

from src.delta_engine.models import Table
from src.logger import LOGGER


class TableIO:
    """Provides methods for reading from and writing to tables."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize TableIO with a Spark session."""
        self.spark = spark

    def read(self, table: Table) -> DataFrame:
        """Read the specified table into a DataFrame."""
        return self.spark.table(table.full_name)

    def overwrite(self, table: Table, dataframe: DataFrame) -> None:
        """Overwrite the given table with the given dataframe."""
        (
            dataframe.select(table.column_names).write.saveAsTable(
                name=table.full_name,
                mode="overwrite",
                format="delta",
            )
        )
        LOGGER.info(f"Table {table.full_name} overwritten.")
