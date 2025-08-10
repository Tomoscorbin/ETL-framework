from pyspark.sql import SparkSession, DataFrame
from src.delta_engine.models.table import Table

class TableIO:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, table: Table) -> DataFrame:
        return self.spark.table(table.full_name)

    def overwrite(self, table: Table, dataframe: DataFrame) -> None:
        """Overwrite the table with the given dataframe."""
        (
            dataframe
            .select(table.column_names)
            .write
            .saveAsTable(
                name=table.full_name, 
                mode="overwrite", 
                format="delta",
            )
        )
        LOGGER.info(f"Table {table.full_name} overwritten.")