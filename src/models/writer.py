from pyspark.sql import DataFrame
from src.models.table import DeltaTable, SparkSession

class DeltaWriter:
  def __init__(self, delta_table: DeltaTable, dataframe: DataFrame):
    self.delta_table = delta_table
    self.dataframe = dataframe

  def overwrite(self) -> None:
    """Overwrite the table with the given dataframe."""
    self.dataframe.select(self.delta_table.column_names).write.saveAsTable(
        name=self.delta_table.full_name,
        mode="overwrite",
        format="delta"
    )