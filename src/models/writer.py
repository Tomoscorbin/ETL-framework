from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from src.models.data_quality_handler import DQHandler

if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DeltaWriter:
    """Delta Table writer."""

    def __init__(self, delta_table: "DeltaTable", dataframe: DataFrame):
        self.delta_table = delta_table
        self.dataframe = dataframe

    def _apply_and_save_checks(self):
        DQHandler(self.delta_table, self.dataframe).apply_and_save_checks()

    def overwrite(self) -> None:
        """Overwrite the table with the given dataframe."""
        self._apply_and_save_checks()
        self.dataframe.select(self.delta_table.column_names).write.saveAsTable(
            name=self.delta_table.full_name, mode="overwrite", format="delta"
        )
