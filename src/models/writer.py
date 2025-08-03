from typing import TYPE_CHECKING

from delta import DeltaTable as dt
from pyspark.sql import DataFrame

from src.models.data_quality_handler import DQHandler
from src.logger import LOGGER


if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DeltaWriter:
    """Handles standardized writing of DataFrames to Delta Tables."""

    def __init__(self, delta_table: "DeltaTable", dataframe: DataFrame):
        self.delta_table = delta_table
        self.dataframe = dataframe

    @property
    def merge_condition(self) -> str:
        """
        Creates a string representing the merge condition for the Delta Table
        using its primary key.
        """
        primary_keys = self.delta_table.primary_key_column_names
        return " AND ".join(f"t.{pk} = s.{pk}" for pk in primary_keys)

    def _apply_and_save_checks(self) -> None:
        DQHandler(self.delta_table, self.dataframe).apply_and_save_checks()

    def overwrite(self) -> None:
        """Overwrite the table with the given dataframe."""
        self._apply_and_save_checks()
        self.dataframe.select(self.delta_table.column_names).write.saveAsTable(
            name=self.delta_table.full_name, mode="overwrite", format="delta"
        )
        LOGGER.info(f"Table {self.delta_table.full_name} overwritten.")

    def merge(self) -> None:
        """
        Upsert rows from `self.dataframe` into the target Delta table.
        *   Uses the table's primary-key columns to match.
        *   Updates all columns when matched, inserts all columns when not matched.
        """
        if not self.delta_table.primary_key_column_names:
            raise ValueError(
                f"Cannot merge into {self.delta_table.full_name}: no primary keys defined."
            )

        self._apply_and_save_checks()

        target = dt.forName(self.dataframe.sparkSession, self.delta_table.full_name)
        source = self.dataframe.select(self.delta_table.column_names)
        (
            target.alias("t")
            .merge(
                source=source.alias("s"),
                condition=self.merge_condition,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        LOGGER.info(f"Table {self.delta_table.full_name} merged.")
