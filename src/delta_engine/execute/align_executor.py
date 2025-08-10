"""
Executes ALTER TABLE alignment actions in Delta Lake.

This module defines `AlignExecutor`, which applies `AlignTable` actions
by adding or dropping columns, updating nullability, setting comments,
and applying table properties.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import AlignTable

from .ddl import DeltaDDL


class AlignExecutor:
    """Executes an `AlignTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor."""
        self.ddl = DeltaDDL(spark)

    def apply(self, action: AlignTable) -> None:
        """
        Apply an ALTER TABLE alignment action.

        Steps:
          1. Add columns (always nullable at add-time).
          2. Drop columns.
          3. Adjust column nullability.
          4. Update column comments.
          5. Update table comment.
          6. Update table properties.

        """
        full = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

        # add columns (always nullable at add-time)
        for add in action.add_columns:
            self.ddl.add_column(full, add.name, add.data_type, add.comment)

        # drop columns
        if action.drop_columns:
            self.ddl.drop_columns(full, [d.name for d in action.drop_columns])

        # nullability tweaks
        for ch in action.change_nullability:
            self.ddl.set_column_nullability(full, ch.name, ch.make_nullable)

        # column comments
        if action.set_column_comments:
            for col, cmt in action.set_column_comments.comments.items():
                self.ddl.set_column_comment(full, col, cmt)

        # table comment
        if action.set_table_comment:
            self.ddl.set_table_comment(full, action.set_table_comment.comment)

        # table properties
        if action.set_table_properties:
            self.ddl.set_table_properties(full, action.set_table_properties.properties)
