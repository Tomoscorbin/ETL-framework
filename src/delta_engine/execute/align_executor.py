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
          0. Drop PRIMARY KEY (if present) — frees schema edits.
          1. Add columns (always nullable at add-time).
          2. Drop columns.
          3. Adjust column nullability.
          4. Add PRIMARY KEY (if requested).
          5. Update column comments.
          6. Update table comment.
          7. Update table properties.

        """
        full = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

        # 0) Drop PK first (unblock column drops / PK reshape)
        if action.drop_primary_key:
            self.ddl.drop_primary_key(full, action.drop_primary_key.name)

        # 1) Add columns (always nullable)
        for add in action.add_columns:
            self.ddl.add_column(full, add.name, add.data_type, add.comment)

        # 2) Drop columns
        if action.drop_columns:
            self.ddl.drop_columns(full, [d.name for d in action.drop_columns])

        # 3) Nullability tweaks
        for ch in action.change_nullability:
            self.ddl.set_column_nullability(full, ch.name, ch.make_nullable)

        # 4) Add PK last (after nullability is tightened)
        if action.add_primary_key:
            self.ddl.add_primary_key(
                full,
                action.add_primary_key.name,
                list(action.add_primary_key.columns),
            )

        # 5) Column comments
        if action.set_column_comments:
            for col, cmt in action.set_column_comments.comments.items():
                self.ddl.set_column_comment(full, col, cmt)

        # 6) Table comment
        if action.set_table_comment:
            self.ddl.set_table_comment(full, action.set_table_comment.comment)

        # 7) Table properties
        if action.set_table_properties:
            self.ddl.set_table_properties(full, action.set_table_properties.properties)
