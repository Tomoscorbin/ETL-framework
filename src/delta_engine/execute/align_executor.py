"""
Executes ALTER TABLE alignment actions in Delta Lake.

`AlignExecutor` applies an `AlignTable` action by (in order):
  0) Dropping the PRIMARY KEY if requested (to unblock schema edits),
  1) Adding columns (with the nullability requested by the action),
  2) Dropping columns,
  3) Updating column nullability,
  4) Adding the PRIMARY KEY if requested,
  5) Updating column comments,
  6) Updating the table comment,
  7) Applying table properties.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import AlignTable
from src.delta_engine.constraints.naming import three_part_to_qualified_name
from src.delta_engine.execute.ddl import DeltaDDL


class AlignExecutor:
    """Executes an `AlignTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor."""
        self.ddl = DeltaDDL(spark)

    def apply(self, action: AlignTable) -> None:
        """Apply an `AlignTable` action in a deterministic, safe order."""
        qualified_table_name = three_part_to_qualified_name(
            (action.catalog_name, action.schema_name, action.table_name)
        )

        self._drop_pk_if_needed(qualified_table_name, action)
        self._add_columns(qualified_table_name, action)
        self._drop_columns(qualified_table_name, action)
        self._apply_nullability_changes(qualified_table_name, action)
        self._add_pk_if_needed(qualified_table_name, action)
        self._apply_column_comments(qualified_table_name, action)
        self._apply_table_comment(qualified_table_name, action)
        self._apply_table_properties(qualified_table_name, action)

    # ----- steps -----

    def _drop_pk_if_needed(self, table: str, action: AlignTable) -> None:
        if action.drop_primary_key:
            self.ddl.drop_primary_key(table, action.drop_primary_key.name)

    def _add_columns(self, table: str, action: AlignTable) -> None:
        for add in action.add_columns:
            self.ddl.add_column(
                table,
                add.name,
                add.data_type,
                add.comment,
            )

    def _drop_columns(self, table: str, action: AlignTable) -> None:
        if action.drop_columns:
            self.ddl.drop_columns(table, [d.name for d in action.drop_columns])

    def _apply_nullability_changes(self, table: str, action: AlignTable) -> None:
        for change in action.change_nullability:
            self.ddl.set_column_nullability(table, change.name, change.make_nullable)

    def _add_pk_if_needed(self, table: str, action: AlignTable) -> None:
        if action.add_primary_key:
            self.ddl.add_primary_key(
                table,
                action.add_primary_key.definition.name,
                list(action.add_primary_key.definition.columns),
            )

    def _apply_column_comments(self, table: str, action: AlignTable) -> None:
        if action.set_column_comments:
            for column_name, comment in action.set_column_comments.comments.items():
                self.ddl.set_column_comment(table, column_name, comment)

    def _apply_table_comment(self, table: str, action: AlignTable) -> None:
        if action.set_table_comment:
            self.ddl.set_table_comment(table, action.set_table_comment.comment)

    def _apply_table_properties(self, table: str, action: AlignTable) -> None:
        if action.set_table_properties:
            self.ddl.set_table_properties(table, action.set_table_properties.properties)