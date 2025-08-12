"""
Execute CREATE TABLE actions in Delta Lake.

`CreateExecutor` applies a `CreateTable` action by:
  1) Creating the table with schema and (optional) table comment if it does not exist,
  2) Setting table properties,
  3) Applying column comments,
  4) Adding the PRIMARY KEY (if specified).

Execution is idempotent with respect to existing objects: re-running yields the same state.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import CreateTable
from src.delta_engine.constraints.naming import three_part_to_qualified_name
from src.delta_engine.execute.ddl import DeltaDDL


class CreateExecutor:
    """Executes a `CreateTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor."""
        self._ddl = DeltaDDL(spark)

    def apply(self, action: CreateTable) -> None:
        """Apply a `CreateTable` action in a deterministic order."""
        table = three_part_to_qualified_name(
            (action.catalog_name, action.schema_name, action.table_name)
        )

        self._ensure_table_exists(table, action)
        self._apply_table_properties(table, action)
        self._apply_column_comments(table, action)
        self._apply_primary_key(table, action)

    # ----- steps -----

    def _ensure_table_exists(self, table: str, action: CreateTable) -> None:
        # Create skeleton with schema + table comment (comment may be empty string)
        self._ddl.create_table_if_not_exists(table, action.schema_struct, action.table_comment)

    def _apply_table_properties(self, table: str, action: CreateTable) -> None:
        if action.table_properties:
            self._ddl.set_table_properties(table, action.table_properties)

    def _apply_column_comments(self, table: str, action: CreateTable) -> None:
        # Only set non-empty comments on create; empty strings mean "no comment".
        for column_name, comment in (action.column_comments or {}).items():
            if comment:
                self._ddl.set_column_comment(table, column_name, comment)

    def _apply_primary_key(self, table: str, action: CreateTable) -> None:
        if action.primary_key:
            self._ddl.add_primary_key(
                table,
                action.primary_key.name,
                list(action.primary_key.columns),
            )
