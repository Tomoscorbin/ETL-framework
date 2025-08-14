from __future__ import annotations

from pyspark.sql import SparkSession
import pyspark.sql.types as T

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.execute.ddl_executor import DDLExecutor
from src.delta_engine.plan.actions import CreateTable


class CreateExecutor:
    """Executes a `CreateTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: CreateTable) -> None:
        """Apply in deterministic order; re-running yields the same state."""
        fully_qualified_table_name = FullyQualifiedTableName(action.catalog_name, action.schema_name, action.table_name)

        self._ensure_table_exists(fully_qualified_table_name, action.schema_struct, action.table_comment)
        self._apply_table_properties(fully_qualified_table_name, action)
        self._apply_column_comments(fully_qualified_table_name, action)
        self._apply_primary_key(fully_qualified_table_name, action)

    # ----- steps -----

    def _ensure_table_exists(self, fully_qualified_table_name: FullyQualifiedTableName, schema_struct: T.StructType, table_comment: str | None) -> None:
        self._ddl.create_table_if_not_exists(fully_qualified_table_name, schema_struct, table_comment)

    def _apply_table_properties(self, fully_qualified_table_name: FullyQualifiedTableName, action: CreateTable) -> None:
        if getattr(action, "table_properties", None):
            self._ddl.set_table_properties(fully_qualified_table_name, action.table_properties)

    def _apply_column_comments(self, fully_qualified_table_name: FullyQualifiedTableName, action: CreateTable) -> None:
        for column_name, comment in (getattr(action, "column_comments", {}) or {}).items():
            if comment:  # only non-empty on create
                self._ddl.set_column_comment(fully_qualified_table_name, column_name, comment)

    def _apply_primary_key(self, fully_qualified_table_name: FullyQualifiedTableName, action: CreateTable) -> None:
        if getattr(action, "primary_key", None):
            self._ddl.add_primary_key(fully_qualified_table_name, action.primary_key.name, tuple(action.primary_key.columns))
