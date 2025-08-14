from __future__ import annotations

from pyspark.sql import SparkSession
import pyspark.sql.types as T

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.exec.ddl import DDLExecutor
from src.delta_engine.plan.actions import AlignTable


class AlignExecutor:
    """Executes an `AlignTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: AlignTable) -> None:
        """Apply in a deterministic, safe order."""
        fully_qualified_table_name = FullyQualifiedTableName(action.catalog_name, action.schema_name, action.table_name)

        self._drop_pk_if_needed(fully_qualified_table_name, action)
        self._add_columns(fully_qualified_table_name, action)
        self._drop_columns(fully_qualified_table_name, action)
        self._apply_nullability_changes(fully_qualified_table_name, action)
        self._add_pk_if_needed(fully_qualified_table_name, action)
        self._apply_column_comments(fully_qualified_table_name, action)
        self._apply_table_comment(fully_qualified_table_name, action)
        self._apply_table_properties(fully_qualified_table_name, action)

    # ----- steps -----

    def _drop_pk_if_needed(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        drop_pk = getattr(action, "drop_primary_key", None)
        if drop_pk:
            self._ddl.drop_primary_key(fully_qualified_table_name, drop_pk.name)

    def _add_columns(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        for add in (getattr(action, "add_columns", []) or []):
            self._ddl.add_column(
                fully_qualified_table_name,
                add.name,
                add.data_type,
                add.is_nullable,
                getattr(add, "comment", None),
            )

    def _drop_columns(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        drops = [d.name for d in (getattr(action, "drop_columns", []) or [])]
        if drops:
            self._ddl.drop_columns(fully_qualified_table_name, drops)

    def _apply_nullability_changes(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        for change in (getattr(action, "change_nullability", []) or []):
            self._ddl.set_column_nullability(fully_qualified_table_name, change.name, change.make_nullable)

    def _add_pk_if_needed(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        add_pk = getattr(action, "add_primary_key", None)
        if add_pk:
            definition = add_pk.definition
            self._ddl.add_primary_key(fully_qualified_table_name, definition.name, tuple(definition.columns))

    def _apply_column_comments(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        comments_obj = getattr(action, "set_column_comments", None)
        if comments_obj and comments_obj.comments:
            for column_name, comment in comments_obj.comments.items():
                self._ddl.set_column_comment(fully_qualified_table_name, column_name, comment)

    def _apply_table_comment(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        comment_obj = getattr(action, "set_table_comment", None)
        if comment_obj:
            self._ddl.set_table_comment(fully_qualified_table_name, comment_obj.comment)

    def _apply_table_properties(self, fully_qualified_table_name: FullyQualifiedTableName, action: AlignTable) -> None:
        props_obj = getattr(action, "set_table_properties", None)
        if props_obj and props_obj.properties:
            self._ddl.set_table_properties(fully_qualified_table_name, props_obj.properties)
