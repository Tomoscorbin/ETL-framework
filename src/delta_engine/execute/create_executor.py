from __future__ import annotations

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.execute.ddl_executor import DDLExecutor
from src.delta_engine.plan.actions import (
    CreateTable,
    SetTableProperties,
    SetTableComment,
    AddPrimaryKey,
)
from src.delta_engine.models import Column


class CreateExecutor:
    """
    Execute a `CreateTable` action in a deterministic order:

      1) CREATE TABLE IF NOT EXISTS (schema + optional table comment)
      2) SET TBLPROPERTIES (if provided)
      3) Set per-column comments (non-empty only; idempotent safety)
      4) ADD CONSTRAINT PRIMARY KEY (if provided)
    """

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: CreateTable) -> None:
        """Apply a CreateTable action."""
        table: FullyQualifiedTableName = action.table
        columns: tuple[Column, ...] = action.add_columns.columns

        # 1) create skeleton (columns + optional table comment)
        comment_value = self._comment_or_none(action.set_table_comment)
        self._ensure_table_exists(table, columns, comment_value)

        # 2) properties (optional)
        self._apply_table_properties(table, action.set_table_properties)

        # 3) column comments (non-empty only)
        self._apply_column_comments(table, columns)

        # 4) primary key (optional)
        self._apply_primary_key(table, action.add_primary_key)

    # ----- tiny helpers -----

    def _ensure_table_exists(
        self,
        table: FullyQualifiedTableName,
        columns: tuple[Column, ...],
        table_comment: str | None,
    ) -> None:
        self._ddl.create_table_if_not_exists(
            full_table_name=table,
            columns=columns,
            table_comment=table_comment,
        )

    def _apply_table_properties(
        self,
        table: FullyQualifiedTableName,
        set_props: SetTableProperties | None,
    ) -> None:
        if set_props and set_props.properties:
            self._ddl.set_table_properties(
                full_table_name=table,
                properties=set_props.properties,
            )

    def _apply_column_comments(
        self,
        table: FullyQualifiedTableName,
        columns: tuple[Column, ...],
    ) -> None:
        for col in columns:
            if col.comment:
                self._ddl.set_column_comment(
                    full_table_name=table,
                    column_name=col.name,
                    comment=col.comment,
                )

    def _apply_primary_key(
        self,
        table: FullyQualifiedTableName,
        add_pk: AddPrimaryKey | None,
    ) -> None:
        if add_pk:
            self._ddl.add_primary_key(
                full_table_name=table,
                constraint_name=add_pk.name,
                column_names=add_pk.columns,
            )

    @staticmethod
    def _comment_or_none(set_comment: SetTableComment | None) -> str | None:
        """
        Extract the comment string to pass to CREATE:
          - None => omit COMMENT clause
          - ""   => COMMENT '' (clear)
          - "x"  => COMMENT 'x'
        """
        if set_comment is None:
            return None
        return set_comment.comment
