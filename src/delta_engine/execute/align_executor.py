from __future__ import annotations

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.execute.ddl_executor import DDLExecutor
from src.delta_engine.plan.actions import (
    AlignTable,
    AddColumns,
    AlterColumnNullability,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
    AddPrimaryKey,
    DropPrimaryKey,
)
from src.delta_engine.models import Column


class AlignExecutor:
    """
    Execute a single per-table alignment action in a deterministic, safe order:

      0) Drop PRIMARY KEY (if requested) — unblocks schema edits
      1) Add columns
      2) Alter column nullability
      3) Add PRIMARY KEY (if requested)
      4) Set column comments
      5) Set table comment
      6) Set table properties
    """

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: AlignTable) -> None:
        table: FullyQualifiedTableName = action.table

        # 0) constraints (drop first)
        self._drop_primary_key(table, action.drop_primary_key)

        # 1) schema adds
        self._add_columns(table, action.add_columns)

        # 2) nullability
        self._alter_nullability(table, action.alter_nullability)

        # 3) constraints (add after schema changes)
        self._add_primary_key(table, action.add_primary_key)

        # 4) comments (columns)
        self._set_column_comments(table, action.set_column_comments)

        # 5) comment (table)
        self._set_table_comment(table, action.set_table_comment)

        # 6) properties
        self._set_table_properties(table, action.set_table_properties)

    # ----- tiny helpers -----

    def _add_columns(self, table: FullyQualifiedTableName, add: AddColumns | None) -> None:
        if not add or not add.columns:
            return
        for col in add.columns:
            self._ddl.add_column(
                full_table_name=table,
                column_name=col.name,
                data_type=col.data_type,
                is_nullable=col.is_nullable,
                comment=(col.comment or None),
            )

    def _alter_nullability(
        self,
        table: FullyQualifiedTableName,
        items: tuple[AlterColumnNullability, ...] | None,
    ) -> None:
        if not items:
            return
        for change in items:
            self._ddl.set_column_nullability(
                full_table_name=table,
                column_name=change.column_name,
                make_nullable=change.make_nullable,
            )

    def _set_column_comments(
        self,
        table: FullyQualifiedTableName,
        set_comments: SetColumnComments | None,
    ) -> None:
        if not set_comments or not set_comments.comments:
            return
        for name, comment in set_comments.comments.items():
            self._ddl.set_column_comment(
                full_table_name=table,
                column_name=name,
                comment=comment,
            )

    def _set_table_comment(
        self,
        table: FullyQualifiedTableName,
        set_comment: SetTableComment | None,
    ) -> None:
        if set_comment is None:
            return
        self._ddl.set_table_comment(
            full_table_name=table,
            comment=set_comment.comment,
        )

    def _set_table_properties(
        self,
        table: FullyQualifiedTableName,
        set_props: SetTableProperties | None,
    ) -> None:
        if not set_props or not set_props.properties:
            return
        self._ddl.set_table_properties(
            full_table_name=table,
            properties=set_props.properties,
        )

    def _add_primary_key(
        self,
        table: FullyQualifiedTableName,
        add_pk: AddPrimaryKey | None,
    ) -> None:
        if not add_pk:
            return
        self._ddl.add_primary_key(
            full_table_name=table,
            constraint_name=add_pk.name,
            column_names=add_pk.columns,
        )

    def _drop_primary_key(
        self,
        table: FullyQualifiedTableName,
        drop_pk: DropPrimaryKey | None,
    ) -> None:
        if not drop_pk:
            return
        self._ddl.drop_primary_key(
            full_table_name=table,
            constraint_name=drop_pk.name,
        )
