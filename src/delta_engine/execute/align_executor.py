from __future__ import annotations
from pyspark.sql import SparkSession

from src.delta_engine.identifiers import fully_qualified_name_to_string
from src.delta_engine.execute.ddl_executor import DDLExecutor
from src.delta_engine.plan.actions import (
    Action,
    AddColumns,
    AlterColumnNullability,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
    CreatePrimaryKey,
    DropPrimaryKey,
    CreateTable,  # only to ignore here
)

class AlignExecutor:
    """Executes all non-create actions by dispatching per action type."""

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: Action) -> None:
        if isinstance(action, AddColumns):
            self._apply_add_columns(action)
            return
        if isinstance(action, AlterColumnNullability):
            self._apply_nullability(action)
            return
        if isinstance(action, SetColumnComments):
            self._apply_column_comments(action)
            return
        if isinstance(action, SetTableComment):
            self._apply_table_comment(action)
            return
        if isinstance(action, SetTableProperties):
            self._apply_table_properties(action)
            return
        if isinstance(action, CreatePrimaryKey):
            self._apply_add_pk(action)
            return
        if isinstance(action, DropPrimaryKey):
            self._apply_drop_pk(action)
            return

        raise TypeError(f"AlignExecutor cannot handle action type: {type(action).__name__}")

    # ---- handlers ----

    def _apply_add_columns(self, action: AddColumns) -> None:
        table = fully_qualified_name_to_string(action.table)
        for spec in action.columns:  # ColumnSpec (name, data_type, is_nullable, comment)
            self._ddl.add_column(
                table_name=table,
                column_name=spec.name,
                data_type=spec.data_type,
                is_nullable=spec.is_nullable,
                comment=(spec.comment or None),
            )

    def _apply_nullability(self, action: AlterColumnNullability) -> None:
        table = fully_qualified_name_to_string(action.table)
        self._ddl.set_column_nullability(
            table_name=table,
            column_name=action.column_name,
            make_nullable=action.make_nullable,
        )

    def _apply_column_comments(self, action: SetColumnComments) -> None:
        table = fully_qualified_name_to_string(action.table)
        for column_name, comment in action.comments.items():
            self._ddl.set_column_comment(
                table_name=table,
                column_name=column_name,
                comment=comment,
            )

    def _apply_table_comment(self, action: SetTableComment) -> None:
        table = fully_qualified_name_to_string(action.table)
        self._ddl.set_table_comment(table_name=table, comment=action.comment)

    def _apply_table_properties(self, action: SetTableProperties) -> None:
        table = fully_qualified_name_to_string(action.table)
        self._ddl.set_table_properties(table_name=table, properties=action.properties)

    def _apply_add_pk(self, action: CreatePrimaryKey) -> None:
        table = fully_qualified_name_to_string(action.table)
        self._ddl.add_primary_key(
            table_name=table,
            constraint_name=action.name,
            columns=list(action.columns),
        )

    def _apply_drop_pk(self, action: DropPrimaryKey) -> None:
        table = fully_qualified_name_to_string(action.table)
        self._ddl.drop_primary_key(
            table_name=table,
            constraint_name=action.name,
        )
