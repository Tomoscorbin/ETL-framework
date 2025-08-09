from __future__ import annotations
from src.delta_engine.actions import (
    AlignTable, CreateTable, ColumnAdd, ColumnDrop, ColumnNullabilityChange,
    SetPrimaryKey, DropPrimaryKey, SetColumnComments, SetTableComment, SetTableProperties
)
from src.delta_engine.models import Table
from .diffs import TableDiff

class TablePlanner:
    """No business rules. Only converts a TableDiff into your action dataclasses."""

    def assemble(self, desired: Table, diff: TableDiff):
        if diff.is_create:
            return CreateTable(
                catalog_name=desired.catalog_name,
                schema_name=desired.schema_name,
                table_name=desired.table_name,
                schema_struct=diff.create_schema,  # already built
                table_comment=diff.create_table_comment,
                table_properties=diff.create_table_properties,
                primary_key_columns=list(diff.create_primary_key_columns),
                column_comments=diff.create_column_comments,
            )

        set_pk  = SetPrimaryKey(columns=list(diff.primary_key.set_columns)) if diff.primary_key.set_columns else None
        drop_pk = DropPrimaryKey() if diff.primary_key.drop else None

        set_col_comments = SetColumnComments(comments=diff.comments.column_comments) if diff.comments.column_comments else None
        set_tbl_comment  = SetTableComment(comment=diff.comments.table_comment) if diff.comments.table_comment else None
        set_props        = SetTableProperties(properties=diff.properties.to_set) if diff.properties.to_set else None

        return AlignTable(
            catalog_name=desired.catalog_name,
            schema_name=desired.schema_name,
            table_name=desired.table_name,
            set_column_comments=set_col_comments,
            set_table_comment=set_tbl_comment,
            set_table_properties=set_props,
            drop_primary_key=drop_pk,
            set_primary_key=set_pk,
            add_columns=[ColumnAdd(a.name, a.data_type, a.is_nullable, a.comment) for a in diff.columns_to_add],
            change_nullability=[ColumnNullabilityChange(c.name, c.make_nullable) for c in diff.nullability_changes],
            drop_columns=[ColumnDrop(d.name) for d in diff.columns_to_drop],
        )
