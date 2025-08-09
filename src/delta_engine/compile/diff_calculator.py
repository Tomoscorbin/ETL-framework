from __future__ import annotations
from typing import Sequence, List
import pyspark.sql.types as T
from src.delta_engine.models import Table, Column
from src.delta_engine.state.snapshot import TableState, ColumnState
from .diffs import (
    TableDiff, ColumnAddDiff, ColumnDropDiff, NullabilityChangeDiff,
    CommentsDiff, PropertiesDiff
)

class DiffCalculator:
    def diff(self, desired: Table, actual: TableState | None) -> TableDiff:
        if actual is None or not actual.exists:
            return self._create_diff(desired)

        return TableDiff(
            is_create=False,
            columns_to_add=self._columns_to_add(desired.columns, actual.columns),
            columns_to_drop=self._columns_to_drop(desired.columns, actual.columns),
            nullability_changes=self._nullability_changes(desired.columns, actual.columns),
            comments=self._comments_diff(desired, actual),
            properties=self._properties_diff(desired, actual),
        )

    def _create_diff(self, desired: Table) -> TableDiff:
        schema = T.StructType([
            T.StructField(c.name, c.data_type, c.is_nullable) for c in desired.columns
        ])
        return TableDiff(
            is_create=True,
            create_schema=schema,
            create_table_comment=desired.comment or "",
            create_table_properties=dict(desired.effective_table_properties),
            create_column_comments={c.name: (c.comment or "") for c in desired.columns},
        )

    def _columns_to_add(self, desired: Sequence[Column], actual: Sequence[ColumnState]) -> List[ColumnAddDiff]:
        actual_names = {c.name for c in actual}
        return [
            ColumnAddDiff(d.name, d.data_type, d.is_nullable, d.comment or "")
            for d in desired
            if d.name not in actual_names
        ]

    def _columns_to_drop(self, desired: Sequence[Column], actual: Sequence[ColumnState]) -> List[ColumnDropDiff]:
        desired_names = {c.name for c in desired}
        return [ColumnDropDiff(a.name) for a in actual if a.name not in desired_names]

    def _nullability_changes(self, desired: Sequence[Column], actual: Sequence[ColumnState]) -> List[NullabilityChangeDiff]:
        actual_by = {c.name: c for c in actual}
        out: List[NullabilityChangeDiff] = []
        for d in desired:
            a = actual_by.get(d.name)
            if a and a.is_nullable != d.is_nullable:
                out.append(NullabilityChangeDiff(d.name, make_nullable=d.is_nullable))
        return out

    def _comments_diff(self, desired: Table, actual: TableState) -> CommentsDiff:
        desired_table_comment = desired.comment or ""
        actual_table_comment  = actual.table_comment or ""
        table_comment = desired_table_comment if desired_table_comment != actual_table_comment else None

        actual_col_comments = {c.name: (c.comment or "") for c in actual.columns}
        desired_col_comments = {c.name: (c.comment or "") for c in desired.columns}
        changed_cols = {
            k: v for k, v in desired_col_comments.items()
            if actual_col_comments.get(k, "") != v
        }
        return CommentsDiff(table_comment=table_comment, column_comments=changed_cols)

    def _properties_diff(self, desired: Table, actual: TableState) -> PropertiesDiff:
        to_set = {}
        for k, v in desired.effective_table_properties.items():
            if actual.table_properties.get(k) != v:
                to_set[k] = v
        return PropertiesDiff(to_set=to_set)
