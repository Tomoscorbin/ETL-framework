from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

import pyspark.sql.types as T

from src.logger import LOGGER
from src.table_management.actions import (
    AlignTable,
    ColumnAdd,
    ColumnDrop,
    ColumnNullabilityChange,
    CreateTable,
    DropPrimaryKey,
    Plan,
    SetColumnComments,
    SetPrimaryKey,
    SetTableComment,
    SetTableProperties,
)
from src.table_management.models import Column, Table
from src.table_management.state.snapshot import CatalogState, ColumnState, TableState


class Planner:
    """
    Compute a plan to create or align Delta tables so that the ACTUAL catalog
    matches the DESIRED models.
    """

    def plan(self, desired_tables: Sequence[Table], actual_catalog_state: CatalogState) -> Plan:
        create_actions = []
        align_actions = []

        for desired_table in desired_tables:
            actual_table_state = actual_catalog_state.get(
                desired_table.catalog_name,
                desired_table.schema_name,
                desired_table.table_name,
            )
            full_name = f"{desired_table.catalog_name}.{desired_table.schema_name}.{desired_table.table_name}"

            if actual_table_state is None or not actual_table_state.exists:
                LOGGER.info(f"Plan: CREATE {full_name}")
                create_action = self._build_create_action(desired_table)
                create_actions.append(create_action)
            else:
                LOGGER.info(f"Plan: ALIGN {full_name}")
                align_action = self._build_align_action(desired_table, actual_table_state)
                align_actions.append(align_action)

        return Plan(create_tables=create_actions, align_tables=align_actions)


    def _build_create_action(self, desired_table: Table) -> CreateTable:
        desired_columns = desired_table.columns
        desired_schema_struct = self._struct_from_desired_columns(desired_columns)
        desired_table_comment = desired_table.comment or ""
        desired_table_properties = dict(desired_table.table_properties)
        desired_primary_key_columns = self._desired_primary_key_columns(desired_columns)
        desired_column_comments = self._desired_column_comments(desired_columns)

        return CreateTable(
            catalog_name=desired_table.catalog_name,
            schema_name=desired_table.schema_name,
            table_name=desired_table.table_name,
            schema_struct=desired_schema_struct,
            table_comment=desired_table_comment,
            table_properties=desired_table_properties,
            primary_key_columns=desired_primary_key_columns,
            column_comments=desired_column_comments,
        )
    
    def _build_align_action(self, desired_table: Table, actual_table_state: TableState) -> AlignTable:
        desired_columns = desired_table.columns
        actual_columns = actual_table_state.columns

        columns_to_add = self._columns_to_add(
            desired_columns=desired_columns,
            actual_columns=actual_columns,
        )
        nullability_changes = self._nullability_changes(
            desired_columns=desired_columns,
            actual_columns=actual_columns,
        )
        set_column_comments = self._column_comments_change(
            desired_columns=desired_columns,
            actual_columns=actual_columns,
        )
        set_table_comment = self._table_comment_change(
            desired_comment=desired_table.comment,
            actual_comment=actual_table_state.table_comment,
        )
        set_table_properties = self._table_properties_change(
            desired_properties=desired_table.table_properties,
            actual_properties=actual_table_state.table_properties,
        )

        desired_pk_cols = [c.name for c in desired_columns if c.is_primary_key]
        actual_pk_cols  = list(actual_table_state.primary_key_columns)
        set_primary_key, drop_primary_key = self._primary_key_change(
            desired_pk_cols=desired_pk_cols,
            actual_pk_cols=actual_pk_cols,
        )

        columns_to_drop = self.columns_to_drop(desired_columns, actual_columns)


        return AlignTable(
            catalog_name=desired_table.catalog_name,
            schema_name=desired_table.schema_name,
            table_name=desired_table.table_name,
            set_column_comments=set_column_comments,
            set_table_comment=set_table_comment,
            set_table_properties=set_table_properties,
            drop_primary_key=drop_primary_key,
            set_primary_key=set_primary_key,
            add_columns=columns_to_add,
            change_nullability=nullability_changes,
            drop_columns=columns_to_drop
        )
    
    def _struct_from_desired_columns(self, desired_columns: Sequence[Column]) -> T.StructType:
        return T.StructType(
            [
                T.StructField(
                    name=desired_column.name,
                    dataType=desired_column.data_type,
                    nullable=desired_column.is_nullable,
                )
                for desired_column in desired_columns
            ]
        )
    
    def _desired_primary_key_columns(self, desired_columns: Sequence[Column]) -> List[str]:
        return [c.name for c in desired_columns if c.is_primary_key]
    
    def _desired_column_comments(self, desired_columns: Sequence[Column]) -> Dict[str, str]:
        return {c.name: (c.comment or "") for c in desired_columns}
    
    def _columns_to_add(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> List[ColumnAdd]:
        actual_names = {c.name for c in actual_columns}
        additions = []

        for desired in desired_columns:
            is_missing = desired.name not in actual_names
            if not is_missing:
                continue
            additions.append(
                ColumnAdd(
                    name=desired.name,
                    data_type=desired.data_type,
                    is_nullable=desired.is_nullable,
                    comment=desired.comment or "",
                )
            )

        return additions
    
    def _columns_to_drop(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> List[ColumnDrop]:
        desired_names = {c.name for c in desired_columns}
        drops = []
        for actual in actual_columns:
            if actual.name not in desired_names:
                drops.append(ColumnDrop(name=actual.name))
        return drops
    
    def _nullability_changes(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> List[ColumnNullabilityChange]:
        actual_by_name = {c.name: c for c in actual_columns}
        changes = []

        for desired in desired_columns:
            actual = actual_by_name.get(desired.name)
            if actual is None:
                continue
            if actual.is_nullable != desired.is_nullable:
                changes.append(
                    ColumnNullabilityChange(
                        name=desired.name,
                        make_nullable=desired.is_nullable,  # True => DROP; False => SET
                    )
                )

        return changes
    
    def _column_comments_change(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> SetColumnComments | None:
        desired_by_name = {c.name: c for c in desired_columns}
        actual_by_name = {c.name: c for c in actual_columns}

        changed = {}
        for column_name, desired in desired_by_name.items():
            desired_comment = desired.comment or ""
            actual_comment = (actual_by_name.get(column_name).comment if column_name in actual_by_name else "") or ""
            if desired_comment != actual_comment:
                changed[column_name] = desired_comment

        return SetColumnComments(comments=changed) if changed else None
    
    def _table_comment_change(
        self,
        desired_comment: str,
        actual_comment: str,
    ) -> SetTableComment | None:
        desired_value = desired_comment or ""
        actual_value = actual_comment or ""
        return SetTableComment(comment=desired_value) if desired_value != actual_value else None

    def _table_properties_change(
        self,
        desired_properties: Dict[str, str],
        actual_properties: Dict[str, str],
    ) -> SetTableProperties | None:
        to_set = {}
        for key, desired_value in desired_properties.items():
            actual_value = actual_properties.get(key)
            if actual_value != desired_value:
                to_set[key] = desired_value

        return SetTableProperties(properties=to_set) if to_set else None


    def _primary_key_change(
        self,
        desired_pk_cols: Sequence[str],
        actual_pk_cols: Sequence[str],
    ) -> Tuple[SetPrimaryKey | None, DropPrimaryKey | None]:
        """
        Rules:
        - No actual, no desired         -> (None, None)
        - Actual exists, desired empty  -> (None, DropPrimaryKey())
        - Desired exists, actual empty  -> (SetPrimaryKey(desired), None)
        - Both exist:
            * if same columns in same order -> (None, None)
            * else                          -> (SetPrimaryKey(desired), DropPrimaryKey())
        """
        has_actual = bool(actual_pk_cols)
        has_desired = bool(desired_pk_cols)

        if not has_actual and not has_desired:
            return None, None

        if has_actual and not has_desired:
            # Remove PK entirely
            return None, DropPrimaryKey()

        if has_desired and not has_actual:
            # Fresh PK; no need to drop anything
            return SetPrimaryKey(columns=list(desired_pk_cols)), None

        # Both exist: compare exactly (order-sensitive)
        if list(desired_pk_cols) == list(actual_pk_cols):
            return None, None

        # Any change (add/remove/reorder) -> drop old, then set new
        return SetPrimaryKey(columns=list(desired_pk_cols)), DropPrimaryKey()