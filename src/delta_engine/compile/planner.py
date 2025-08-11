"""
Plans schema changes for Delta tables.

This module defines `TablePlanner`, which compares desired table models against
the current catalog state and produces a `TablePlan` of `CreateTable` and
`AlignTable` actions to bring the catalog into alignment.
"""

from collections.abc import Mapping, Sequence

import pyspark.sql.types as T

from src.delta_engine.actions import (
    AlignTable,
    ColumnAdd,
    ColumnDrop,
    ColumnNullabilityChange,
    CreateTable,
    PrimaryKeyAdd,
    PrimaryKeyDefinition,
    PrimaryKeyDrop,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
    TablePlan,
)
from src.delta_engine.constraints.naming import build_primary_key_name
from src.delta_engine.models import Column, Table
from src.delta_engine.state.states import CatalogState, ColumnState, PrimaryKeyState, TableState


class TablePlanner:
    """Compares desired table definitions with actual state and builds a change plan."""

    def build_plan(self, desired_tables: Sequence[Table], catalog_state: CatalogState) -> TablePlan:
        """Compare desired tables against the current catalog state and produce a plan."""
        create_actions: list[CreateTable] = []
        align_actions: list[AlignTable] = []

        for desired in desired_tables:
            actual = catalog_state.get(
                desired.catalog_name, desired.schema_name, desired.table_name
            )
            if actual is None or not actual.exists:
                create_actions.append(self._build_create(desired))
            else:
                align_actions.append(self._build_align(desired, actual))

        return TablePlan(create_tables=create_actions, align_tables=align_actions)

    # ---------- create ----------

    def _build_create(self, desired: Table) -> CreateTable:
        schema_struct = self._to_struct(desired.columns)
        table_comment = self._normalize_comment(desired.comment)
        table_properties = dict(desired.effective_table_properties)
        column_comments = self._map_column_comments(desired.columns)
        primary_key = self._desired_pk_definition(desired)
        return CreateTable(
            catalog_name=desired.catalog_name,
            schema_name=desired.schema_name,
            table_name=desired.table_name,
            schema_struct=schema_struct,
            table_comment=table_comment,
            table_properties=table_properties,
            column_comments=column_comments,
            primary_key=primary_key,
        )

    def _to_struct(self, desired_columns: Sequence[Column]) -> T.StructType:
        fields = []
        for col in desired_columns:
            fields.append(
                T.StructField(name=col.name, dataType=col.data_type, nullable=col.is_nullable)
            )
        return T.StructType(fields)

    def _map_column_comments(self, desired_columns: Sequence[Column]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for col in desired_columns:
            mapping[col.name] = self._normalize_comment(col.comment)
        return mapping

    # ---------- align ----------

    def _build_align(self, desired: Table, actual: TableState) -> AlignTable:
        additions = self._compute_columns_to_add(desired.columns, actual.columns)
        drops = self._compute_columns_to_drop(desired.columns, actual.columns)
        nullables = self._compute_nullability_changes(desired.columns, actual.columns)
        col_notes = self._compute_column_comment_updates(desired.columns, actual.columns)
        tbl_note = self._compute_table_comment_update(desired.comment, actual.table_comment)
        tbl_props = self._compute_table_property_updates(
            desired.effective_table_properties, actual.table_properties
        )
        drop_pk, add_pk = self._compute_primary_key_changes(desired, actual)

        if add_pk:
            # Tighten every PK column unless already explicitly make_nullable=False
            planned = {c.name.lower(): c for c in nullables}
            for col in add_pk.definition.columns:
                key = col.lower()
                if planned.get(key) is None or planned[key].make_nullable is not False:
                    nullables.append(
                        ColumnNullabilityChange(name=col, make_nullable=False)
                    )

        return AlignTable(
            catalog_name=desired.catalog_name,
            schema_name=desired.schema_name,
            table_name=desired.table_name,
            add_columns=additions,
            drop_columns=drops,
            change_nullability=nullables,
            set_column_comments=col_notes,
            set_table_comment=tbl_note,
            set_table_properties=tbl_props,
            drop_primary_key=drop_pk,
            add_primary_key=add_pk,
        )

    # ----- column add -----

    def _compute_columns_to_add(
        self, desired_columns: Sequence[Column], actual_columns: Sequence[ColumnState]
    ) -> list[ColumnAdd]:
        actual_names = self._actual_column_name_set(actual_columns)
        additions: list[ColumnAdd] = []

        for desired in desired_columns:
            if self._is_missing_in_actual(desired.name, actual_names):
                additions.append(self._build_column_add(desired))

        return additions

    def _actual_column_name_set(self, actual_columns: Sequence[ColumnState]) -> set[str]:
        names: set[str] = set()
        for c in actual_columns:
            names.add(c.name)
        return names

    def _is_missing_in_actual(self, column_name: str, actual_names: set[str]) -> bool:
        return column_name not in actual_names

    def _build_column_add(self, desired: Column) -> ColumnAdd:
        return ColumnAdd(
            name=desired.name,
            data_type=desired.data_type,
            is_nullable=True,  # force nullable on ADD
            comment=self._normalize_comment(desired.comment),
        )

    # ----- column drop -----

    def _compute_columns_to_drop(
        self, desired_columns: Sequence[Column], actual_columns: Sequence[ColumnState]
    ) -> list[ColumnDrop]:
        desired_names = self._desired_column_name_set(desired_columns)
        drops: list[ColumnDrop] = []

        for actual in actual_columns:
            if self._is_extra_in_actual(actual.name, desired_names):
                drops.append(ColumnDrop(name=actual.name))

        return drops

    def _desired_column_name_set(self, desired_columns: Sequence[Column]) -> set[str]:
        names: set[str] = set()
        for c in desired_columns:
            names.add(c.name)
        return names

    def _is_extra_in_actual(self, column_name: str, desired_names: set[str]) -> bool:
        return column_name not in desired_names

    # ----- nullability -----

    def _compute_nullability_changes(
        self, desired_columns: Sequence[Column], actual_columns: Sequence[ColumnState]
    ) -> list[ColumnNullabilityChange]:
        actual_by_name = self._index_actual_by_name(actual_columns)
        changes: list[ColumnNullabilityChange] = []

        for desired in desired_columns:
            actual = actual_by_name.get(desired.name)
            if actual is None:
                # New column: we add as NULLABLE; if model wants NOT NULL, tighten now.
                if desired.is_nullable is False:
                    changes.append(
                        ColumnNullabilityChange(name=desired.name, make_nullable=False)
                    )
                continue

            # Existing column: emit change when desired != actual
            if desired.is_nullable != actual.is_nullable:
                changes.append(
                    ColumnNullabilityChange(name=desired.name, make_nullable=desired.is_nullable)
                )

        return changes

    def _index_actual_by_name(
        self, actual_columns: Sequence[ColumnState]
    ) -> dict[str, ColumnState]:
        return {c.name: c for c in actual_columns}

    def _nullability_differs(self, desired_is_nullable: bool, actual_is_nullable: bool) -> bool:
        return desired_is_nullable != actual_is_nullable

    # ----- comments -----

    def _compute_column_comment_updates(
        self, desired_columns: Sequence[Column], actual_columns: Sequence[ColumnState]
    ) -> SetColumnComments | None:
        actual_by_name = self._index_actual_by_name(actual_columns)
        changed: dict[str, str] = {}

        actual_by_name = self._index_actual_by_name(actual_columns)
        for desired in desired_columns:
            desired_comment = self._normalize_comment(desired.comment)
            actual = actual_by_name.get(desired.name)
            actual_comment = self._normalize_comment(actual.comment if actual else "")
            if desired_comment != actual_comment:
                changed[desired.name] = desired_comment

        return SetColumnComments(changed) if changed else None

    def _compute_table_comment_update(
        self, desired_comment: str, actual_comment: str
    ) -> SetTableComment | None:
        desired_value = self._normalize_comment(desired_comment)
        actual_value = self._normalize_comment(actual_comment)
        return SetTableComment(desired_value) if desired_value != actual_value else None

    def _normalize_comment(self, comment: str | None) -> str:
        return comment or ""

    # ----- properties -----

    def _compute_table_property_updates(
        self, desired_properties: Mapping[str, str], actual_properties: Mapping[str, str]
    ) -> SetTableProperties | None:
        to_set: dict[str, str] = {}

        for key, desired_value in desired_properties.items():
            actual_value = actual_properties.get(key)
            if self._property_differs(desired_value, actual_value):
                to_set[key] = desired_value

        return SetTableProperties(to_set) if to_set else None

    def _property_differs(self, desired_value: str, actual_value: str | None) -> bool:
        return actual_value != desired_value

        # ----- primary key -----

    def _compute_primary_key_changes(
        self, desired: Table, actual: TableState
    ) -> tuple[PrimaryKeyDrop | None, PrimaryKeyAdd | None]:
        desired_definition = self._desired_pk_definition(desired)
        actual_state = actual.primary_key

        drop_action = self._compute_primary_key_drop(desired_definition, actual_state)
        add_action = self._compute_primary_key_add(desired_definition, actual_state)

        return drop_action, add_action

    def _compute_primary_key_drop(
        self,
        desired_definition: PrimaryKeyDefinition | None,
        actual_state: PrimaryKeyState | None,
    ) -> PrimaryKeyDrop | None:
        """
        Drop when:
        - no desired PK but one exists, or
        - both exist but differ.
        """
        if actual_state is None:
            return None
        if desired_definition is None:
            return PrimaryKeyDrop(name=actual_state.name)
        if not self._primary_key_definitions_equal(desired_definition, actual_state):
            return PrimaryKeyDrop(name=actual_state.name)
        return None

    def _compute_primary_key_add(
        self,
        desired_definition: PrimaryKeyDefinition | None,
        actual_state: PrimaryKeyState | None,
    ) -> PrimaryKeyAdd | None:
        """
        Add when:
        - desired PK exists but none currently, or
        - both exist but differ.
        """
        if desired_definition is None:
            return None
        if actual_state is None:
            return PrimaryKeyAdd(definition=desired_definition)
        if not self._primary_key_definitions_equal(desired_definition, actual_state):
            return PrimaryKeyAdd(definition=desired_definition)
        return None

    def _primary_key_definitions_equal(
        self, desired_definition: PrimaryKeyDefinition, actual_state: PrimaryKeyState
    ) -> bool:
        """Order-sensitive equality on name and columns."""
        names_match = desired_definition.name == actual_state.name
        columns_match = desired_definition.columns == actual_state.columns
        return names_match and columns_match

    def _desired_pk_definition(self, desired: Table) -> PrimaryKeyDefinition | None:
        cols = getattr(desired, "primary_key", None)
        if not cols:
            return None
        name = build_primary_key_name(
            catalog=desired.catalog_name,
            schema=desired.schema_name,
            table=desired.table_name,
            columns=cols,
        )
        return PrimaryKeyDefinition(name=name, columns=cols)
