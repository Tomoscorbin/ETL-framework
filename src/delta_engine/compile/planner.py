"""
Plan schema changes for Delta tables.

Compares desired table models to current catalog state and produces a `TablePlan`
consisting of `CreateTable` and `AlignTable` actions. Plans are intended to be:
- Idempotent: executing a produced plan should bring the catalog to the desired state.
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


class Planner:
    """Compares desired table definitions with actual state and builds a change plan."""

    def plan(self, desired_tables: Sequence[Table], catalog_state: CatalogState) -> TablePlan:
        """
        Build a change plan that makes the live catalog match the desired table models.

        For each desired table, this compares the model against the snapshot in `catalog_state`.
        - If the table does not exist, it emits a `CreateTable` action.
        - If the table exists, it emits an `AlignTable` action capturing column adds/drops,
        nullability changes, comment/property updates, and primary-key drop/add if needed.

        The resulting `TablePlan` is idempotent: applying it once should bring the catalog
        into alignment with the models.
        """
        return self._build_plan(desired_tables, catalog_state)

    def _build_plan(
        self, desired_tables: Sequence[Table], catalog_state: CatalogState
    ) -> TablePlan:
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

        return TablePlan(
            create_tables=tuple(create_actions),
            align_tables=tuple(align_actions),
        )

    # ---------- create ----------

    def _build_create(self, desired: Table) -> CreateTable:
        """Build a `CreateTable` action from the desired model."""
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
        """Convert desired columns to a Spark StructType for CREATE TABLE."""
        fields = []
        for col in desired_columns:
            fields.append(
                T.StructField(name=col.name, dataType=col.data_type, nullable=col.is_nullable)
            )
        return T.StructType(fields)

    def _map_column_comments(self, desired_columns: Sequence[Column]) -> dict[str, str]:
        """Map column name → normalized comment (empty string means no comment)."""
        mapping: dict[str, str] = {}
        for col in desired_columns:
            mapping[col.name] = self._normalize_comment(col.comment)
        return mapping

    # ---------- align ----------

    def _build_align(self, desired: Table, actual: TableState) -> AlignTable:
        """Build an `AlignTable` action by diffing desired vs actual."""
        additions = self._compute_columns_to_add(desired.columns, actual.columns)
        drops = self._compute_columns_to_drop(desired.columns, actual.columns)
        nullables = self._compute_nullability_changes(desired.columns, actual.columns)
        col_notes = self._compute_column_comment_updates(desired.columns, actual.columns)
        tbl_note = self._compute_table_comment_update(desired.comment, actual.table_comment)
        tbl_props = self._compute_table_property_updates(
            desired.effective_table_properties, actual.table_properties
        )
        desired_definition = self._desired_pk_definition(desired)
        drop_pk = self._compute_primary_key_drop(desired_definition, actual.primary_key)
        add_pk = self._compute_primary_key_add(desired_definition, actual.primary_key)

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
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> tuple[ColumnAdd, ...]:
        """Columns present in desired but missing in actual."""
        actual_columns_by_name = {c.name: c for c in actual_columns}
        additions: list[ColumnAdd] = []

        for desired in desired_columns:
            if desired.name not in actual_columns_by_name:
                additions.append(self._build_column_add(desired))

        return tuple(additions)

    def _build_column_add(self, desired: Column) -> ColumnAdd:
        """Build a `ColumnAdd` exactly as requested by the model."""
        return ColumnAdd(
            name=desired.name,
            data_type=desired.data_type,
            is_nullable=desired.is_nullable,
            comment=self._normalize_comment(desired.comment),
        )

    # ----- column drop -----

    def _compute_columns_to_drop(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> tuple[ColumnDrop, ...]:
        """Calculate which columns are present in actual but not in desired."""
        desired_column_names = {c.name for c in desired_columns}
        drops: list[ColumnDrop] = []

        for actual in actual_columns:
            if actual.name not in desired_column_names:
                drops.append(ColumnDrop(name=actual.name))

        return tuple(drops)

    # ----- nullability -----

    def _compute_nullability_changes(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> tuple[ColumnNullabilityChange, ...]:
        """Plan nullability changes for columns that exist in both desired and actual."""
        actual_columns_by_name = {c.name: c for c in actual_columns}
        changes: list[ColumnNullabilityChange] = []

        for desired in desired_columns:
            actual = actual_columns_by_name.get(desired.name)
            if actual is None:
                continue
            if desired.is_nullable != actual.is_nullable:
                changes.append(
                    ColumnNullabilityChange(
                        name=desired.name,
                        make_nullable=desired.is_nullable,
                    )
                )

        return tuple(changes)

    # ----- comments -----

    def _compute_column_comment_updates(
        self,
        desired_columns: Sequence[Column],
        actual_columns: Sequence[ColumnState],
    ) -> SetColumnComments | None:
        """Return `SetColumnComments` when one or more comments differ."""
        actual_columns_by_name = {c.name: c for c in actual_columns}
        changed: dict[str, str] = {}

        for desired in desired_columns:
            desired_comment = self._normalize_comment(desired.comment)
            actual = actual_columns_by_name.get(desired.name)
            actual_comment = self._normalize_comment(actual.comment if actual else "")
            if desired_comment != actual_comment:
                changed[desired.name] = desired_comment

        return SetColumnComments(changed) if changed else None

    def _compute_table_comment_update(
        self,
        desired_comment: str,
        actual_comment: str | None,
    ) -> SetTableComment | None:
        """Return `SetTableComment` if the normalized table comment differs."""
        desired_value = self._normalize_comment(desired_comment)
        actual_value = self._normalize_comment(actual_comment)
        return SetTableComment(desired_value) if desired_value != actual_value else None

    def _normalize_comment(self, comment: str | None) -> str:
        """Normalize a nullable comment to an empty string when absent."""
        return comment or ""

    # ----- properties -----

    def _compute_table_property_updates(
        self,
        desired_properties: Mapping[str, str],
        actual_properties: Mapping[str, str],
    ) -> SetTableProperties | None:
        """Properties to set/overwrite where desired != actual."""
        # TODO: remove properties absent from desired
        properties_to_set: dict[str, str] = {}

        for key, desired in desired_properties.items():
            actual = actual_properties.get(key)
            if desired != actual:
                properties_to_set[key] = desired

        return SetTableProperties(properties_to_set) if properties_to_set else None

        # ----- primary key -----

    def _compute_primary_key_drop(
        self,
        desired_definition: PrimaryKeyDefinition | None,
        actual_primary_key: PrimaryKeyState | None,
    ) -> PrimaryKeyDrop | None:
        """
        Drop when:
        - no desired PK but one exists, or
        - both exist but differ (name or columns, order-sensitive).
        """
        if actual_primary_key is None:
            return None
        if desired_definition is None:
            return PrimaryKeyDrop(name=actual_primary_key.name)
        if not self._primary_key_definitions_equal(desired_definition, actual_primary_key):
            return PrimaryKeyDrop(name=actual_primary_key.name)
        return None

    def _compute_primary_key_add(
        self,
        desired_definition: PrimaryKeyDefinition | None,
        actual_primary_key: PrimaryKeyState | None,
    ) -> PrimaryKeyAdd | None:
        """
        Add when:
        - desired PK exists but none currently, or
        - both exist but differ (name or columns, order-sensitive).
        """
        if desired_definition is None:
            return None
        if actual_primary_key is None:
            return PrimaryKeyAdd(definition=desired_definition)
        if not self._primary_key_definitions_equal(desired_definition, actual_primary_key):
            return PrimaryKeyAdd(definition=desired_definition)
        return None

    def _primary_key_definitions_equal(
        self, desired_definition: PrimaryKeyDefinition, actual_state: PrimaryKeyState
    ) -> bool:
        """Compare ordered columns case-insensitively"""
        def norm(cols: tuple[str, ...]) -> tuple[str, ...]:
            return tuple(c.lower() for c in cols)
        return norm(desired_definition.columns) == norm(actual_state.columns)

    def _desired_pk_definition(self, desired: Table) -> PrimaryKeyDefinition | None:
        """Build the desired PK definition (standardized name) if a PK is declared."""
        columns = getattr(desired, "primary_key", None)
        if not columns:
            return None
        name = build_primary_key_name(
            catalog=desired.catalog_name,
            schema=desired.schema_name,
            table=desired.table_name,
            columns=tuple(columns),
        )
        return PrimaryKeyDefinition(name=name, columns=tuple(columns))
