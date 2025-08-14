"""
Diff engine: desired spec + live state -> concrete plan actions.

Principles
----------
- Explicit helpers per concern (properties, table comment, columns, PK).
- No side effects; this module only computes actions.
- Tri-state semantics:
  - desired.table_comment is None  → unmanaged (no action)
  - desired.table_comment == ""    → clear
  - desired.table_comment == "x"   → set to "x"
  - desired.table_properties is None → unmanaged (no action)
  - desired.table_properties is dict → enforce exactly those (executor may whitelist)
- Column comments are set only when a non-empty desired comment differs from live.
- No ad-hoc name assembly; use identifier helpers.

Output
------
Flat list of `Action` objects (see plan/actions.py). PlanBuilder will order them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from src.delta_engine.identifiers import (
    fully_qualified_name_to_string,
    FullyQualifiedTableName,
)
from src.delta_engine.identifiers import build_primary_key_name
from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.models import Column
from src.delta_engine.state.states import CatalogState, TableState, ColumnState
from src.delta_engine.plan.actions import (
    Action,
    ColumnSpec,
    CreateTable,
    AddColumns,
    AlterColumnNullability,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
    CreatePrimaryKey,
    DropPrimaryKey,
)


# ---------- options ----------

@dataclass(frozen=True)
class DiffOptions:
    """
    Feature switches for which aspects to manage.
    Set a flag to False to skip producing actions for that aspect.
    """
    manage_schema: bool = True            # columns + nullability + column comments
    manage_table_comment: bool = True
    manage_properties: bool = True
    manage_primary_key: bool = True


# ---------- public API ----------

def diff_catalog(
    desired: DesiredCatalog,
    live: CatalogState,
    options: DiffOptions = DiffOptions(),
) -> list[Action]:
    """Compute actions for every desired table against the live catalog state."""
    actions: list[Action] = []
    for desired_table in desired.tables:
        live_key = fully_qualified_name_to_string(desired_table.fully_qualified_table_name)
        live_table_state = live.tables.get(live_key)
        actions.extend(diff_table(desired_table, live_table_state, options))
    return actions


def diff_table(
    desired: DesiredTable,
    live: TableState | None,
    options: DiffOptions,
) -> list[Action]:
    """Compute actions to make a single live table match the desired spec."""
    # Table does not exist → create with everything we know up-front
    if live is None or not live.exists:
        return [_create_from_scratch(desired)]

    out: list[Action] = []

    if options.manage_properties:
        out.extend(_diff_properties(desired, live))

    if options.manage_table_comment:
        out.extend(_diff_table_comment(desired, live))

    if options.manage_schema:
        out.extend(_diff_columns(desired, live))

    if options.manage_primary_key:
        out.extend(_diff_primary_key(desired, live))

    return out


# ---------- aspect helpers ----------

def _create_from_scratch(desired: DesiredTable) -> CreateTable:
    """
    CreateTable action with all columns, properties, and table comment.
    (Executor will apply any whitelisting of properties.)
    """
    column_specs = tuple(_to_column_spec(c) for c in desired.columns)
    properties_dict = {} if desired.table_properties is None else dict(desired.table_properties)
    # Pass-through tri-state comment (None => no COMMENT clause on create)
    table_comment_value = desired.table_comment

    return CreateTable(
        fully_qualified_table_name=desired.fully_qualified_table_name,
        columns=column_specs,
        properties=properties_dict,
        comment=table_comment_value,
    )


def _diff_properties(desired: DesiredTable, live: TableState) -> list[Action]:
    """Compare desired vs live properties. None means 'unmanaged' (no action)."""
    if desired.table_properties is None:
        return []
    desired_props = {str(k): str(v) for k, v in dict(desired.table_properties).items()}
    live_props = {str(k): str(v) for k, v in dict(live.table_properties).items()}
    if desired_props != live_props:
        return [SetTableProperties(table=desired.fully_qualified_table_name, properties=desired_props)]
    return []


def _diff_table_comment(desired: DesiredTable, live: TableState) -> list[Action]:
    """
    Compare desired vs live table comment. None means 'unmanaged' (no action).
    Empty string means 'clear comment'.
    """
    if desired.table_comment is None:
        return []
    desired_comment = desired.table_comment or ""  # allow clearing via ""
    live_comment = live.table_comment or ""
    if desired_comment != live_comment:
        return [SetTableComment(table=desired.fully_qualified_table_name, comment=desired_comment)]
    return []


def _diff_columns(desired: DesiredTable, live: TableState) -> list[Action]:
    """
    Handle:
    - Adding missing columns (batched into a single AddColumns action).
    - Nullability changes (one AlterColumnNullability per column).
    - Column comments (SetColumnComments for only the ones that changed and are non-empty).
    """
    actions: list[Action] = []

    live_by_lower: Dict[str, ColumnState] = {c.name.lower(): c for c in live.columns}

    # 1) Missing columns → build specs one by one, then aggregate
    add_specs = _compute_add_column_specs(desired.columns, live_by_lower)
    if add_specs:
        actions.append(AddColumns(table=desired.fully_qualified_table_name, columns=tuple(add_specs)))

    # 2) Nullability changes
    actions.extend(_compute_nullability_actions(desired.columns, desired.fully_qualified_table_name, live_by_lower))

    # 3) Column comments (only set when desired has a non-empty comment and it differs)
    comment_updates = _compute_column_comment_updates(desired.columns, live_by_lower)
    if comment_updates:
        actions.append(SetColumnComments(table=desired.fully_qualified_table_name, comments=comment_updates))

    return actions


def _diff_primary_key(desired: DesiredTable, live: TableState) -> list[Action]:
    """
    Semantics:
      - desired.primary_key_columns is None  => unmanaged (no action)
      - desired.primary_key_columns == ()    => ensure NO PK (drop if exists)
      - desired.primary_key_columns non-empty => ensure PK with those columns
        - name = desired.primary_key_name_override or derived default
    """
    cols = desired.primary_key_columns
    live_pk = live.primary_key

    # unmanaged (skip)
    if cols is None:
        return []

    # enforce no PK
    if len(cols) == 0:
        return [DropPrimaryKey(table=desired.fully_qualified_table_name)] if live_pk is not None else []

    # enforce PK with derived/override name
    desired_name = desired.primary_key_name_override or build_primary_key_name(
        catalog=desired.fully_qualified_table_name.catalog,
        schema=desired.fully_qualified_table_name.schema,
        table=desired.fully_qualified_table_name.table,
        columns=cols,
    )

    # live has no PK -> create
    if live_pk is None:
        return [CreatePrimaryKey(table=desired.fully_qualified_table_name, name=desired_name, columns=tuple(cols))]

    # live has PK -> compare name + ordered columns
    if desired_name != live_pk.name or tuple(cols) != tuple(live_pk.columns):
        return [
            DropPrimaryKey(table=desired.fully_qualified_table_name),
            CreatePrimaryKey(table=desired.fully_qualified_table_name, name=desired_name, columns=tuple(cols)),
        ]
    return []


# ---------- utilities ----------

def _to_column_spec(desired_column: Column) -> ColumnSpec:
    """Construct a single ColumnSpec from a desired Column."""
    return ColumnSpec(
        name=desired_column.name,
        data_type=desired_column.data_type,
        is_nullable=desired_column.is_nullable,
        comment=desired_column.comment or "",
    )


def _compute_add_column_specs(
    desired_columns: Tuple[Column, ...],
    live_by_lower: Dict[str, ColumnState],
) -> list[ColumnSpec]:
    """For each desired column that does not exist live, build a ColumnSpec."""
    specs: list[ColumnSpec] = []
    for col in desired_columns:
        if col.name.lower() in live_by_lower:
            continue
        specs.append(_to_column_spec(col))
    return specs


def _compute_nullability_actions(
    desired_columns: Tuple[Column, ...],
    table_name: FullyQualifiedTableName,
    live_by_lower: Dict[str, ColumnState],
) -> list[AlterColumnNullability]:
    """
    Generate one AlterColumnNullability action per column where desired nullability
    differs from live.
    """
    actions: list[AlterColumnNullability] = []
    for col in desired_columns:
        live_col = live_by_lower.get(col.name.lower())
        if live_col is None:
            continue
        desired_is_nullable = bool(col.is_nullable)
        live_is_nullable = bool(live_col.is_nullable)
        if desired_is_nullable != live_is_nullable:
            actions.append(
                AlterColumnNullability(
                    table=table_name,
                    column_name=col.name,
                    make_nullable=desired_is_nullable,
                )
            )
    return actions


def _compute_column_comment_updates(
    desired_columns: Tuple[Column, ...],
    live_by_lower: Dict[str, ColumnState],
) -> dict[str, str]:
    """
    Return {column_name -> desired_comment} for columns where desired
    has a non-empty comment that differs from live.
    """
    updates: dict[str, str] = {}
    for col in desired_columns:
        if not col.comment:
            continue
        live_col = live_by_lower.get(col.name.lower())
        live_comment = "" if live_col is None else (live_col.comment or "")
        desired_comment = col.comment or ""
        if desired_comment != live_comment:
            updates[col.name] = desired_comment
    return updates
