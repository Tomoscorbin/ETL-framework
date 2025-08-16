"""
Diff engine: desired spec + live state -> concrete plan actions.

Principles
----------
- Small helpers per concern (properties, table comment, columns, PK).
- No side effects; this module only computes actions.
- Tri-state semantics:
  - desired.table_comment is None  → unmanaged (no action)
  - desired.table_comment == ""    → clear
  - desired.table_comment == "x"   → set to "x"
  - desired.table_properties is None → unmanaged (no action)
  - desired.table_properties is dict → enforce exactly those (executor may whitelist)
- Column comments: only set when a non-empty desired comment differs from live.
- No ad-hoc name assembly; use identifier helpers.

Output
------
Flat list of `Action` objects (see plan/actions.py). PlanBuilder orders them.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.identifiers import FullyQualifiedTableName, build_primary_key_name
from src.delta_engine.models import Column
from src.delta_engine.plan.actions import (
    Action,
    AddColumns,
    AddPrimaryKey,
    AlignTable,
    CreateTable,
    DropColumns,
    DropPrimaryKey,
    SetColumnComments,
    SetColumnNullability,
    SetTableComment,
    SetTableProperties,
)
from src.delta_engine.state.states import CatalogState, ColumnState, TableState

# ---------- options ----------


@dataclass(frozen=True)
class DiffOptions:
    """Feature switches for which aspects to manage."""

    manage_schema: bool = True  # columns + nullability + column comments
    manage_table_comment: bool = True
    manage_properties: bool = True
    manage_primary_key: bool = True


# ---------- public API ----------


class Differ:
    """
    Compute differences between desired and live catalog state.

    The `Differ` produces a list of `Action` objects representing
    the changes required to reconcile the live state with the desired
    specification.

    Workflow
    --------
    1. For each desired table, compare against the corresponding live table.
    2. If the table does not exist, generate a full CREATE action.
    3. Otherwise, compute granular diffs (properties, comments, schema, PK).
    4. Coalesce granular diffs into an `AlignTable` action, if needed.

    """
    def diff(
        self, desired: DesiredCatalog, live: CatalogState, options: DiffOptions
    ) -> list[Action]:
        """Compute all actions needed to align desired vs. live catalog state."""
        return self._diff_catalog(desired, live, options)

    def _diff_catalog(
        self,
        desired: DesiredCatalog,
        live: CatalogState,
        options: DiffOptions,
    ) -> list[Action]:
        actions: list[Action] = []
        for desired_table in desired.tables:
            full_table_name = desired_table.full_table_name
            live_table = live.tables.get(full_table_name)
            actions.extend(self._diff_table(desired_table, live_table, options))
        return actions

    @staticmethod
    def _diff_table(
        desired: DesiredTable,
        live: TableState | None,
        options: DiffOptions,
    ) -> list[Action]:
        # 1) create from scratch
        if live is None or not live.exists:
            return [_create_from_scratch(desired)]

        # 2) collect granular changes
        sub_actions: list[object] = []

        if options.manage_properties:
            sub_actions.extend(_diff_properties(desired, live))
        if options.manage_table_comment:
            sub_actions.extend(_diff_table_comment(desired, live))
        if options.manage_schema:
            sub_actions.extend(_diff_columns(desired, live))
        if options.manage_primary_key:
            sub_actions.extend(_diff_primary_key(desired, live))

        # 3) coalesce → AlignTable (or nothing)
        align = _coalesce_to_align(desired.full_table_name, sub_actions)
        return [align] if align is not None else []


# ---------- aspect helpers ----------


def _create_from_scratch(desired: DesiredTable) -> CreateTable:
    """
    Build a composed CreateTable action for a single desired table.

    Semantics:
      - Table comment:
          None  -> omit COMMENT clause
          ""    -> clear comment
          "x"   -> set to "x"
      - Table properties:
          None  -> unmanaged (omit)
          dict  -> pass through (even empty {}) to executor
      - Primary key:
          None / empty -> omit
          non-empty    -> add with deterministic/override name
    """
    # 1) required schema (always present on create)
    columns_tuple: tuple[Column, ...] = tuple(desired.columns)
    add_columns = AddColumns(columns=columns_tuple)

    # 2) optional table comment (tri-state)
    set_table_comment: SetTableComment | None = (
        SetTableComment(comment=(desired.table_comment or ""))
        if desired.table_comment is not None
        else None
    )

    # 3) optional table properties (tri-state: None => unmanaged)
    set_table_properties: SetTableProperties | None = (
        SetTableProperties(properties=dict(desired.table_properties))
        if desired.table_properties is not None
        else None
    )

    # 4) optional primary key
    add_primary_key: AddPrimaryKey | None = None
    if desired.primary_key_columns and len(desired.primary_key_columns) > 0:
        pk_columns = tuple(desired.primary_key_columns)
        pk_name = desired.primary_key_name_override or build_primary_key_name(
            catalog_name=desired.full_table_name.catalog,
            schema_name=desired.full_table_name.schema,
            table_name=desired.full_table_name.table,
            columns=pk_columns,
        )
        add_primary_key = AddPrimaryKey(name=pk_name, columns=pk_columns)

    return CreateTable(
        full_table_name=desired.full_table_name,
        add_columns=add_columns,
        set_table_comment=set_table_comment,
        set_table_properties=set_table_properties,
        add_primary_key=add_primary_key,
    )


def _diff_properties(desired: DesiredTable, live: TableState) -> list[Action]:
    """Compare desired vs live properties. None means 'unmanaged' (no action)."""
    desired_props = desired.table_properties
    live_props = live.properties  # TableState uses `properties`

    if desired_props is None:
        return []

    # We replace wholesale to the desired mapping (executor may whitelist keys).
    # Early-exit if already equal to avoid unnecessary writes.
    if dict(desired_props) == dict(live_props):
        return []

    return [SetTableProperties(properties=dict(desired_props))]


def _diff_table_comment(desired: DesiredTable, live: TableState) -> list[Action]:
    """
    Compare desired vs live table comment. None means 'unmanaged' (no action).
    Empty string means 'clear comment'.
    """
    if desired.table_comment is None:
        return []
    desired_comment = desired.table_comment or ""  # allow clearing via ""
    live_comment = live.comment or ""
    if desired_comment != live_comment:
        return [SetTableComment(comment=desired_comment)]
    return []


def _diff_columns(desired: DesiredTable, live: TableState) -> list[object]:
    """
    Return granular sub-actions 
    (AddColumns, DropColumns, SetColumnNullability, SetColumnComments).
    """
    sub_actions: list[object] = []
    live_by_lower: dict[str, ColumnState] = {c.name.lower(): c for c in live.columns}

    desired_columns: tuple[Column, ...] = tuple(desired.columns)

    # 1) Adds
    desired_missing = _compute_missing_columns(desired_columns, live_by_lower)
    if desired_missing:
        sub_actions.append(AddColumns(columns=tuple(desired_missing)))

    # 2) Drops
    desired_lower = {c.name.lower() for c in desired_columns}
    live_extras = tuple(c.name for c in live.columns if c.name.lower() not in desired_lower)
    if live_extras:
        sub_actions.append(DropColumns(columns=live_extras))

    # 3) Nullability deltas
    sub_actions.extend(_compute_nullability_actions(desired_columns, live_by_lower))

    # 4) Column comments (only set when desired has a non-empty comment and it differs)
    comment_updates = _compute_column_comment_updates(desired_columns, live_by_lower)
    if comment_updates:
        sub_actions.append(SetColumnComments(comments=comment_updates))

    return sub_actions


def _diff_primary_key(desired: DesiredTable, live: TableState) -> list[Action]:
    """
    PK semantics:
      - desired.primary_key_columns is None  => unmanaged (no action)
      - desired.primary_key_columns == ()    => ensure NO PK (drop if exists)
      - desired.primary_key_columns non-empty => ensure PK with those columns
        - name = desired.primary_key_name_override or derived default
    """
    cols = desired.primary_key_columns
    live_pk = live.primary_key

    # unmanaged
    if cols is None:
        return []

    # enforce no PK
    if len(cols) == 0:
        return [DropPrimaryKey(name=live_pk.name)] if live_pk is not None else []

    # enforce PK with derived/override name
    desired_name = desired.primary_key_name_override or build_primary_key_name(
        catalog_name=desired.full_table_name.catalog,
        schema_name=desired.full_table_name.schema,
        table_name=desired.full_table_name.table,
        columns=cols,
    )

    # live has no PK -> create
    if live_pk is None:
        return [AddPrimaryKey(name=desired_name, columns=tuple(cols))]

    # live has PK -> compare name + ordered columns
    if desired_name != live_pk.name or tuple(cols) != tuple(live_pk.columns):
        return [
            DropPrimaryKey(name=live_pk.name),
            AddPrimaryKey(name=desired_name, columns=tuple(cols)),
        ]
    return []


def _coalesce_to_align(
    full_table_name: FullyQualifiedTableName,
    actions: Iterable[object],  # sub-action payloads (not top-level Action)
) -> AlignTable | None:
    """
    Fold a sequence of granular sub-actions into one AlignTable.

    Rules:
    - AddColumns: merge into a single payload (append order preserved).
    - SetColumnNullability: accumulate as-is (one per column).
    - SetColumnComments: merged; later entries override earlier ones.
    - SetTableComment / SetTableProperties: last writer wins (there should be at most one of each).
    - AddPrimaryKey / DropPrimaryKey: keep the last seen of each kind.
    - If nothing to do, return None.
    """
    add_columns_buf: list[Column] = []
    drop_columns_buf: list[str] = []
    nullability_buf: list[SetColumnNullability] = []
    comments_buf: dict[str, str] = {}

    table_comment_payload: SetTableComment | None = None
    table_props_payload: SetTableProperties | None = None
    add_pk_payload: AddPrimaryKey | None = None
    drop_pk_payload: DropPrimaryKey | None = None

    for a in actions:
        if isinstance(a, AddColumns):
            add_columns_buf.extend(a.columns)
        elif isinstance(a, DropColumns):
            drop_columns_buf.extend(a.columns)
        elif isinstance(a, SetColumnNullability):
            nullability_buf.append(a)
        elif isinstance(a, SetColumnComments):
            # merge; later wins
            comments_buf.update(dict(a.comments))
        elif isinstance(a, SetTableComment):
            table_comment_payload = a  # last writer wins
        elif isinstance(a, SetTableProperties):
            table_props_payload = a  # last writer wins
        elif isinstance(a, DropPrimaryKey):
            drop_pk_payload = a  # last writer wins
        elif isinstance(a, AddPrimaryKey):
            add_pk_payload = a  # last writer wins
        else:
            continue  # ignore unknown items

    add_columns_payload: AddColumns | None = (
        AddColumns(columns=tuple(add_columns_buf)) if add_columns_buf else None
    )
    drop_columns_payload: DropColumns | None = (
        DropColumns(columns=tuple(drop_columns_buf)) if drop_columns_buf else None
    )
    set_col_comments_payload: SetColumnComments | None = (
        SetColumnComments(comments=comments_buf) if comments_buf else None
    )

    has_any = any(
        [
            add_columns_payload is not None,
            drop_columns_payload is not None,
            bool(nullability_buf),
            set_col_comments_payload is not None,
            table_comment_payload is not None,
            table_props_payload is not None,
            drop_pk_payload is not None,
            add_pk_payload is not None,
        ]
    )
    if not has_any:
        return None

    return AlignTable(
        full_table_name=full_table_name,
        add_columns=add_columns_payload,
        drop_columns=drop_columns_payload,
        set_nullability=tuple(nullability_buf),
        set_column_comments=set_col_comments_payload,
        set_table_comment=table_comment_payload,
        set_table_properties=table_props_payload,
        add_primary_key=add_pk_payload,
        drop_primary_key=drop_pk_payload,
    )


# ---------- utilities ----------


def _compute_missing_columns(
    desired_columns: Sequence[Column],
    live_by_lower: dict[str, ColumnState],
) -> list[Column]:
    """Return desired Column objects that are not present in live."""
    missing: list[Column] = []
    for column in desired_columns:
        if column.name.lower() not in live_by_lower:
            missing.append(column)
    return missing


def _compute_nullability_actions(
    desired_columns: Sequence[Column],
    live_by_lower: dict[str, ColumnState],
) -> list[SetColumnNullability]:
    """
    Generate one SetColumnNullability action per column 
    where desired nullability differs from live.
    """
    actions: list[SetColumnNullability] = []
    for column in desired_columns:
        live_col = live_by_lower.get(column.name.lower())
        if live_col is None:
            continue
        desired_is_nullable = bool(column.is_nullable)
        live_is_nullable = bool(live_col.is_nullable)
        if desired_is_nullable != live_is_nullable:
            actions.append(
                SetColumnNullability(
                    column_name=column.name,
                    make_nullable=desired_is_nullable,
                )
            )
    return actions


def _compute_column_comment_updates(
    desired_columns: Sequence[Column],
    live_by_lower: dict[str, ColumnState],
) -> dict[str, str]:
    """
    Return {column_name -> desired_comment} for columns where desired
    has a non-empty comment that differs from live.
    """
    updates: dict[str, str] = {}
    for column in desired_columns:
        if not column.comment:
            continue
        live_col = live_by_lower.get(column.name.lower())
        live_comment = "" if live_col is None else (live_col.comment or "")
        desired_comment = column.comment or ""
        if desired_comment != live_comment:
            updates[column.name] = desired_comment
    return updates
