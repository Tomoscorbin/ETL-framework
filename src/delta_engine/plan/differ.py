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

from dataclasses import dataclass

from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.identifiers import (
    FullyQualifiedTableName,
    build_primary_key_name,
    fully_qualified_name_to_string,
)
from src.delta_engine.models import Column
from src.delta_engine.plan.actions import (
    Action,
    AddColumns,
    AddPrimaryKey,
    AlignTable,
    AlterColumnNullability,
    CreateTable,
    DropColumns,
    DropPrimaryKey,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
)
from src.delta_engine.state.states import CatalogState, ColumnState, TableState

# ---------- options ----------


@dataclass(frozen=True)
class DiffOptions:
    """
    Feature switches for which aspects to manage.
    Set a flag to False to skip producing actions for that aspect.
    """

    manage_schema: bool = True  # columns + nullability + column comments
    manage_table_comment: bool = True
    manage_properties: bool = True
    manage_primary_key: bool = True


# ---------- public API ----------


class Differ:
    def diff(
        self, desired: DesiredCatalog, live: CatalogState, options: DiffOptions
    ) -> list[Action]:
        return self._diff_catalog(desired, live, options)

    def _diff_catalog(
        self,
        desired: DesiredCatalog,
        live: CatalogState,
        options: DiffOptions = DiffOptions(),
    ) -> list[Action]:
        actions: list[Action] = []
        for d in desired.tables:
            live_key = fully_qualified_name_to_string(d.fully_qualified_table_name)
            live_table = live.tables.get(live_key)
            actions.extend(self._diff_table(d, live_table, options))
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
        actions: list[Action] = []

        if options.manage_properties:
            actions.extend(_diff_properties(desired, live))
        if options.manage_table_comment:
            actions.extend(_diff_table_comment(desired, live))
        if options.manage_schema:
            actions.extend(_diff_columns(desired, live))
        if options.manage_primary_key:
            actions.extend(_diff_primary_key(desired, live))

        # 3) coalesce → AlignTable (or nothing)
        align = _coalesce_to_align(desired.fully_qualified_table_name, actions)
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
    set_table_comment: Optional[SetTableComment] = (
        SetTableComment(comment=(desired.table_comment or ""))
        if desired.table_comment is not None
        else None
    )

    # 3) optional table properties (tri-state: None => unmanaged)
    set_table_properties: Optional[SetTableProperties] = (
        SetTableProperties(properties=dict(desired.table_properties))
        if desired.table_properties is not None
        else None
    )

    # 4) optional primary key
    add_primary_key: Optional[AddPrimaryKey] = None
    if desired.primary_key_columns and len(desired.primary_key_columns) > 0:
        pk_cols = tuple(desired.primary_key_columns)
        pk_name = desired.primary_key_name_override or build_primary_key_name(
            catalog=desired.fully_qualified_table_name.catalog,
            schema=desired.fully_qualified_table_name.schema,
            table=desired.fully_qualified_table_name.table,
            columns=pk_cols,
        )
        add_primary_key = AddPrimaryKey(name=pk_name, columns=pk_cols)

    return CreateTable(
        table=desired.fully_qualified_table_name,
        add_columns=add_columns,
        set_table_comment=set_table_comment,
        set_table_properties=set_table_properties,
        add_primary_key=add_primary_key,
    )


def _diff_properties(desired: DesiredTable, live: TableState) -> list[Action]:
    """Compare desired vs live properties. None means 'unmanaged' (no action)."""
    desired = desired.table_properties
    live = live.table_properties

    if desired is None:
        return []

    mismatches = {k: v for k, v in desired.items() if live.get(k) != v}
    if not mismatches:
        return []

    return [SetTableProperties(properties=desired)]


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
        return [SetTableComment(comment=desired_comment)]
    return []


def _diff_columns(desired: DesiredTable, live: TableState) -> list[object]:
    """Return granular sub-actions (AddColumns, DropColumns, AlterColumnNullability, SetColumnComments)."""
    actions: list[object] = []
    live_by_lower: dict[str, ColumnState] = {c.name.lower(): c for c in live.columns}

    # 1) Adds
    desired_missing = [c for c in desired.columns if c.name.lower() not in live_by_lower]
    if desired_missing:
        actions.append(AddColumns(columns=tuple(desired_missing)))

    # 2) Drops
    desired_lower = {c.name.lower() for c in desired.columns}
    live_extras = tuple(c.name for c in live.columns if c.name.lower() not in desired_lower)
    if live_extras:
        actions.append(DropColumns(columns=live_extras))

    # 3) Nullability deltas
    actions.extend(_compute_nullability_actions(desired.columns, live_by_lower))

    # 4) Column comments
    comment_updates = _compute_column_comment_updates(desired.columns, live_by_lower)
    if comment_updates:
        actions.append(SetColumnComments(comments=comment_updates))

    return actions

    # 3) Column comments (only set when desired has a non-empty comment and it differs)
    comment_updates = _compute_column_comment_updates(desired.columns, live_by_lower)
    if comment_updates:
        actions.append(SetColumnComments(comments=comment_updates))

    return actions


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
        catalog=desired.fully_qualified_table_name.catalog,
        schema=desired.fully_qualified_table_name.schema,
        table=desired.fully_qualified_table_name.table,
        columns=cols,
    )

    # live has no PK -> create
    if live_pk is None:
        return [
            AddPrimaryKey(
                name=desired_name,
                columns=tuple(cols),
            )
        ]

    # live has PK -> compare name + ordered columns
    if desired_name != live_pk.name or tuple(cols) != tuple(live_pk.columns):
        return [
            DropPrimaryKey(name=live_pk.name),
            AddPrimaryKey(name=desired_name, columns=tuple(cols)),
        ]
    return []


def _coalesce_to_align(
    table: FullyQualifiedTableName,
    action: Iterable[object],  # sub-actions payloads (not top-level Action)
) -> Optional[AlignTable]:
    """
    Fold a sequence of granular sub-actions into one AlignTable.

    Rules:
    - AddColumns: merge into a single payload (append order preserved).
    - AlterColumnNullability: accumulate as-is (one per column).
    - SetColumnComments: merged; later entries override earlier ones.
    - SetTableComment / SetTableProperties: last writer wins (there should be at most one of each).
    - AddPrimaryKey / DropPrimaryKey: keep the last seen of each kind.
    - If nothing to do, return None.
    """
    add_columns_buf: list[Column] = []
    drop_columns_buf: list[Column] = []
    nullability_buf: list[AlterColumnNullability] = []
    comments_buf: dict[str, str] = {}

    table_comment_payload: Optional[SetTableComment] = None
    table_props_payload: Optional[SetTableProperties] = None
    add_pk_payload: Optional[AddPrimaryKey] = None
    drop_pk_payload: Optional[DropPrimaryKey] = None

    for a in action:
        if isinstance(a, AddColumns):
            add_columns_buf.extend(a.columns)
        elif isinstance(a, DropColumns):
            drop_columns_buf.extend(a.columns)
        elif isinstance(a, AlterColumnNullability):
            nullability_buf.append(g)
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
            # Ignore unknown items by design.
            continue

    add_columns_payload: Optional[AddColumns] = (
        AddColumns(columns=tuple(add_columns_buf)) if add_columns_buf else None
    )
    drop_columns_payload: Optional[AddColumns] = (
        AddColumns(columns=tuple(drop_columns_buf)) if drop_columns_buf else None
    )
    set_col_comments_payload: Optional[SetColumnComments] = (
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
        table=table,
        add_columns=add_columns_payload,
        drop_columns=drop_columns_payload,
        alter_nullability=tuple(nullability_buf),
        set_column_comments=set_col_comments_payload,
        set_table_comment=table_comment_payload,
        set_table_properties=table_props_payload,
        add_primary_key=add_pk_payload,
        drop_primary_key=drop_pk_payload,
    )


# ---------- utilities ----------


def _compute_missing_columns(
    desired_columns: tuple[Column, ...],
    live_by_lower: dict[str, ColumnState],
) -> list[Column]:
    """Return desired Column objects that are not present in live."""
    missing: list[Column] = []
    for col in desired_columns:
        if col.name.lower() not in live_by_lower:
            missing.append(col)
    return missing


def _compute_nullability_actions(
    desired_columns: tuple[Column, ...],
    live_by_lower: dict[str, ColumnState],
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
                    column_name=col.name,
                    make_nullable=desired_is_nullable,
                )
            )
    return actions


def _compute_column_comment_updates(
    desired_columns: tuple[Column, ...],
    live_by_lower: dict[str, ColumnState],
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
