"""
AlignExecutor

Applies a single coalesced `AlignTable` action in a deterministic, safe order:

  0) drop primary key (if requested) — unblocks schema edits
  1) add columns
  2) drop columns
  3) set column nullability
  4) add primary key (if requested)
  5) set column comments
  6) set table comment
  7) set table properties

Respects ExecutionPolicy:
- dry_run=True  → do not execute; return SKIPPED results with descriptive messages
- stop_on_first_error=True  → skip remaining steps after a failure
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.delta_engine.execute.ddl_executor import DDLExecutor
from src.delta_engine.execute.ports import ActionResult, ApplyStatus, ExecutionPolicy
from src.delta_engine.identifiers import (
    FullyQualifiedTableName,
    format_fully_qualified_table_name_from_parts,
)
from src.delta_engine.plan.actions import (
    AddColumns,
    AddPrimaryKey,
    AlignTable,
    DropColumns,
    DropPrimaryKey,
    SetColumnComments,
    SetColumnNullability,
    SetTableComment,
    SetTableProperties,
)


class AlignExecutor:
    """
    Executor that applies a single per-table `AlignTable` action.

    Responsibilities:
    ----------------
    - Delegates all DDL operations to `DDLExecutor`.
    - Applies granular changes for one table in a consistent order:
        0. Drop primary key (if requested).
        1. Add columns.
        2. [Subsequent steps: drop columns, set comments, update properties, etc.]
    - Halts execution early if `policy.stop_on_first_error` is True and a step fails.

    Notes:
    -----
    Align actions represent the coalesced set of diffs for one table
    (columns, comments, properties, constraints).
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor."""
        self._ddl = DDLExecutor(spark)

    def apply(self, action: AlignTable, *, policy: ExecutionPolicy) -> tuple[ActionResult, ...]:
        """
        Apply an `AlignTable` action by executing the necessary DDL steps.

        Workflow:
        --------
        0. Drop primary key (if specified).
        1. Add new columns.
        2. Apply subsequent alignment steps (column drops, comments,
           properties, nullability changes, primary key adds, etc.).
        """
        results: list[ActionResult] = []
        full_table_name: FullyQualifiedTableName = action.full_table_name

        # 0) Drop PRIMARY KEY
        result = self._drop_primary_key(full_table_name, action.drop_primary_key, policy, action)
        if result is not None:
            results.append(result)
            if result.status == ApplyStatus.FAILED and policy.stop_on_first_error:
                return tuple(results)

        # 1) Add columns
        results.extend(self._add_columns(full_table_name, action.add_columns, policy, action))
        if any(r.status == ApplyStatus.FAILED for r in results) and policy.stop_on_first_error:
            return tuple(results)

        # 2) Drop columns
        result = self._drop_columns(full_table_name, action.drop_columns, policy, action)
        if result is not None:
            results.append(result)
            if result.status == ApplyStatus.FAILED and policy.stop_on_first_error:
                return tuple(results)

        # 3) Set column nullability
        results.extend(
            self._set_nullability(full_table_name, action.set_nullability, policy, action)
        )
        if any(r.status == ApplyStatus.FAILED for r in results) and policy.stop_on_first_error:
            return tuple(results)

        # 4) Add PRIMARY KEY
        result = self._add_primary_key(full_table_name, action.add_primary_key, policy, action)
        if result is not None:
            results.append(result)
            if result.status == ApplyStatus.FAILED and policy.stop_on_first_error:
                return tuple(results)

        # 5) Set column comments
        results.extend(
            self._set_column_comments(full_table_name, action.set_column_comments, policy, action)
        )
        if any(r.status == ApplyStatus.FAILED for r in results) and policy.stop_on_first_error:
            return tuple(results)

        # 6) Set table comment
        result = self._set_table_comment(full_table_name, action.set_table_comment, policy, action)
        if result is not None:
            results.append(result)
            if result.status == ApplyStatus.FAILED and policy.stop_on_first_error:
                return tuple(results)

        # 7) Set table properties
        result = self._set_table_properties(
            full_table_name, action.set_table_properties, policy, action
        )
        if result is not None:
            results.append(result)

        return tuple(results)

    # ---------- helpers (no SQL here) ----------

    @staticmethod
    def _as_unquoted(full_table_name: FullyQualifiedTableName) -> str:
        """Render 'catalog.schema.table' for human-readable messages."""
        return format_fully_qualified_table_name_from_parts(
            full_table_name.catalog, full_table_name.schema, full_table_name.table
        )

    def _add_columns(
        self,
        full_table_name: FullyQualifiedTableName,
        add: AddColumns | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> tuple[ActionResult, ...]:
        if not add or not add.columns:
            return ()

        table_str = self._as_unquoted(full_table_name)
        results: list[ActionResult] = []

        if policy.dry_run:
            for column in add.columns:
                msg = f"(dry-run) add column {table_str}.{column.name}"
                results.append(ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg))
            return tuple(results)

        for column in add.columns:
            try:
                self._ddl.add_column(
                    full_table_name=full_table_name,
                    column_name=column.name,
                    data_type=column.data_type,
                    is_nullable=column.is_nullable,
                    comment=(column.comment or None),
                )
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.OK,
                        message=f"Added column {table_str}.{column.name}",
                    )
                )
            except Exception as error:
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.FAILED,
                        message=(
                            f"Failed to add column {table_str}.{column.name}:"
                            f" {type(error).__name__}: {error}",
                        )
                    )
                )
        return tuple(results)

    def _drop_columns(
        self,
        full_table_name: FullyQualifiedTableName,
        drop: DropColumns | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> ActionResult | None:
        if not drop or not drop.columns:
            return None

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            cols = ", ".join(drop.columns)
            msg = f"(dry-run) drop {len(drop.columns)} column(s) from {table_str}: {cols}"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.drop_columns(full_table_name=full_table_name, column_names=drop.columns)
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Dropped {len(drop.columns)} column(s) from {table_str}",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=(
                    f"Failed to drop columns from {table_str}:"
                    f" {type(error).__name__}: {error}",
                )
            )

    def _set_nullability(
        self,
        full_table_name: FullyQualifiedTableName,
        items: tuple[SetColumnNullability, ...] | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> tuple[ActionResult, ...]:
        if not items:
            return ()

        table_str = self._as_unquoted(full_table_name)
        results: list[ActionResult] = []

        if policy.dry_run:
            for change in items:
                to = "NULL" if change.make_nullable else "NOT NULL"
                msg = f"(dry-run) set {table_str}.{change.column_name} {to}"
                results.append(ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg))
            return tuple(results)

        for change in items:
            try:
                self._ddl.set_column_nullability(
                    full_table_name=full_table_name,
                    column_name=change.column_name,
                    make_nullable=change.make_nullable,
                )
                to = "NULL" if change.make_nullable else "NOT NULL"
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.OK,
                        message=f"Set {table_str}.{change.column_name} {to}",
                    )
                )
            except Exception as error:
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.FAILED,
                        message=(
                            f"Failed to set nullability on {table_str}.{change.column_name}:"
                            f" {type(error).__name__}: {error}",
                        )
                    )
                )
        return tuple(results)

    def _set_column_comments(
        self,
        full_table_name: FullyQualifiedTableName,
        set_comments: SetColumnComments | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> tuple[ActionResult, ...]:
        if not set_comments or not set_comments.comments:
            return ()

        table_str = self._as_unquoted(full_table_name)
        results: list[ActionResult] = []

        if policy.dry_run:
            for name in set_comments.comments:
                msg = f"(dry-run) set comment on column {table_str}.{name}"
                results.append(ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg))
            return tuple(results)

        for name, comment in set_comments.comments.items():
            try:
                self._ddl.set_column_comment(
                    full_table_name=full_table_name,
                    column_name=name,
                    comment=comment,
                )
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.OK,
                        message=f"Set comment on column {table_str}.{name}",
                    )
                )
            except Exception as error:
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.FAILED,
                        message=(
                            f"Failed to set comment on column {table_str}.{name}:"
                            f" {type(error).__name__}: {error}",
                        )
                    )
                )
        return tuple(results)

    def _set_table_comment(
        self,
        full_table_name: FullyQualifiedTableName,
        set_comment: SetTableComment | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> ActionResult | None:
        if set_comment is None:
            return None

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            msg = f"(dry-run) set table comment on {table_str}"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.set_table_comment(
                full_table_name=full_table_name, comment=set_comment.comment
            )
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Set table comment on {table_str}",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=(
                    f"Failed to set table comment on {table_str}:"
                    f" {type(error).__name__}: {error}",
                )
            )

    def _set_table_properties(
        self,
        full_table_name: FullyQualifiedTableName,
        set_props: SetTableProperties | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> ActionResult | None:
        if not set_props or not set_props.properties:
            return None

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            msg = f"(dry-run) set {len(set_props.properties)} table propertie(s) on {table_str}"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.set_table_properties(
                full_table_name=full_table_name, properties=set_props.properties
            )
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Set {len(set_props.properties)} table propertie(s) on {table_str}",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=(
                    f"Failed to set table properties on {table_str}:"
                    f" {type(error).__name__}: {error}",
                )
            )

    def _add_primary_key(
        self,
        full_table_name: FullyQualifiedTableName,
        add_pk: AddPrimaryKey | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> ActionResult | None:
        if not add_pk:
            return None

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            cols = ", ".join(add_pk.columns)
            msg = f"(dry-run) add primary key {add_pk.name} ({cols}) on {table_str}"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.add_primary_key(
                full_table_name=full_table_name,
                constraint_name=add_pk.name,
                column_names=add_pk.columns,
            )
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Added primary key {add_pk.name} on {table_str}",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=(
                    f"Failed to add primary key on {table_str}:"
                    f" {type(error).__name__}: {error}",
                )
            )

    def _drop_primary_key(
        self,
        full_table_name: FullyQualifiedTableName,
        drop_pk: DropPrimaryKey | None,
        policy: ExecutionPolicy,
        action: AlignTable,
    ) -> ActionResult | None:
        if not drop_pk:
            return None

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            msg = f"(dry-run) drop primary key {drop_pk.name} on {table_str}"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.drop_primary_key(
                full_table_name=full_table_name, constraint_name=drop_pk.name
            )
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Dropped primary key {drop_pk.name} on {table_str}",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=(
                    f"Failed to drop primary key on {table_str}:"
                    f" {type(error).__name__}: {error}",
                )
            )
