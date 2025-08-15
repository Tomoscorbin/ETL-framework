"""
CreateExecutor

Applies a `CreateTable` action in a deterministic order:

  1) ensure table exists (schema + optional table comment)
  2) set table properties (if provided)
  3) set per-column comments (non-empty only; idempotent)
  4) add primary key (if provided)

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
from src.delta_engine.models import Column
from src.delta_engine.plan.actions import (
    AddPrimaryKey,
    CreateTable,
    SetTableComment,
    SetTableProperties,
)


class CreateExecutor:
    """Execute a `CreateTable` action by delegating all DDL to `DDLExecutor`."""

    def __init__(self, spark: SparkSession) -> None:
        self._ddl = DDLExecutor(spark)

    def apply(self, action: CreateTable, *, policy: ExecutionPolicy) -> tuple[ActionResult, ...]:
        results: list[ActionResult] = []

        full_table_name: FullyQualifiedTableName = action.full_table_name
        columns: tuple[Column, ...] = action.add_columns.columns

        # 1) ensure table exists (columns + optional table comment)
        comment_value = self._comment_or_none(action.set_table_comment)
        result = self._ensure_table_exists(
            full_table_name=full_table_name,
            columns=columns,
            table_comment=comment_value,
            policy=policy,
            action=action,
        )
        results.append(result)
        if result.status == ApplyStatus.FAILED and policy.stop_on_first_error:
            return tuple(results)

        # 2) properties (optional)
        results.extend(
            self._set_table_properties(
                full_table_name=full_table_name,
                set_props=action.set_table_properties,
                policy=policy,
                action=action,
            )
        )
        if any(r.status == ApplyStatus.FAILED for r in results) and policy.stop_on_first_error:
            return tuple(results)

        # 3) column comments (non-empty only)
        results.extend(
            self._set_column_comments(
                full_table_name=full_table_name,
                columns=columns,
                policy=policy,
                action=action,
            )
        )
        if any(r.status == ApplyStatus.FAILED for r in results) and policy.stop_on_first_error:
            return tuple(results)

        # 4) primary key (optional)
        pk_result = self._add_primary_key(
            full_table_name=full_table_name,
            add_pk=action.add_primary_key,
            policy=policy,
            action=action,
        )
        if pk_result is not None:
            results.append(pk_result)

        return tuple(results)

    # ---------- helpers (no SQL here) ----------

    def _ensure_table_exists(
        self,
        *,
        full_table_name: FullyQualifiedTableName,
        columns: tuple[Column, ...],
        table_comment: str | None,
        policy: ExecutionPolicy,
        action: CreateTable,
    ) -> ActionResult:
        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            msg = f"(dry-run) ensure table {table_str} with {len(columns)} column(s)"
            if table_comment is not None:
                msg += " (include table comment)"
            return ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg)

        try:
            self._ddl.create_table_if_not_exists(
                full_table_name=full_table_name,
                columns=columns,
                table_comment=table_comment,
            )
            return ActionResult(
                action=action,
                status=ApplyStatus.OK,
                message=f"Ensured table {table_str} exists",
            )
        except Exception as error:
            return ActionResult(
                action=action,
                status=ApplyStatus.FAILED,
                message=f"Failed to ensure table {table_str}: {type(error).__name__}: {error}",
            )

    def _set_table_properties(
        self,
        *,
        full_table_name: FullyQualifiedTableName,
        set_props: SetTableProperties | None,
        policy: ExecutionPolicy,
        action: CreateTable,
    ) -> tuple[ActionResult, ...]:
        if set_props is None or not set_props.properties:
            return ()

        table_str = self._as_unquoted(full_table_name)

        if policy.dry_run:
            msg = f"(dry-run) set {len(set_props.properties)} table propertie(s) on {table_str}"
            return (ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg),)

        try:
            self._ddl.set_table_properties(
                full_table_name=full_table_name,
                properties=set_props.properties,
            )
            return (
                ActionResult(
                    action=action,
                    status=ApplyStatus.OK,
                    message=f"Set {len(set_props.properties)} table propertie(s) on {table_str}",
                ),
            )
        except Exception as error:
            return (
                ActionResult(
                    action=action,
                    status=ApplyStatus.FAILED,
                    message=f"Failed to set table properties on {table_str}: {type(error).__name__}: {error}",
                ),
            )

    def _set_column_comments(
        self,
        *,
        full_table_name: FullyQualifiedTableName,
        columns: tuple[Column, ...],
        policy: ExecutionPolicy,
        action: CreateTable,
    ) -> tuple[ActionResult, ...]:
        wanted = [(c.name, c.comment) for c in columns if c.comment]
        if not wanted:
            return ()

        table_str = self._as_unquoted(full_table_name)
        results: list[ActionResult] = []

        if policy.dry_run:
            for column_name, _ in wanted:
                msg = f"(dry-run) set comment on column {table_str}.{column_name}"
                results.append(ActionResult(action=action, status=ApplyStatus.SKIPPED, message=msg))
            return tuple(results)

        for column_name, comment in wanted:
            try:
                self._ddl.set_column_comment(
                    full_table_name=full_table_name,
                    column_name=column_name,
                    comment=comment,
                )
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.OK,
                        message=f"Set comment on column {table_str}.{column_name}",
                    )
                )
            except Exception as error:
                results.append(
                    ActionResult(
                        action=action,
                        status=ApplyStatus.FAILED,
                        message=f"Failed to set comment on column {table_str}.{column_name}: {type(error).__name__}: {error}",
                    )
                )
        return tuple(results)

    def _add_primary_key(
        self,
        *,
        full_table_name: FullyQualifiedTableName,
        add_pk: AddPrimaryKey | None,
        policy: ExecutionPolicy,
        action: CreateTable,
    ) -> ActionResult | None:
        if add_pk is None:
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
                message=f"Failed to add primary key on {table_str}: {type(error).__name__}: {error}",
            )

    @staticmethod
    def _comment_or_none(set_comment: SetTableComment | None) -> str | None:
        """None → omit; '' → clear; 'x' → set to 'x'."""
        return None if set_comment is None else set_comment.comment

    @staticmethod
    def _as_unquoted(full_table_name: FullyQualifiedTableName) -> str:
        """Render 'catalog.schema.table' for human-readable messages."""
        return format_fully_qualified_table_name_from_parts(
            full_table_name.catalog, full_table_name.schema, full_table_name.table
        )
