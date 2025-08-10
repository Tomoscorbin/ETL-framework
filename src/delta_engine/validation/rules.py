"""
Validation rules for schema table plans.

This module defines a validation framework for `Plan` objects, allowing
rules to enforce safety constraints before execution. Each rule inspects
the plan and raises `UnsafePlanError` if violations are found.
"""

from __future__ import annotations
from typing import Sequence, Tuple, Protocol, TypeVar, runtime_checkable
import pyspark.sql.types as T
from collections import Counter

from src.delta_engine.models import Table, Column
from src.delta_engine.constraints.naming import build_primary_key_name, build_foreign_key_name
from src.delta_engine.actions import TablePlan, ConstraintPlan


# ----------------- BASE RULE -----------------


# ----------------- EXCEPTIONS -----------------

class UnsafePlanError(Exception):
    """Plan violates safety rules."""


# ----------------- TABLE RULES -----------------

class NoAddNotNullColumns:
    """Never ADD COLUMN as NOT NULL in AlignTable actions."""
    def check(self, plan: TablePlan) -> None:
        for action in plan.align_tables:
            for add in action.add_columns:
                if add.is_nullable is False:
                    raise UnsafeTablePlanError(
                        f"Unsafe plan: ADD NOT NULL column `{add.name}` on {_qualified(action)}. "
                        "Add as NULLABLE first, backfill, then tighten."
                    )

class NoDuplicateCreateTableColumnNames:
    """CREATE TABLE schema must not contain duplicate column names (case-insensitive)."""
    def check(self, plan: TablePlan) -> None:
        for ct in plan.create_tables:
            names = [f.name for f in ct.schema_struct.fields]  # T.StructField
            dupes = [n for n, c in Counter(n.lower() for n in names).items() if c > 1]
            if dupes:
                raise UnsafeTablePlanError(
                    f"{ct.catalog_name}.{ct.schema_name}.{ct.table_name}: duplicate column names in CREATE TABLE: {sorted(set(dupes))}"
                )

class NoDuplicateAddColumnNamesPerTable:
    """Within a single AlignTable, no duplicate ADD COLUMN names (case-insensitive)."""
    def check(self, plan: TablePlan) -> None:
        for at in plan.align_tables:
            names = [a.name for a in at.add_columns]
            dupes = [n for n, c in Counter(n.lower() for n in names).items() if c > 1]
            if dupes:
                raise UnsafeTablePlanError(
                    f"{_qualified(at)}: duplicate column names in ADD COLUMN: {sorted(set(dupes))}"
                )


class PrimaryKeyColumnsNotNull:
    """
    CREATE TABLE: if a PK is specified, all PK columns must exist in the schema
    and be NOT NULL in that schema.
    """
    def check(self, plan: TablePlan) -> None:
        for ct in plan.create_tables:
            if not ct.primary_key:
                continue

            # map column -> nullable from create schema
            null_by_col = {f.name.lower(): f.nullable for f in ct.schema_struct.fields}  # type: ignore[attr-defined]

            # must exist
            missing = [c for c in ct.primary_key.columns if c.lower() not in null_by_col]
            if missing:
                raise UnsafePlanError(
                    f"{_qualified(ct)}: PRIMARY KEY references missing column(s): {missing}"
                )

            # must be NOT NULL
            nullable_cols = [c for c in ct.primary_key.columns if null_by_col[c.lower()] is True]
            if nullable_cols:
                raise UnsafePlanError(
                    f"{_qualified(ct)}: PRIMARY KEY columns must be NOT NULL in CREATE TABLE: {nullable_cols}"
                )

class PrimaryKeyAddMustNotMakeColumnsNullable:
    """
    ALIGN TABLE with an added PK: none of the PK columns may be made NULLABLE
    in the same plan (i.e., no change_nullability with make_nullable=True on PK cols).
    """
    def check(self, plan: TablePlan) -> None:
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue

            pk_cols = {c.lower() for c in at.add_primary_key.columns}
            null_change = {c.name.lower(): c.make_nullable for c in at.change_nullability}

            illegal = [c for c in pk_cols if null_change.get(c) is True]
            if illegal:
                raise UnsafePlanError(
                    f"{_qualified(at)}: PRIMARY KEY columns cannot be made NULLABLE in the same plan: {illegal}"
                )

class PrimaryKeyNewColumnsMustBeSetNotNull:
    """
    ALIGN TABLE with an added PK: any PK column that is newly added in this plan
    must also be SET NOT NULL in this plan (since adds are created nullable).
    """
    def check(self, plan: TablePlan) -> None:
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue

            pk_cols = {c.lower() for c in at.add_primary_key.columns}
            added_cols = {a.name.lower() for a in at.add_columns}
            null_change = {c.name.lower(): c.make_nullable for c in at.change_nullability}

            # For PK columns added now, require an explicit make_nullable=False change
            missing_notnull = [
                c for c in pk_cols
                if c in added_cols and null_change.get(c) is not False
            ]
            if missing_notnull:
                raise UnsafePlanError(
                    f"{_qualified(at)}: Newly added PRIMARY KEY columns must be SET NOT NULL in the same plan: {missing_notnull}"
                )

# ----------------- helpers -----------------

def _qualified(a) -> str:
    return f"{a.catalog_name}.{a.schema_name}.{a.table_name}"