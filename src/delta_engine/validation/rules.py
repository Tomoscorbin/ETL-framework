"""
Validation rules for schema table plans.

This module defines a validation framework for `Plan` objects, allowing
rules to enforce safety constraints before execution. Each rule inspects
the plan and raises `UnsafePlanError` if violations are found.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from typing import Any

from src.delta_engine.actions import TablePlan
from src.delta_engine.utils import qualify_table_name

# ----------------- EXCEPTIONS -----------------


class UnsafePlanError(Exception):
    """Plan violates safety rules."""


# ----------------- BASE RULE -----------------


class ValidationRule(ABC):
    """Abstract base for plan validation rules providing fail() and table-name qualification."""

    @property
    def name(self) -> str:
        """Return the rule's simple class name for error prefixes."""
        return self.__class__.__name__

    def fail(self, message: str) -> None:
        """Raise UnsafePlanError with a standardized [RuleName] prefix."""
        raise UnsafePlanError(f"[{self.name}] {message}")

    @staticmethod
    def _qualify(obj: Any) -> str:
        """
        Return 'catalog.schema.table' for an action-like object,
        or '<unknown>' if unavailable.
        """
        try:
            return qualify_table_name(obj)
        except Exception:
            return "<unknown>"

    @abstractmethod
    def check(self, plan: TablePlan) -> None:
        """Validate the plan and raise UnsafePlanError on violation."""
        ...


# ----------------- TABLE RULES -----------------


class NoAddNotNullColumns(ValidationRule):
    """Never ADD COLUMN as NOT NULL in AlignTable actions."""

    def check(self, plan: TablePlan) -> None:
        """Forbid ADD COLUMN with NOT NULL in AlignTable actions."""
        for action in plan.align_tables:
            for add in action.add_columns:
                if add.is_nullable is False:
                    self.fail(
                        f"{self._qualify(action)}: ADD COLUMN `{add.name}` must be created "
                        "as NULLABLE; backfill, then tighten with SET NOT NULL."
                    )


class NoDuplicateCreateTableColumnNames(ValidationRule):
    """CREATE TABLE schema must not contain duplicate column names (case-insensitive)."""

    def check(self, plan: TablePlan) -> None:
        """Validate no duplicate column names appear in CREATE TABLE schemas."""
        for ct in plan.create_tables:
            fields = ct.schema_struct.fields
            names = [f.name for f in fields]
            dupes = sorted({n for n, c in Counter(n.lower() for n in names).items() if c > 1})
            if dupes:
                self.fail(f"{self._qualify(ct)}: duplicate column names in CREATE TABLE: {dupes}")


class NoDuplicateAddColumnNamesPerTable(ValidationRule):
    """Within a single AlignTable, no duplicate ADD COLUMN names (case-insensitive)."""

    def check(self, plan: TablePlan) -> None:
        """Validate no duplicate column names are added within the same AlignTable."""
        for at in plan.align_tables:
            names = [a.name for a in at.add_columns]
            dupes = sorted({n for n, c in Counter(n.lower() for n in names).items() if c > 1})
            if dupes:
                self.fail(f"{self._qualify(at)}: duplicate column names in ADD COLUMN: {dupes}")


class PrimaryKeyColumnsNotNull(ValidationRule):
    """
    CREATE TABLE: if a PK is specified, all PK columns must exist and
    be NOT NULL in the schema.
    """

    def check(self, plan: TablePlan) -> None:
        """Validate CREATE TABLE PK columns exist and are NOT NULL."""
        for ct in plan.create_tables:
            if not ct.primary_key:
                continue

            fields = ct.schema_struct.fields
            null_by_col = {f.name.lower(): f.nullable for f in fields}

            missing = [c for c in ct.primary_key.columns if c.lower() not in null_by_col]
            if missing:
                self.fail(
                    f"{self._qualify(ct)}: PRIMARY KEY references missing column(s): {missing}"
                )

            nullable_cols = [c for c in ct.primary_key.columns if null_by_col[c.lower()] is True]
            if nullable_cols:
                self.fail(
                    f"{self._qualify(ct)}: PRIMARY KEY columns must be NOT NULL"
                    " in CREATE TABLE: {nullable_cols}"
                )


class PrimaryKeyAddMustNotMakeColumnsNullable(ValidationRule):
    """When adding a PK, none of its columns may be made NULLABLE in the same plan."""

    def check(self, plan: TablePlan) -> None:
        """Validate AlignTable does not set PK columns to NULLABLE."""
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue

            pk_cols = {c.lower() for c in at.add_primary_key.definition.columns}
            null_change = {c.name.lower(): c.make_nullable for c in at.change_nullability}

            illegal = [c for c in pk_cols if null_change.get(c) is True]
            if illegal:
                self.fail(
                    f"{self._qualify(at)}: PRIMARY KEY columns cannot be made NULLABLE"
                    " in the same plan: {illegal}"
                )


class PrimaryKeyNewColumnsMustBeSetNotNull(ValidationRule):
    """When adding a PK, any newly added PK column must also be SET NOT NULL in the same plan."""

    def check(self, plan: TablePlan) -> None:
        """Validate newly added PK columns are tightened to NOT NULL in the same AlignTable."""
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue

            pk_cols = {c.lower() for c in at.add_primary_key.definition.columns}
            added_cols = {a.name.lower() for a in at.add_columns}
            null_change = {c.name.lower(): c.make_nullable for c in at.change_nullability}

            missing_notnull = [
                c for c in pk_cols if c in added_cols and null_change.get(c) is not False
            ]
            if missing_notnull:
                self.fail(
                    f"{self._qualify(at)}: Newly added PRIMARY KEY columns must be"
                    " SET NOT NULL in the same plan: {missing_notnull}"
                )
