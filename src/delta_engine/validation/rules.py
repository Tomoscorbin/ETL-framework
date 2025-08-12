"""
Validation rules for table plans and models.

This module defines a validation framework for `TablePlan` objects and
for desired `Table` models. Rules enforce safety constraints before execution.
Each rule inspects the input and raises `UnsafePlanError` (for plan rules)
or `InvalidModelError` (for model rules) if violations are found.

To add a rule, implement either:
- `PlanRule.check(plan: TablePlan) -> None`
- `ModelRule.check(models: Sequence[Table]) -> None`
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Sequence
from typing import Any

from src.delta_engine.actions import TablePlan
from src.delta_engine.models import Table
from src.delta_engine.utils import qualify_table_name

# ----------------- EXCEPTIONS -----------------


class UnsafePlanError(Exception):
    """Plan violates safety rules."""


class InvalidModelError(Exception):
    """The defined model is invalid."""


# ----------------- BASE RULES -----------------


class ValidationRule:
    """Shared utilities for all rules."""

    @property
    def name(self) -> str:
        """Rule's class name for error prefixes."""
        return self.__class__.__name__

    @staticmethod
    def _qualify(obj: Any) -> str:
        """Return 'catalog.schema.table' for an action-like object, or '<unknown>'."""
        try:
            return qualify_table_name(obj)
        except Exception:
            return "<unknown>"

    @staticmethod
    def _format_columns(cols: Iterable[str]) -> str:
        """Format a list of column names for error messages."""
        return ", ".join(f"`{c}`" for c in cols)


class PlanRule(ValidationRule, ABC):
    """Rules that validate a compiled TablePlan."""

    def fail(self, message: str) -> None:
        """Raise UnsafePlanError with a standardized prefix."""
        raise UnsafePlanError(f"[{self.name}] {message}")

    @abstractmethod
    def check(self, plan: TablePlan) -> None:
        """Validate the table plan; raise UnsafePlanError on violation."""
        ...


class ModelRule(ValidationRule, ABC):
    """Rules that validate desired Table models (no catalog state)."""

    def fail(self, message: str) -> None:
        """Raise InvalidModelError with a standardized prefix."""
        raise InvalidModelError(f"[{self.name}] {message}")

    @abstractmethod
    def check(self, models: Sequence[Table]) -> None:
        """Validate the models; raise InvalidModelError on violation."""
        ...


# ----------------- TABLE RULES -----------------


class NoAddNotNullColumns(PlanRule):
    """
    Rejects adding columns as NOT NULL during an AlignTable.

    Why: in Delta/UC, adding a NOT NULL column is unsafe because legacy rows violate the constraint.
    Safer pattern: add as NULLABLE → backfill → SET NOT NULL.
    """

    def check(self, plan: TablePlan) -> None:
        """Scan AlignTable.add_columns and fail if any new column is declared NOT NULL."""
        for at in plan.align_tables:
            for add in at.add_columns:
                if add.is_nullable is False:
                    self.fail(
                        f"{self._qualify(at)}: ADD COLUMN `{add.name}` must be"
                        " created as NULLABLE. backfill, then tighten with SET NOT NULL."
                    )


class CreatePrimaryKeyColumnsNotNull(PlanRule):
    """
    Ensures CREATE TABLE primary key columns exist and are NOT NULL in the create schema.

    Why: UC enforces PK columns to be non-nullable; creating an invalid PK will fail on apply.
    """

    def check(self, plan: TablePlan) -> None:
        """For each CreateTable with a PK, require all PK columns to exist and be NOT NULL."""
        for ct in plan.create_tables:
            if not ct.primary_key:
                continue

            column_nullable_by_name = {
                f.name.lower(): bool(f.nullable) for f in ct.schema_struct.fields
            }
            missing = [
                c for c in ct.primary_key.columns if c.lower() not in column_nullable_by_name
            ]
            if missing:
                self.fail(
                    f"{self._qualify(ct)}: PRIMARY KEY references missing column(s): "
                    f"{self._format_columns(missing)}"
                )

            nullable_cols = [
                c for c in ct.primary_key.columns if column_nullable_by_name[c.lower()] is True
            ]
            if nullable_cols:
                self.fail(
                    f"{self._qualify(ct)}: PRIMARY KEY columns must be NOT NULL in CREATE TABLE: "
                    f"{self._format_columns(nullable_cols)}"
                )


class PrimaryKeyAddMustNotMakeColumnsNullable(PlanRule):
    """
    Disallows loosening PK columns in the same AlignTable that adds a PK.

    Why: adding a PK while also making one of its columns NULLABLE is contradictory and unsafe.
    """

    def check(self, plan: TablePlan) -> None:
        """When add_primary_key is present, fail if any PK column has make_nullable=True."""
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue
            pk_cols = {c.lower() for c in at.add_primary_key.definition.columns}
            null_change_by_col = {c.name.lower(): c.make_nullable for c in at.change_nullability}
            illegal = [c for c in pk_cols if null_change_by_col.get(c) is True]
            if illegal:
                self.fail(
                    f"{self._qualify(at)}: PRIMARY KEY columns cannot be made NULLABLE"
                    f" in the same plan: {self._format_columns(illegal)}"
                )


class PrimaryKeyNewColumnsMustBeSetNotNull(PlanRule):
    """
    Requires tightening newly added PK columns to NOT NULL in the same AlignTable.

    Why: ADD COLUMN creates the column as NULLABLE; a PK over it would fail unless
    you also SET NOT NULL.
    """

    def check(self, plan: TablePlan) -> None:
        """
        When adding a PK, any PK column added in this AlignTable must
        have make_nullable=False.
        """
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue
            pk_cols = {c.lower() for c in at.add_primary_key.definition.columns}
            added_cols = {a.name.lower() for a in at.add_columns}
            null_change_by_col = {c.name.lower(): c.make_nullable for c in at.change_nullability}
            missing_notnull = [
                c for c in pk_cols if c in added_cols and null_change_by_col.get(c) is not False
            ]
            if missing_notnull:
                self.fail(
                    f"{self._qualify(at)}: Newly added PRIMARY KEY columns must be SET NOT NULL"
                    f" in the same plan: {self._format_columns(missing_notnull)}"
                )


class PrimaryKeyExistingColumnsMustBeSetNotNull(PlanRule):
    """
    Requires tightening *existing* PK columns to NOT NULL in the same AlignTable
    that adds the PK.

    Policy note: this rule is deliberately conservative because the validator
    does not see live state.
    Requiring an explicit SET NOT NULL for existing PK columns guarantees UC will
    accept the PK, regardless of current nullability.
    """

    def check(self, plan: TablePlan) -> None:
        """
        When adding a PK, any PK column that is not added in this AlignTable must
        have make_nullable=False.
        """
        for at in plan.align_tables:
            if not at.add_primary_key:
                continue
            pk_cols = {c.lower() for c in at.add_primary_key.definition.columns}
            added_cols = {a.name.lower() for a in at.add_columns}
            existing_pk_cols = sorted(pk_cols - added_cols)
            null_change_by_col = {c.name.lower(): c.make_nullable for c in at.change_nullability}
            missing_tighten = [
                c for c in existing_pk_cols if null_change_by_col.get(c) is not False
            ]
            if missing_tighten:
                self.fail(
                    f"{self._qualify(at)}: Existing PRIMARY KEY columns must be SET NOT NULL"
                    f" in the same plan: {self._format_columns(missing_tighten)}"
                )


class PrimaryKeyColumnsNotNull(ModelRule):
    """
    Ensure that, in the desired models, primary-key columns exist and are NOT NULL.

    Why: Unity Catalog requires PK columns to be non-nullable. If the model marks a PK column as
    nullable, later DDL will fail; catch it at authoring time.
    """

    def check(self, models: Sequence[Table]) -> None:
        """For each model with a PK, verify each PK column exists and has is_nullable=False."""
        for m in models:
            pk = m.primary_key
            if pk is None:
                continue

            nullable_by_name = {c.name.lower(): bool(c.is_nullable) for c in m.columns}

            # must exist
            missing = [c for c in pk if c.lower() not in nullable_by_name]
            if missing:
                full_name = f"{m.catalog_name}.{m.schema_name}.{m.table_name}"
                self.fail(
                    f"{full_name}: primary key columns missing from model: "
                    f"{self._format_columns(missing)}"
                )

            # must be NOT NULL
            nullable = [c for c in pk if nullable_by_name.get(c.lower()) is True]
            if nullable:
                full_name = f"{m.catalog_name}.{m.schema_name}.{m.table_name}"
                self.fail(
                    f"{full_name}: primary key columns must be NOT NULL in model: "
                    f"{self._format_columns(nullable)}"
                )


class DuplicateColumnNames(ModelRule):
    """
    Reject duplicate column names in a model's column list (case-insensitive).

    Why: ambiguous schemas lead to brittle queries and may fail at runtime.
    """

    def check(self, models: Sequence[Table]) -> None:
        """Fail any model whose declared columns include case-insensitive duplicates."""
        for m in models:
            names = [c.name for c in m.columns]
            lower_counts = Counter(n.lower() for n in names)
            duplicates = sorted({n for n, count in lower_counts.items() if count > 1})
            if duplicates:
                full_name = f"{m.catalog_name}.{m.schema_name}.{m.table_name}"
                self.fail(
                    f"{full_name}: duplicate column names in model: "
                    f"{self._format_columns(duplicates)}"
                )


class PrimaryKeyMustBeOrderedSequence(ModelRule):
    """
    Require `Table.primary_key` to be an ordered sequence (list or tuple).

    Why: Primary-key column **order** is semantically meaningful (constraint definition and
    validation) and is used to generate deterministic constraint names. Unordered or
    non-sequence types (e.g., sets, dicts, strings, scalars) lead to nondeterministic
    plans or accidental per-character iteration.
    """

    def check(self, models: Sequence[Table]) -> None:
        """Ensure each model's `primary_key` is an ordered sequence (list or tuple)."""
        for m in models:
            pk = getattr(m, "primary_key", None)
            if pk is None:
                continue
            if not isinstance(pk, list | tuple):
                self.fail(
                    f"{m.catalog_name}.{m.schema_name}.{m.table_name}: "
                    "primary_key must be an ordered sequence (list/tuple)."
                )
