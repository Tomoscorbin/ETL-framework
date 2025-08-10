"""
Validation rules for schema table plans.

This module defines a validation framework for `Plan` objects, allowing
rules to enforce safety constraints before execution. Each rule inspects
the plan and raises `UnsafePlanError` if violations are found.
"""

from src.delta_engine.actions import Plan


class UnsafePlanError(Exception):
    """Plan violates safety rules."""


class ValidationRule:
    """Base interface for a plan validation rule."""

    def check(self, plan: Plan) -> None:
        """Validate the given plan against the rule."""
        raise NotImplementedError


class NoAddNotNullColumnsRule(ValidationRule):
    """
    For AlignTable actions, never plan ADD COLUMN with is_nullable=False.
    Columns should be added as NULLABLE, then tightened after backfill.
    This check safeguards against not nullable columns being
    added to pre-existing tables, where new columns are automatically
    populated with nulls upon creation.
    """

    def check(self, plan: Plan) -> None:
        """Validate that no AlignTable action adds a NOT NULL column."""
        for action in plan.align_tables:
            for add in action.add_columns:
                if add.is_nullable is False:
                    full_name = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"
                    raise UnsafePlanError(
                        f"Unsafe plan: ADD NOT NULL column `{add.name}` on {full_name}. "
                        "Add as NULLABLE first, backfill in ETL, then tighten nullability later."
                    )
