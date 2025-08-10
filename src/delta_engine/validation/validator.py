"""
Plan validation orchestration.

This module defines `PlanValidator`, which runs all registered
validation rules against a `Plan` before execution to ensure
it meets defined safety constraints.
"""

from typing import ClassVar

from src.delta_engine.models import Table
from src.delta_engine.actions import Plan
from .validation_rules import (
    ValidationRule
    NoAddNotNullColumnsRule,
    ConstraintRule,
    PrimaryKeyNonEmptyRule,
    PrimaryKeyNoDuplicatesRule,
    PrimaryKeyColumnsExistRule,
    PrimaryKeyColumnsNotNullableRule,
    PrimaryKeyNoComplexTypesRule,
    ForeignKeyTargetTableExistsRule,
    ForeignKeyTargetHasPrimaryKeyRule,
    ForeignKeyTargetColumnsMatchPrimaryKeyRule,
    ForeignKeyArityMatchesRule,
    ForeignKeySourceColumnsExistRule,
    ForeignKeyTargetColumnsExistRule,
    ForeignKeySourceColumnsNotNullableRule,
    ForeignKeyTypesMatchExactlyRule,
    ConstraintNameCollisionRule,
)



class PlanValidator:
    """Validates a plan against a set of safety rules."""

    RULES: ClassVar[list[ValidationRule]] = [
        NoAddNotNullColumnsRule(),
    ]

    def validate(self, plan: Plan) -> None:
        """Run all validation rules against the plan."""
        for rule in self.RULES:
            rule.check(plan)


class ConstraintValidator:
    RULES: ClassVar[tuple[ConstraintRule, ...]] = (
        PrimaryKeyNonEmptyRule(),
        PrimaryKeyNoDuplicatesRule(),
        PrimaryKeyColumnsExistRule(),
        PrimaryKeyColumnsNotNullableRule(),
        PrimaryKeyNoComplexTypesRule(),
        ForeignKeyTargetTableExistsRule(),
        ForeignKeyTargetHasPrimaryKeyRule(),
        ForeignKeyTargetColumnsMatchPrimaryKeyRule(),
        ForeignKeyArityMatchesRule(),
        ForeignKeySourceColumnsExistRule(),
        ForeignKeyTargetColumnsExistRule(),
        ForeignKeySourceColumnsNotNullableRule(),
        ForeignKeyTypesMatchExactlyRule(),
        ConstraintNameCollisionRule(),
    )

    def validate(self, tables: Sequence[Table]) -> None:
        for rule in self.RULES:
            rule.check(tables)