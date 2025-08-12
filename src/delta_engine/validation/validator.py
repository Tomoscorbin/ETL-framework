"""Run validation rules over models and compiled table plans.

This module wires together model-level and plan-level validation rules.
Rules are executed fail-fast: the first violation raises either
`InvalidModelError` (for model rules) or `UnsafePlanError` (for plan rules).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from src.delta_engine.actions import TablePlan
from src.delta_engine.models import Table
from src.delta_engine.validation.rules import (
    CreatePrimaryKeyColumnsNotNull,
    DuplicateColumnNames,
    ModelRule,
    NoAddNotNullColumns,
    PlanRule,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyColumnsNotNull,
    PrimaryKeyExistingColumnsMustBeSetNotNull,
    PrimaryKeyNewColumnsMustBeSetNotNull,
)


class PlanValidator:
    """Validates models and/or a `TablePlan` by running a fixed set of rules (fail-fast)."""

    DEFAULT_MODEL_RULES: ClassVar[tuple[ModelRule, ...]] = (
        DuplicateColumnNames(),
        PrimaryKeyColumnsNotNull(),
    )
    DEFAULT_PLAN_RULES: ClassVar[tuple[PlanRule, ...]] = (
        NoAddNotNullColumns(),
        CreatePrimaryKeyColumnsNotNull(),
        PrimaryKeyAddMustNotMakeColumnsNullable(),
        PrimaryKeyNewColumnsMustBeSetNotNull(),
        PrimaryKeyExistingColumnsMustBeSetNotNull(),
    )


    def __init__(
        self,
        model_rules: Sequence[ModelRule] | None = None,
        plan_rules: Sequence[PlanRule] | None = None,
    ) -> None:
        """
        Optionally override the rule sets (useful for tests or policy changes).
        """
        self.model_rules: tuple[ModelRule, ...] = (
            tuple(model_rules) if model_rules is not None else self.DEFAULT_MODEL_RULES
        )
        self.plan_rules: tuple[PlanRule, ...] = (
            tuple(plan_rules) if plan_rules is not None else self.DEFAULT_PLAN_RULES
        )

    def validate_models(self, models: Sequence[Table]) -> None:
        """Run model rules against the desired models."""
        for rule in self.model_rules:
            rule.check(models)

    def validate_plan(self, plan: TablePlan) -> None:
        """Run all plan rules against the compiled `TablePlan`."""
        for rule in self.plan_rules:
            rule.check(plan)
