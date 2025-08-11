from __future__ import annotations

from typing import ClassVar, Sequence

from src.delta_engine.models import Table
from src.delta_engine.actions import TablePlan
from src.delta_engine.validation.rules import (
    NoAddNotNullColumns,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyColumnsNotNull,
    PrimaryKeyNewColumnsMustBeSetNotNull,
    DuplicateColumnNames,
    PrimaryKeyColumnsNotNull,
    ModelRule,
    PlanRule,
)


class PlanValidator:
    """Validates a `TablePlan` by running a fixed set of validation rules."""

    MODEL_RULES: ClassVar[tuple[ModelRule, ...]] = (
        DuplicateColumnNames(),
        PrimaryKeyColumnsNotNull(),
    )

    PLAN_RULES: ClassVar[list[PlanRule]] = [
        NoAddNotNullColumns(),
        PrimaryKeyColumnsNotNull(),
        PrimaryKeyAddMustNotMakeColumnsNullable(),
        PrimaryKeyNewColumnsMustBeSetNotNull(),
    ]

    def validate_models(self, models: Sequence[Table]) -> None:
        """Run model rules against the desired models (no catalog state needed)."""
        for rule in self.MODEL_RULES:
            rule.check(models)

    def validate_table_plan(self, plan: TablePlan) -> None:
        """Run all plan rules against the compiled TablePlan."""
        for rule in self.PLAN_RULES:
            rule.check(plan)