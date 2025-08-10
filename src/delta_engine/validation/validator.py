from __future__ import annotations
from typing import ClassVar, Iterable

from src.delta_engine.actions import TablePlan
from src.delta_engine.validation.rules import (
    NoAddNotNullColumns,
    NoDuplicateCreateTableColumnNames,
    NoDuplicateAddColumnNamesPerTable,
    PrimaryKeyColumnsNotNull,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyNewColumnsMustBeSetNotNull,
)

class PlanValidator:
    """Validates a TablePlan using dedicated rules."""

    TABLE_RULES: ClassVar[tuple] = (
        NoAddNotNullColumns(),
        NoDuplicateCreateTableColumnNames(),
        NoDuplicateAddColumnNamesPerTable(),
        PrimaryKeyColumnsNotNull(),
        PrimaryKeyAddMustNotMakeColumnsNullable(),
        PrimaryKeyNewColumnsMustBeSetNotNull(),
    )

    def validate_table_plan(self, plan: TablePlan) -> None:
        self._run_rules(plan, self.TABLE_RULES)

    @staticmethod
    def _run_rules(plan: TablePlan, rules: Iterable) -> None:
        for rule in rules:
            rule.check(plan)
