from __future__ import annotations

from typing import ClassVar

from src.delta_engine.actions import TablePlan
from src.delta_engine.validation.rules import (
    NoAddNotNullColumns,
    NoDuplicateAddColumnNamesPerTable,
    NoDuplicateCreateTableColumnNames,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyColumnsNotNull,
    PrimaryKeyNewColumnsMustBeSetNotNull,
    ValidationRule,
)


class PlanValidator:
    """Validates a `TablePlan` by running a fixed set of validation rules."""

    TABLE_RULES: ClassVar[list[ValidationRule]] = [
        NoAddNotNullColumns(),
        NoDuplicateCreateTableColumnNames(),
        NoDuplicateAddColumnNamesPerTable(),
        PrimaryKeyColumnsNotNull(),
        PrimaryKeyAddMustNotMakeColumnsNullable(),
        PrimaryKeyNewColumnsMustBeSetNotNull(),
    ]

    def validate_table_plan(self, plan: TablePlan) -> None:
        """Run all table-plan rules against `plan`, raising on the first violation."""
        for rule in self.TABLE_RULES:
            rule.check(plan)
