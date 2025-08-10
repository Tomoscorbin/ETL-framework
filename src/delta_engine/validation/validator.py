from __future__ import annotations
from typing import ClassVar, Iterable
from src.delta_engine.actions import TablePlan, ConstraintPlan
from src.delta_engine.validation.rules import (
    TablePlan, 
    ConstraintPlan,
    NoEmptyPrimaryKeyColumns,
    ForeignKeyArityMatches,
    UniqueForeignKeyNamesWithinPlan,
    NoAddNotNullColumns,
    NoDuplicateCreateTableColumnNames,
    NoDuplicateAddColumnNamesPerTable,
    PrimaryKeyColumnsNotNull,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyNewColumnsMustBeSetNotNull
)

class PlanValidator:
    """Validates TablePlan and ConstraintPlan using dedicated rule buckets."""

    TABLE_RULES: ClassVar[tuple[TablePlanRule, ...]] = (
        NoAddNotNullColumns(),
        NoDuplicateCreateTableColumnNames(),
        NoDuplicateAddColumnNamesPerTable(),
        PrimaryKeyColumnsNotNull(),
        PrimaryKeyAddMustNotMakeColumnsNullable(),
        PrimaryKeyNewColumnsMustBeSetNotNull()
    )

    # CONSTRAINT_RULES: ClassVar[tuple[ConstraintPlanRule, ...]] = (
    #     NoEmptyPrimaryKeyColumns(),
    #     ForeignKeyArityMatches(),
    #     UniqueForeignKeyNamesWithinPlan(),
    #     PrimaryKeyColumnsDeclaredNotNullable(),
    # )

    def validate_table_plan(self, plan: TablePlan) -> None:
        self._run_rules(plan, self.TABLE_RULES)

    # def validate_constraint_plan(self, plan: ConstraintPlan) -> None:
    #     self._run_rules(plan, self.CONSTRAINT_RULES)

    @staticmethod
    def _run_rules(plan: TablePlan | ConstraintPlan, rules: Iterable) -> None:
        for rule in rules:
            rule.check(plan)
