from __future__ import annotations
from typing import ClassVar, Sequence, Tuple

from src.delta_engine.models.table import Table
from .validation_rules import (
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


class ConstraintsValidator:
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