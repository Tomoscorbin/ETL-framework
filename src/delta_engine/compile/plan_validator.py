from dataclasses import dataclass
from typing import Self
from src.delta_engine.actions import Plan
from .validation_rules import ValidationRule, NoAddNotNullColumnsRule

@dataclass
class PlanValidator:
    rules: list[ValidationRule]

    @classmethod
    def with_default_rules(cls) -> Self:
        return cls(rules=[
            NoAddNotNullColumnsRule(),
        ])

    def validate(self, plan: Plan) -> None:
        for rule in self.rules:
            rule.check(plan)
