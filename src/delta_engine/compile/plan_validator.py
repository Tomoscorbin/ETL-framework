from dataclasses import dataclass
from typing import ClassVar
from src.delta_engine.actions import Plan
from .validation_rules import ValidationRule, NoAddNotNullColumnsRule

@dataclass
class PlanValidator:
    rules: ClassVar[list[ValidationRule]] = [
        NoAddNotNullColumnsRule(),
    ]

    def validate(self, plan: Plan) -> None:
        for rule in self.rules:
            rule.check(plan)
