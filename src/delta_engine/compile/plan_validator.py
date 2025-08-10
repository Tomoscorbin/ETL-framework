"""
Plan validation orchestration.

This module defines `PlanValidator`, which runs all registered
validation rules against a `Plan` before execution to ensure
it meets defined safety constraints.
"""

from dataclasses import dataclass
from typing import ClassVar

from src.delta_engine.actions import Plan

from .validation_rules import NoAddNotNullColumnsRule, ValidationRule


@dataclass
class PlanValidator:
    """Validates a plan against a set of safety rules."""

    rules: ClassVar[list[ValidationRule]] = [
        NoAddNotNullColumnsRule(),
    ]

    def validate(self, plan: Plan) -> None:
        """Run all validation rules against the plan."""
        for rule in self.rules:
            rule.check(plan)
