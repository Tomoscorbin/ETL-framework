"""
Execution ports and result types.

- PlanExecutor: protocol for anything that can apply a Plan (Spark SQL, unit-test fakes, etc.)
- ExecutionPolicy: toggles for dry-run and error handling
- ActionResult / ApplyReport: structured outcome to log
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable, Protocol, Tuple

from src.delta_engine.plan.plan_builder import Plan
from src.delta_engine.plan.actions import Action


class ApplyStatus(StrEnum):
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"   # e.g., dry-run with stop_on_first_error after a failure


@dataclass(frozen=True)
class ExecutionPolicy:
    """Controls how the executor behaves."""
    dry_run: bool = False
    stop_on_first_error: bool = True


@dataclass(frozen=True)
class ActionResult:
    """Outcome for a single action."""
    action: Action
    status: ApplyStatus
    message: str  # one line; include rendered SQL in dry-run


@dataclass(frozen=True)
class ApplyReport:
    """Outcome for applying a whole plan."""
    results: Tuple[ActionResult, ...]

    @property
    def ok(self) -> bool:
        return all(r.status == ApplyStatus.OK for r in self.results)

class CreateExecutor:
    def apply(self, action: CreateTable, *, policy: ExecutionPolicy) -> tuple[ActionResult, ...]:
        ...

class AlignExecutor:
    def apply(self, action: Action, *, policy: ExecutionPolicy) -> tuple[ActionResult, ...]:
        ...

class PlanExecutor(Protocol):
    """Executor protocol."""
    def apply(self, plan: Plan, *, policy: ExecutionPolicy = ExecutionPolicy()) -> ApplyReport:
        ...
