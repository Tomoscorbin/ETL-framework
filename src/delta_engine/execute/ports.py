"""
Execution ports and result types.

- PlanExecutor: protocol for anything that can apply a Plan (Spark SQL, fakes, etc.)
- ExecutionPolicy: toggles for dry-run and error handling
- ActionResult / ApplyReport: structured outcomes to log or surface upstream
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from src.delta_engine.plan.actions import Action, AlignTable, CreateTable
from src.delta_engine.plan.plan_builder import Plan


class ApplyStatus(StrEnum):
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"  # e.g., dry-run or short-circuited after a failure


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

    results: tuple[ActionResult, ...]

    @property
    def ok(self) -> bool:
        return all(result.status == ApplyStatus.OK for result in self.results)


class CreateExecutor(Protocol):
    """Executor capable of applying a CreateTable action."""

    def apply(
        self, action: CreateTable, *, policy: ExecutionPolicy
    ) -> tuple[ActionResult, ...]: ...


class AlignExecutor(Protocol):
    """Executor capable of applying an AlignTable action (coalesced sub-actions)."""

    def apply(self, action: AlignTable, *, policy: ExecutionPolicy) -> tuple[ActionResult, ...]: ...


class PlanExecutor(Protocol):
    """High-level executor that applies an entire Plan."""

    def apply(self, plan: Plan, *, policy: ExecutionPolicy) -> ApplyReport: ...
