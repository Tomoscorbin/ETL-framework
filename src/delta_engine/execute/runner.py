"""
Plan Runner

Purpose
-------
Execute a table plan in two deterministic phases:
  1) CreateTable actions (in listed order)
  2) AlignTable actions (in listed order)

Design
------
- No SQL rendering here. All DDL is delegated to DDLExecutor via the injected executors.
- Respects ExecutionPolicy:
  - dry_run=True: sub-executors return SKIPPED ActionResults with descriptive messages.
  - stop_on_first_error=True: after the first FAILED result, remaining actions are
    marked SKIPPED with a short-circuit message.
- Returns an ApplyReport aggregating all per-action results.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.ports import (
    ActionResult,
    ApplyReport,
    ApplyStatus,
    ExecutionPolicy,
)
from src.delta_engine.plan.actions import AlignTable, CreateTable


@dataclass(frozen=True)
class TablePlan:
    """A plan split into creation vs. alignment phases."""

    create_tables: tuple[CreateTable, ...]
    align_tables: tuple[AlignTable, ...]


class PlanRunner:
    """
    Execute all actions in `TablePlan` in a deterministic order:
      1) CreateTable actions
      2) AlignTable actions

    Thin orchestration: no Spark dependency here; executors are injected.
    """

    def __init__(self, create_executor: CreateExecutor, align_executor: AlignExecutor) -> None:
        self._create_executor = create_executor
        self._align_executor = align_executor

    def apply(self, plan: TablePlan, *, policy: ExecutionPolicy) -> ApplyReport:
        """Apply the plan and return an aggregated ApplyReport."""
        results: list[ActionResult] = []

        # Phase 1: creates
        for action in plan.create_tables:
            action_results = self._create_executor.apply(action, policy=policy)
            results.extend(action_results)
            if policy.stop_on_first_error and any(
                r.status == ApplyStatus.FAILED for r in action_results
            ):
                results.extend(self._skip_remaining(plan.align_tables))
                return ApplyReport(results=tuple(results))

        # Phase 2: aligns
        for action in plan.align_tables:
            action_results = self._align_executor.apply(action, policy=policy)
            results.extend(action_results)
            if policy.stop_on_first_error and any(
                r.status == ApplyStatus.FAILED for r in action_results
            ):
                # Nothing left to run after an align failure, but keep symmetry.
                return ApplyReport(results=tuple(results))

        return ApplyReport(results=tuple(results))

    # ---------- helpers ----------

    @staticmethod
    def _skip_remaining(actions: tuple[AlignTable, ...]) -> list[ActionResult]:
        """Create SKIPPED stubs for remaining actions after a failure when short-circuiting."""
        skipped: list[ActionResult] = []
        for action in actions:
            skipped.append(
                ActionResult(
                    action=action,
                    status=ApplyStatus.SKIPPED,
                    message="Skipped due to previous failure (stop_on_first_error)",
                )
            )
        return skipped
