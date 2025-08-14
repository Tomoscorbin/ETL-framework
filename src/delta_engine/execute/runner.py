from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.plan.plan_builder import Plan
from src.delta_engine.plan.actions import CreateTable, AlignTable, Action


@dataclass(frozen=True)
class TablePlan:
    """A plan split into creation vs alignment phases."""
    create_tables: Tuple[CreateTable, ...]
    align_tables: Tuple[AlignTable, ...]


def split_plan(plan: Plan) -> TablePlan:
    """
    Split a flat Plan into (creates, aligns) while preserving order.
    Assumes Plan.actions is already deterministically ordered by the planner.
    """
    creates: list[CreateTable] = []
    aligns: list[AlignTable] = []
    for action in plan.actions:
        if isinstance(action, CreateTable):
            creates.append(action)
        elif isinstance(action, AlignTable):
            aligns.append(action)
        else:
            # If you still have granular actions (AddColumns, …),
            # either wrap them into AlignTable earlier, or handle here.
            raise TypeError(f"Unexpected action type for runner: {type(action).__name__}")
    return TablePlan(create_tables=tuple(creates), align_tables=tuple(aligns))


class PlanRunner:
    """
    Execute all actions in `TablePlan` in a deterministic order:

      1) CreateTable actions (in listed order),
      2) AlignTable actions (in listed order).

    This class is deliberately thin: no Spark dependency and no policy flags.
    It delegates all work to the injected executors.
    """

    def __init__(self, create_executor: CreateExecutor, align_executor: AlignExecutor) -> None:
        self._create_executor = create_executor
        self._align_executor = align_executor

    def apply(self, plan: TablePlan) -> None:
        """Apply the plan: creates first, then aligns. No return; executors raise on failure."""
        if not plan.create_tables and not plan.align_tables:
            return
        self._apply_creates(plan)
        self._apply_aligns(plan)

    # ----- steps -----

    def _apply_creates(self, plan: TablePlan) -> None:
        for action in plan.create_tables:
            self._create_executor.apply(action)

    def _apply_aligns(self, plan: TablePlan) -> None:
        for action in plan.align_tables:
            self._align_executor.apply(action)
