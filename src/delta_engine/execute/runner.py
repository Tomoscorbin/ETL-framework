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
        """Apply the plan: creates first, then aligns.."""
        self._apply_creates(plan)
        self._apply_aligns(plan)

    # ----- steps -----

    def _apply_creates(self, plan: TablePlan) -> None:
        for action in plan.actions:
            if isinstance(action, CreateTable):
                self._create_executor.apply(action)

    def _apply_aligns(self, plan: TablePlan) -> None:
        for action in plan.actions:
            if isinstance(action, AlignTable):
                self._align_executor.apply(action)
