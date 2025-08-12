"""
Executes a table change plan.

This module defines `ActionRunner`, which orchestrates execution of a `Plan`
by delegating to the appropriate executor for each action type.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import TablePlan
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor


class ActionRunner:
    """Runs all actions in a `TablePlan` against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the runner."""
        self.spark = spark
        self._create_executor = CreateExecutor(spark)
        self._align_executor = AlignExecutor(spark)

    def apply(self, plan: TablePlan) -> None:
        """
        Execute all actions in `plan` in a deterministic order.

        The runner applies:
          1) `CreateTable` actions (in listed order),
          2) `AlignTable` actions (in listed order).
        """
        if not plan.create_tables and not plan.align_tables:
            return

        self._apply_creates(plan)
        self._apply_aligns(plan)

    def _apply_creates(self, plan: TablePlan) -> None:
        for action in plan.create_tables:
            self._create_executor.apply(action)

    def _apply_aligns(self, plan: TablePlan) -> None:
        for action in plan.align_tables:
            self._align_executor.apply(action)
