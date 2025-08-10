"""
Executes a table change plan.

This module defines `ActionRunner`, which orchestrates execution of a `Plan`
by delegating to the appropriate executor for each action type.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import TablePlan, ConstraintPlan
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.constraint_executor import ConstraintExecutor


class ActionRunner:
    """Runs all actions in a `TablePlan` against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the runner."""
        self.spark = spark

    def apply_table_plan(self, plan: TablePlan) -> None:
        """Execute all actions in the given plan."""
        create_executor = CreateExecutor(self.spark)
        align_executor = AlignExecutor(self.spark)

        for create_action in plan.create_tables:
            create_executor.apply(create_action)

        for align_action in plan.align_tables:
            align_executor.apply(align_action)

    def apply_constraint_plan(self, plan: ConstraintPlan):
        constraint_executor = ConstraintExecutor(self.spark)
        constraint_executor.apply(plan)
