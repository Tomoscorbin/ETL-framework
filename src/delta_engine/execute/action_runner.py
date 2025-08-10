"""
Executes a table change plan.

This module defines `ActionRunner`, which orchestrates execution of a `Plan`
by delegating to the appropriate executor for each action type.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import Plan
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor


class ActionRunner:
    """Runs all actions in a `Plan` against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the runner."""
        self.spark = spark

    def apply(self, plan: Plan) -> None:
        """Execute all actions in the given plan."""
        create_executor = CreateExecutor(self.spark)
        align_executor = AlignExecutor(self.spark)

        for create_action in plan.create_tables:
            create_executor.apply(create_action)

        for align_action in plan.align_tables:
            align_executor.apply(align_action)
