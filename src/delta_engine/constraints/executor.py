from __future__ import annotations
from pyspark.sql import SparkSession
from src.delta_engine.constraints.actions import ConstraintPlan
from src.delta_engine.constraints.sql_renderer import render_plan

class ConstraintExecutor:
    """Executes constraint plans; delegates SQL rendering to sql_renderer."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def to_sql(self, plan: ConstraintPlan) -> list[str]:
        """Return the SQL that would be executed (for dry-run/review)."""
        return render_plan(plan)

    def apply(self, plan: ConstraintPlan) -> None:
        """Execute the plan in order. Assumes prior structural validation."""
        for statement in render_plan(plan):
            self.spark.sql(statement)
