from __future__ import annotations
from pyspark.sql import SparkSession
from src.delta_engine.actions import ConstraintPlan
from src.delta_engine.execute.sql_renderer import render_plan

class ConstraintExecutor:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def to_sql(self, plan: ConstraintPlan) -> list[str]:
        return render_plan(plan)

    def apply(self, plan: ConstraintPlan) -> None:
        for stmt in render_plan(plan):
            self.spark.sql(stmt)
