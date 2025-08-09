from pyspark.sql import SparkSession
from src.delta_engine.actions import Plan
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.align_executor import AlignExecutor


class ActionRunner:
    """Runs a Plan end-to-end."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def apply(self, plan: Plan) -> None:
        create_executor = CreateExecutor(self.spark)
        align_executor  = AlignExecutor(self.spark)

        for action in plan.create_tables:
            create_executor.apply(action)

        for action in plan.align_tables:
            align_executor.apply(action)
