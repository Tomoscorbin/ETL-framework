from typing import Sequence
from pyspark.sql import SparkSession
from src.logger import LOGGER
from src.delta_engine.models import Table
from src.delta_engine.state.spark_reader import SparkCatalogReader
from src.delta_engine.compile.planner import Planner
from src.delta_engine.compile.plan_validator import PlanValidator
from src.delta_engine.execute.action_runner import ActionRunner

class Orchestrator:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.reader = SparkCatalogReader(spark)
        self.planner = Planner()
        self.validator = PlanValidator()
        self.runner = ActionRunner(spark)

    def sync_tables(self, desired_tables: Sequence[Table]) -> None:
        LOGGER.info("Starting orchestration for %d table(s).", len(desired_tables))

        catalog_state = self.reader.snapshot(desired_tables)
        plan = self.planner.plan(desired_tables=desired_tables, catalog_state=catalog_state)
        LOGGER.info("Plan generated — creates: %d, aligns: %d", len(plan.create_tables), len(plan.align_tables))

        # Plan validation
        self.validator.with_default_rules().validate(plan)

        # Execute if validation passes
        self.runner.apply(plan)
        LOGGER.info("Orchestration completed.")
