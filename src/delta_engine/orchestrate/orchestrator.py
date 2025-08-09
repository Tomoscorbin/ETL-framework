from __future__ import annotations
from typing import Sequence
from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.delta_engine.models import Table
from src.delta_engine.state.spark_reader import SparkCatalogReader
from src.delta_engine.compile.planner import Planner
from src.delta_engine.execute.action_runner import ActionRunner

class Orchestrator:
    """End-to-end: desired models -> snapshot -> plan -> execute."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.reader = SparkCatalogReader(spark)
        self.planner = Planner()
        self.runner = ActionRunner(spark)

    def sync_tables(self, desired_tables: Sequence[Table]) -> None:
        LOGGER.info(f"Starting orchestration for {len(desired_tables)} table(s).")

        catalog_state = self.reader.snapshot(desired_tables)

        plan = self.planner.plan(desired_tables=desired_tables, actual_catalog_state=catalog_state)
        LOGGER.info(f"Plan generated — creates: {len(plan.create_tables)}, aligns: {len(plan.align_tables)}")
        
        self.runner.apply(plan)
        LOGGER.info("Orchestration completed.")
