"""
High-level orchestration for Delta table synchronization.

This module defines `Orchestrator`, which coordinates the full lifecycle:
reading current catalog state, planning changes, validating the plan, and
executing the resulting actions.
"""

from collections.abc import Sequence

from pyspark.sql import SparkSession

from src.delta_engine.compile.planner import TablePlanner
from src.delta_engine.execute.action_runner import ActionRunner
from src.delta_engine.models import Table
from src.delta_engine.state.catalog_reader import CatalogReader
from src.delta_engine.validation.validator import PlanValidator
from src.logger import LOGGER


class Orchestrator:
    """Coordinates reading, planning, validating, and executing table changes."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the orchestrator."""
        self.spark = spark
        self.reader = CatalogReader(spark)
        self.table_planner = TablePlanner()
        self.validator = PlanValidator()
        self.runner = ActionRunner(spark)

    def sync_tables(self, desired_tables: Sequence[Table]) -> None:
        """
        Synchronize the given tables with Unity Catalog.

        Steps:
          1. Read the current catalog state.
          2. Plan the required changes.
          3. Validate the plan.
          4. Execute the plan if valid.
        """
        LOGGER.info("Starting orchestration for %d table(s).", len(desired_tables))

        catalog_state = self.reader.snapshot(desired_tables)
        table_plan = self.table_planner.build_plan(
            desired_tables=desired_tables, catalog_state=catalog_state
        )

        # Plan validation
        self.validator.validate_table_plan(table_plan)

        LOGGER.info(
            "Plan generated — creates: %d, aligns: %d",
            len(table_plan.create_tables),
            len(table_plan.align_tables),
        )

        # Execute if validation passes
        self.runner.apply_table_plan(table_plan)
        LOGGER.info("Orchestration completed.")
