"""
High-level orchestration for Delta table synchronization.

`Orchestrator` coordinates the full lifecycle:
  1) Read current catalog state
  2) Plan changes
  3) Validate the plan
  4) Execute the actions

Fail-fast: validation errors bubble up to the caller.
"""

from collections.abc import Sequence

from pyspark.sql import SparkSession

from src.delta_engine.actions import TablePlan
from src.delta_engine.compile.planner import Planner
from src.delta_engine.execute.action_runner import ActionRunner
from src.delta_engine.models import Table
from src.delta_engine.state.catalog_reader import CatalogReader
from src.delta_engine.state.states import CatalogState
from src.delta_engine.validation.validator import Validator
from src.logger import LOGGER


class Orchestrator:
    """Coordinates reading, planning, validating, and executing table changes."""

    def __init__(
        self,
        spark: SparkSession,
        reader: CatalogReader | None = None,
        planner: Planner | None = None,
        validator: Validator | None = None,
        runner: ActionRunner | None = None,
    ) -> None:
        """
        Initialize the orchestrator.

        Custom components can be injected for testing or alternate implementations.
        """
        self.spark: SparkSession = spark
        self.reader: CatalogReader = reader or CatalogReader(spark)
        self.table_planner: Planner = planner or Planner()
        self.validator: Validator = validator or Validator()
        self.runner: ActionRunner = runner or ActionRunner(spark)

    # ---------- public API ----------

    def sync_tables(self, desired_tables: Sequence[Table]) -> None:
        """
        Synchronize the given tables with Unity Catalog.

        Steps:
          1. Read the current catalog state.
          2. Plan the required changes.
          3. Validate the plan.
          4. Execute the plan.
        """
        LOGGER.info("Starting orchestration for %d table(s).", len(desired_tables))
        catalog_state = self._snapshot(desired_tables)
        table_plan = self._compile(desired_tables, catalog_state)
        self._validate(desired_tables, table_plan)
        self._execute(table_plan)
        LOGGER.info("Orchestration completed.")

    # ---------- pipeline stages ----------

    def _snapshot(self, desired_tables: Sequence[Table]) -> CatalogState:
        """Read the current catalog state for the desired tables."""
        return self.reader.snapshot(desired_tables)

    def _compile(self, desired_tables: Sequence[Table], catalog_state: CatalogState) -> TablePlan:
        """Compile a `TablePlan` from desired models and current state."""
        plan = self.table_planner.plan(desired_tables, catalog_state)
        LOGGER.info(
            "Plan generated — create=%d, align=%d",
            len(plan.create_tables),
            len(plan.align_tables),
        )
        return plan

    def _validate(self, desired_tables: Sequence[Table], table_plan: TablePlan) -> None:
        """Run model and plan validation (fail-fast on first violation)."""
        self.validator.validate_models(desired_tables)
        self.validator.validate_plan(table_plan)

    def _execute(self, table_plan: TablePlan) -> None:
        """Execute the compiled plan."""
        self.runner.apply(table_plan)
