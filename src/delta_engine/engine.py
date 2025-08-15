"""
Engine: high-level entry point for the Delta engine.

Responsibilities
----------------
- Wire default components (catalog reader, differ, plan builder, validator, executors).
- Expose a single entry point to run the full orchestration:
    - run(desired_catalog, options)

Notes:
-----
- No SQL or Spark logic here; work is delegated to injected components.
- Defaults are provided, but everything can be overridden for testing or custom behaviour.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.orchestrator import OrchestrationReport, Orchestrator, OrchestratorOptions
from src.delta_engine.plan.differ import Differ
from src.delta_engine.plan.plan_builder import PlanBuilder
from src.delta_engine.state.adapters.catalog_reader import CatalogReader
from src.delta_engine.validation.rules import default_rule_set
from src.delta_engine.validation.validator import Validator


class Engine:
    """
    High-level entry point for the Delta engine.

    You can:
      - pass your own components (for custom behaviour), or
      - rely on defaults (simple, batteries included).

    Method:
      - run(desired_catalog, options)
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog_reader: CatalogReader | None = None,
        differ: Differ | None = None,
        plan_builder: PlanBuilder | None = None,
        validator: Validator | None = None,
        create_executor: CreateExecutor | None = None,
        align_executor: AlignExecutor | None = None,
    ) -> None:
        self.spark = spark

        # Wire defaults if not supplied
        self.catalog_reader = catalog_reader or CatalogReader(spark)
        self.differ = differ or Differ()
        self.plan_builder = plan_builder or PlanBuilder()

        if validator is None:
            model_rules, state_rules, plan_rules, warnings_rules = default_rule_set()
            self.validator = Validator(
                model_rules=model_rules,
                state_rules=state_rules,
                plan_rules=plan_rules,
                warnings_rules=warnings_rules,
            )
        else:
            self.validator = validator

        self.create_executor = create_executor or CreateExecutor(spark)
        self.align_executor = align_executor or AlignExecutor(spark)

        # Orchestrator (glue)
        self.orchestrator = Orchestrator(
            catalog_reader=self.catalog_reader,
            differ=self.differ,
            plan_builder=self.plan_builder,
            validator=self.validator,
            create_executor=self.create_executor,
            align_executor=self.align_executor,
        )

    def run(self, desired: DesiredCatalog, options: OrchestratorOptions) -> OrchestrationReport:
        """Engine-native path: run from a DesiredCatalog."""
        return self.orchestrator.run(desired, options)
