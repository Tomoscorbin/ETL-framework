from __future__ import annotations
from typing import Iterable

from pyspark.sql import SparkSession

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.state.adapters.catalog_reader import CatalogReader
from src.delta_engine.state.ports import Aspect, SnapshotPolicy
from src.delta_engine.plan.differ import Differ, DiffOptions
from src.delta_engine.plan.plan_builder import PlanBuilder
from src.delta_engine.validation.validator import Validator
from src.delta_engine.validation.rules import default_rule_set
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.orchestrator import Orchestrator, OrchestratorOptions, OrchestrationReport
from src.delta_engine.models import Table


class Engine:
    """
    High-level entry point for the Delta engine.

    You can:
      - pass your own components (for custom behavior), or
      - rely on defaults (simple, batteries-included).

    Methods:
      - run(desired_catalog, options)
      - run_from_tables(tables, options, mapping)
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog_reader: Optional[CatalogReader] = None,
        differ: Optional[Differ] = None,
        plan_builder: Optional[PlanBuilder] = None,
        validator: Optional[Validator] = None,
        create_executor: Optional[CreateExecutor] = None,
        align_executor: Optional[AlignExecutor] = None,
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

        # One orchestrator to rule them all
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
