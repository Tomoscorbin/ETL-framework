"""
End-to-end orchestration for the Delta engine.

Flow (one pass):
  1) Snapshot live state for the requested tables.
  2) Diff desired vs live into actions; order into a plan.
  3) Run validation rules (model/state/plan + snapshot warnings).
  4) Optionally execute: create tables first, then align tables.

Design goals:
- Clear boundaries: snapshot, plan, validate, execute are separate steps.
- No SQL here, no catalog plumbing: this file glues components together.
- Accepts dependencies via constructor for testability and decoupling.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Tuple

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.ports import Aspect, SnapshotPolicy, SnapshotRequest, SnapshotResult
from src.delta_engine.state.states import CatalogState
from src.delta_engine.state.adapters.catalog_reader import CatalogReader

from src.delta_engine.plan.actions import Action
from src.delta_engine.plan.plan_builder import Plan, PlanBuilder
from src.delta_engine.plan.differ import Differ, DiffOptions
from src.delta_engine.validation.validator import Validator
from src.delta_engine.validation.diagnostics import ValidationReport

from src.delta_engine.exec.create_executor import CreateExecutor
from src.delta_engine.exec.align_executor import AlignExecutor
from src.delta_engine.exec.runner import TablePlan, split_plan, PlanRunner


# ---------- orchestration inputs/outputs ----------

@dataclass(frozen=True)
class OrchestratorOptions:
    """
    Orchestrator toggles for a single run.
    - aspects: which slices of metadata to manage (schema, comments, properties, primary key)
    - snapshot_policy: STRICT → raise on snapshot warnings; PERMISSIVE → proceed with warnings
    - execute: if False, stop after planning+validation (no changes applied)
    - fail_on_validation_errors: if True, block execution when validation has any ERROR
    """
    aspects: frozenset[Aspect]
    snapshot_policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE
    execute: bool = True
    fail_on_validation_errors: bool = True


@dataclass(frozen=True)
class OrchestrationReport:
    """Everything a caller would want to inspect or log from a single run."""
    snapshot: SnapshotResult
    plan: Plan
    validation: ValidationReport

# ---------- orchestrator ----------

class Orchestrator:
    """
    Glue for snapshot → plan → validate → (optional) execute.

    This class does not do any I/O other than calling the injected components.
    """

    def __init__(
        self,
        catalog_reader: CatalogReader,
        differ: Differ,
        plan_builder: PlanBuilder,
        validator: Validator,
        create_executor: CreateExecutor,
        align_executor: AlignExecutor,
    ) -> None:
        self._catalog_reader = catalog_reader
        self._differ = differ
        self._plan_builder = plan_builder
        self._validator = validator
        self._create_executor = create_executor
        self._align_executor = align_executor

    # ----- public API -----

    def run(self, desired: DesiredCatalog, options: OrchestratorOptions) -> OrchestrationReport:
        """Run the full flow once; optionally executes changes."""
        snapshot = self._snapshot(desired, options.aspects, options.snapshot_policy)

        if options.snapshot_policy is SnapshotPolicy.STRICT and snapshot.warnings:
            # Strict means: do not even plan if snapshot had warnings (e.g., permissions)
            raise RuntimeError(f"Snapshot produced {len(snapshot.warnings)} warning(s) under STRICT policy")

        plan = self._plan(desired, snapshot.state, options.aspects)
        validation = self._validate(desired, snapshot.state, plan, snapshot.warnings)

        if options.execute and (not options.fail_on_validation_errors or validation.ok):
            self._execute(plan)

        return OrchestrationReport(snapshot=snapshot, plan=plan, validation=validation)

    # ----- steps (tiny, single-purpose) -----

    def _snapshot(
        self,
        desired: DesiredCatalog,
        aspects: frozenset[Aspect],
        policy: SnapshotPolicy,
    ) -> SnapshotResult:
        """Ask the CatalogReader for the live state of the requested tables."""
        tables: Tuple[FullyQualifiedTableName, ...] = tuple(
            t.fully_qualified_table_name for t in desired.tables
        )
        request = SnapshotRequest(tables=tables, aspects=aspects, policy=policy)
        return self._catalog_reader.snapshot(request)

    def _plan(
        self,
        desired: DesiredCatalog,
        live: CatalogState,
        aspects: frozenset[Aspect],
    ) -> Plan:
        """Diff desired vs live into actions, then order them deterministically."""
        options = DiffOptions(
            manage_schema=(Aspect.SCHEMA in aspects),
            manage_table_comment=(Aspect.COMMENTS in aspects),
            manage_properties=(Aspect.PROPERTIES in aspects),
            manage_primary_key=(Aspect.PRIMARY_KEY in aspects),
        )
        actions: list[Action] = self._differ.diff(desired=desired, live=live, options=options)
        return self._plan_builder.build(actions)

    def _validate(
        self,
        desired: DesiredCatalog,
        live: CatalogState,
        plan: Plan,
        snapshot_warnings,
    ) -> ValidationReport:
        """Run model/state/plan rules and transform snapshot warnings into diagnostics."""
        return self._validator.validate(
            desired=desired,
            live=live,
            plan=plan,
            snapshot_warnings=tuple(snapshot_warnings),
        )

    def _execute(self, plan: Plan) -> None:
        """Create first, then align (per-table)."""
        table_plan: TablePlan = split_plan(plan)
        runner = PlanRunner(self._create_executor, self._align_executor)
        runner.apply(table_plan)
