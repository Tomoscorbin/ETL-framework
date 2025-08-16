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

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.execute.ports import ApplyReport, ExecutionPolicy
from src.delta_engine.execute.runner import PlanRunner, TablePlan
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.plan.actions import Action, AlignTable, CreateTable
from src.delta_engine.plan.differ import Differ, DiffOptions
from src.delta_engine.plan.plan_builder import Plan, PlanBuilder
from src.delta_engine.state.adapters.catalog_reader import CatalogReader
from src.delta_engine.state.ports import Aspect, SnapshotPolicy, SnapshotRequest, SnapshotResult
from src.delta_engine.state.states import CatalogState
from src.delta_engine.validation.diagnostics import ValidationReport
from src.delta_engine.validation.validator import Validator

# ---------- orchestration inputs/outputs ----------


@dataclass(frozen=True)
class OrchestratorOptions:
    """
    Orchestrator toggles for a single run.

    aspects:
        Which slices of metadata to manage (schema, comments, properties, primary key).
    snapshot_policy:
        STRICT → raise on snapshot warnings; PERMISSIVE → proceed with warnings.
    execute:
        If False, stop after planning+validation (no changes applied).
    fail_on_validation_errors:
        If True, block execution when validation has any ERROR.
    execution_policy:
        How to apply actions (dry-run, stop-on-first-error).
    """

    aspects: frozenset[Aspect]
    snapshot_policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE
    execute: bool = True
    fail_on_validation_errors: bool = True
    execution_policy: ExecutionPolicy = ExecutionPolicy()


@dataclass(frozen=True)
class OrchestrationReport:
    """Everything a caller would want to inspect or log from a single run."""

    snapshot: SnapshotResult
    plan: Plan
    validation: ValidationReport
    apply_report: ApplyReport | None = None


# ---------- orchestrator ----------


class Orchestrator:
    """
    Glue for snapshot → plan → validate → (optional) execute.

    This class does not do any SQL or Spark operations itself; it delegates to injected components.
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
            raise RuntimeError(
                f"Snapshot produced {len(snapshot.warnings)} warning(s) under STRICT policy"
            )

        plan = self._plan(desired, snapshot.state, options.aspects)
        validation = self._validate(desired, snapshot.state, plan, snapshot.warnings)

        apply_report: ApplyReport | None = None
        if options.execute and (validation.ok or not options.fail_on_validation_errors):
            apply_report = self._execute(plan, options.execution_policy)

        return OrchestrationReport(
            snapshot=snapshot, plan=plan, validation=validation, apply_report=apply_report
        )

    # ----- steps -----

    def _snapshot(
        self,
        desired: DesiredCatalog,
        aspects: frozenset[Aspect],
        policy: SnapshotPolicy,
    ) -> SnapshotResult:
        """Ask the CatalogReader for the live state of the requested tables."""
        tables: tuple[FullyQualifiedTableName, ...] = tuple(
            t.full_table_name for t in desired.tables
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
        warnings_tuple = tuple(snapshot_warnings)
        return self._validator.validate(
            desired=desired,
            live=live,
            plan=plan,
            snapshot_warnings=warnings_tuple,
        )

    def _execute(self, plan: Plan, policy: ExecutionPolicy) -> ApplyReport:
        """Create first, then align (per-table). Uses PlanRunner which delegates to executors."""
        table_plan = _to_table_plan(plan)
        runner = PlanRunner(self._create_executor, self._align_executor)
        return runner.apply(table_plan, policy=policy)


# ---------- tiny helpers ----------


def _to_table_plan(plan: Plan) -> TablePlan:
    """Split a flat Plan into a TablePlan with (create_tables, align_tables)."""
    create_list: list[CreateTable] = []
    align_list: list[AlignTable] = []
    for action in plan.actions:
        if isinstance(action, CreateTable):
            create_list.append(action)
        elif isinstance(action, AlignTable):
            align_list.append(action)
        else:
            align_list.append(action)  # type: ignore[arg-type]
    create_tables = tuple(create_list)
    align_tables = tuple(align_list)  # type: ignore[assignment]
    return TablePlan(create_tables=create_tables, align_tables=align_tables)
