"""
Validator core: run model/state/plan rules per table, then warnings rules once.

Responsibilities
----------------
- Keep rules decoupled via simple Protocols (each rule receives only what it needs).
- Perform no I/O. Caller supplies desired spec, observed state, a plan, and snapshot warnings.
- Produce a ValidationReport (immutable) with a convenience .ok flag.

Conventions
-----------
- Table keys are unquoted "catalog.schema.table" strings generated via identifiers helpers.
- No abbreviations in rule codes/messages (enforced by rule implementations).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.identifiers import format_fully_qualified_table_name_from_parts
from src.delta_engine.plan.actions import Action
from src.delta_engine.plan.plan_builder import Plan
from src.delta_engine.state.ports import SnapshotWarning
from src.delta_engine.state.states import CatalogState, TableState
from src.delta_engine.validation.diagnostics import Diagnostic, ValidationReport

# ---------- rule protocols ----------


class ModelRule(Protocol):
    """
    A model-only rule.

    Contract
    --------
    - Receives the desired table specification only.
    - Returns zero or more diagnostics; must not raise for normal invalid input.
    """

    code: str
    description: str

    def check(self, desired: DesiredTable) -> list[Diagnostic]: ...


class StateRule(Protocol):
    """
    A rule that considers desired specification and live state.

    Contract
    --------
    - Receives desired spec and the matching live TableState (or None).
    - Returns zero or more diagnostics.
    """

    code: str
    description: str

    def check(self, desired: DesiredTable, live: TableState | None) -> list[Diagnostic]: ...


class PlanRule(Protocol):
    """
    A rule that considers desired, live, and planned actions for one table.

    Contract
    --------
    - Receives desired spec, live state (or None), and the tuple of actions
      planned for that specific table.
    - Returns zero or more diagnostics.
    """

    code: str
    description: str

    def check(
        self,
        desired: DesiredTable,
        live: TableState | None,
        planned_actions: tuple[Action, ...],
    ) -> list[Diagnostic]: ...


class WarningsRule(Protocol):
    """
    A global rule that converts snapshot warnings into diagnostics.

    Contract
    --------
    - Receives all snapshot warnings (for all tables).
    - Returns zero or more diagnostics.
    """

    code: str
    description: str

    def check(self, warnings: tuple[SnapshotWarning, ...]) -> list[Diagnostic]: ...


# ---------- tiny helpers (single-purpose) ----------


def _table_key_from_desired(desired: DesiredTable) -> str:
    """Render the unquoted table key 'catalog.schema.table' from a DesiredTable."""
    fq = desired.fully_qualified_table_name
    key = format_fully_qualified_table_name_from_parts(fq.catalog, fq.schema, fq.table)
    return key


def _table_key_from_action(action: Action) -> str:
    """Render the unquoted table key 'catalog.schema.table' from an Action."""
    fq = action.table
    key = format_fully_qualified_table_name_from_parts(fq.catalog, fq.schema, fq.table)
    return key


def _index_actions_by_table(plan: Plan) -> dict[str, tuple[Action, ...]]:
    """
    Build a lookup from unquoted table key -> tuple of actions for that table.

    The key shape is 'catalog.schema.table'. Preserves the input order of actions per table.
    """
    grouped: dict[str, list[Action]] = {}
    for action in plan.actions:
        key = _table_key_from_action(action)
        grouped.setdefault(key, []).append(action)

    by_table: dict[str, tuple[Action, ...]] = {}
    for key, items in grouped.items():
        actions_tuple = tuple(items)
        by_table[key] = actions_tuple
    return by_table


# ---------- validator orchestrator ----------


class Validator:
    """
    Orchestrates validation in four stages per table, then applies warnings rules once:

      1) Model rules        (desired only)
      2) State rules        (desired + live)
      3) Plan rules         (desired + live + planned actions for that table)
      4) Warnings rules     (all snapshot warnings at once)

    This class performs no I/O and has no Spark or SQL dependencies.
    """

    def __init__(
        self,
        model_rules: Iterable[ModelRule] = (),
        state_rules: Iterable[StateRule] = (),
        plan_rules: Iterable[PlanRule] = (),
        warnings_rules: Iterable[WarningsRule] = (),
    ) -> None:
        """
        Construct a Validator with explicit rule sets.

        All iterables are materialised to tuples for determinism.
        """
        self._model_rules = tuple(model_rules)
        self._state_rules = tuple(state_rules)
        self._plan_rules = tuple(plan_rules)
        self._warnings_rules = tuple(warnings_rules)

    def validate(
        self,
        desired: DesiredCatalog,
        live: CatalogState,
        plan: Plan,
        snapshot_warnings: tuple[SnapshotWarning, ...] = (),
    ) -> ValidationReport:
        """
        Run all configured rules and return a ValidationReport.

        Parameters
        ----------
        desired:
            Desired catalog specification (tables in scope).
        live:
            Observed catalog state keyed by unquoted 'catalog.schema.table'.
        plan:
            Ordered sequence of actions to be applied.
        snapshot_warnings:
            Warnings produced while snapshotting (optional).

        Returns:
        -------
        ValidationReport
            Immutable collection of diagnostics with a convenience .ok flag.
        """
        diagnostics: list[Diagnostic] = []
        actions_by_table_key = _index_actions_by_table(plan)

        for desired_table in desired.tables:
            table_key = _table_key_from_desired(desired_table)
            live_state = live.tables.get(table_key)
            planned_actions = actions_by_table_key.get(table_key, ())

            # 1) model-only
            for rule in self._model_rules:
                findings = rule.check(desired_table)
                diagnostics.extend(findings)

            # 2) state-aware
            for rule in self._state_rules:
                findings = rule.check(desired_table, live_state)
                diagnostics.extend(findings)

            # 3) plan-aware
            for rule in self._plan_rules:
                findings = rule.check(desired_table, live_state, planned_actions)
                diagnostics.extend(findings)

        # 4) snapshot warnings (global)
        if snapshot_warnings and self._warnings_rules:
            for rule in self._warnings_rules:
                findings = rule.check(snapshot_warnings)
                diagnostics.extend(findings)

        report = ValidationReport(diagnostics=tuple(diagnostics))
        return report
