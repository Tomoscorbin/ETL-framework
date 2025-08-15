"""
Validator core: run model/state/plan rules per table, then warnings rules once.

- Keeps rules decoupled via simple Protocols (signatures match exactly what they need).
- No I/O. Caller supplies desired, live state, plan, and snapshot warnings.
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
    code: str
    description: str

    def check(self, desired: DesiredTable) -> list[Diagnostic]: ...


class StateRule(Protocol):
    code: str
    description: str

    def check(self, desired: DesiredTable, live: TableState | None) -> list[Diagnostic]: ...


class PlanRule(Protocol):
    code: str
    description: str

    def check(
        self,
        desired: DesiredTable,
        live: TableState | None,
        planned_actions: tuple[Action, ...],
    ) -> list[Diagnostic]: ...


class WarningsRule(Protocol):
    code: str
    description: str

    def check(self, warnings: tuple[SnapshotWarning, ...]) -> list[Diagnostic]: ...


# ---------- tiny helpers ----------

def _table_key_from_desired(desired: DesiredTable) -> str:
    fq = desired.fully_qualified_table_name
    return format_fully_qualified_table_name_from_parts(fq.catalog, fq.schema, fq.table)


def _table_key_from_action(action: Action) -> str:
    fq = action.table
    return format_fully_qualified_table_name_from_parts(fq.catalog, fq.schema, fq.table)


def _index_actions_by_table(plan: Plan) -> dict[str, tuple[Action, ...]]:
    """
    Build a lookup: 'catalog.schema.table' (unescaped) -> tuple of actions targeting that table.
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
    Runs rules in four stages per table, then warning rules once for all warnings:
      1) Model rules (desired only)
      2) State rules (desired + live)
      3) Plan rules (desired + live + planned actions for that table)
      4) Global warnings rules
    """

    def __init__(
        self,
        model_rules: Iterable[ModelRule] = (),
        state_rules: Iterable[StateRule] = (),
        plan_rules: Iterable[PlanRule] = (),
        warnings_rules: Iterable[WarningsRule] = (),
    ) -> None:
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

        return ValidationReport(diagnostics=tuple(diagnostics))
