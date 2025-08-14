"""
Validator core: run model/state/plan rules per table, then warnings rules once.

- Keeps rules decoupled via simple Protocols (signatures match exactly what they need).
- No I/O. Caller supplies desired, live state, plan, and snapshot warnings.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Protocol, Tuple

from src.delta_engine.identifiers import fully_qualified_name_to_string
from src.delta_engine.desired.models import DesiredCatalog, DesiredTable
from src.delta_engine.state.states import CatalogState, TableState
from src.delta_engine.state.ports import SnapshotWarning
from src.delta_engine.plan.plan_builder import Plan
from src.delta_engine.plan.actions import Action
from src.delta_engine.validation.diagnostics import Diagnostic, DiagnosticLevel, ValidationReport


# ---------- rule protocols ----------

class ModelRule(Protocol):
    code: str
    description: str
    def check(self, desired: DesiredTable) -> List[Diagnostic]: ...


class StateRule(Protocol):
    code: str
    description: str
    def check(self, desired: DesiredTable, live: TableState | None) -> List[Diagnostic]: ...


class PlanRule(Protocol):
    code: str
    description: str
    def check(
        self,
        desired: DesiredTable,
        live: TableState | None,
        planned_actions: Tuple[Action, ...],
    ) -> List[Diagnostic]: ...


class WarningsRule(Protocol):
    code: str
    description: str
    def check(self, warnings: Tuple[SnapshotWarning, ...]) -> List[Diagnostic]: ...


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
        snapshot_warnings: Tuple[SnapshotWarning, ...] = (),
    ) -> ValidationReport:
        diagnostics: List[Diagnostic] = []
        actions_by_table_key = _index_actions_by_table(plan)

        for desired_table in desired.tables:
            table_key = fully_qualified_name_to_string(desired_table.fully_qualified_table_name)
            live_state = live.tables.get(table_key)
            planned_actions = actions_by_table_key.get(table_key, ())

            # 1) model-only
            for rule in self._model_rules:
                diagnostics.extend(rule.check(desired_table))

            # 2) state-aware
            for rule in self._state_rules:
                diagnostics.extend(rule.check(desired_table, live_state))

            # 3) plan-aware
            for rule in self._plan_rules:
                diagnostics.extend(rule.check(desired_table, live_state, planned_actions))

        # 4) snapshot warnings (global)
        if snapshot_warnings and self._warnings_rules:
            for rule in self._warnings_rules:
                diagnostics.extend(rule.check(snapshot_warnings))

        return ValidationReport(diagnostics=tuple(diagnostics))


# ---------- tiny helper ----------

def _index_actions_by_table(plan: Plan) -> Dict[str, Tuple[Action, ...]]:
    """
    Build a lookup: 'catalog.schema.table' (unescaped) -> tuple of actions
    targeting that table.
    """
    grouped: Dict[str, List[Action]] = {}
    for action in plan.actions:
        key = fully_qualified_name_to_string(action.table)
        grouped.setdefault(key, []).append(action)
    return {k: tuple(v) for k, v in grouped.items()}
