"""
Concrete validation rules.

- Centralised RuleCode (StrEnum)
- One example of each kind: model, state, plan, warnings
- A 'default_rule_set()' factory that returns (model_rules, state_rules, plan_rules, warnings_rules)
"""

from __future__ import annotations
from enum import StrEnum
from typing import Dict, List, Tuple

from src.delta_engine.desired.models import DesiredTable
from src.delta_engine.state.states import TableState, ColumnState
from src.delta_engine.state.ports import SnapshotWarning, Aspect
from src.delta_engine.plan.actions import Action, DropPrimaryKey, CreatePrimaryKey
from src.delta_engine.validation.diagnostics import Diagnostic, DiagnosticLevel


# ---------- Centralised rule codes ----------

class RuleCode(StrEnum):
    PK_COLUMNS_PRESENT = "PK_COLUMNS_PRESENT"
    TYPE_NARROWING_FORBIDDEN = "TYPE_NARROWING_FORBIDDEN"
    DROP_PK_REQUIRES_EXPLICIT_NONE = "DROP_PK_REQUIRES_EXPLICIT_NONE"
    SNAPSHOT_WARNINGS = "SNAPSHOT_WARNINGS"   # we suffix by aspect at emission time


# ---------- MODEL RULES (desired only) ----------

class PrimaryKeyColumnsMustBePresent:
    """Every desired PK column must exist in the desired column list."""
    code = RuleCode.PK_COLUMNS_PRESENT.value
    description = "Primary key columns must exist in the desired column list."

    def check(self, desired: DesiredTable) -> List[Diagnostic]:
        if not desired.primary_key_columns:
            return []

        desired_lower = {c.name.lower() for c in desired.columns}
        missing = [c for c in desired.primary_key_columns if c.lower() not in desired_lower]
        if not missing:
            return []

        key = (
            f"{desired.fully_qualified_table_name.catalog}."
            f"{desired.fully_qualified_table_name.schema}."
            f"{desired.fully_qualified_table_name.table}"
        )
        return [
            Diagnostic(
                table_key=key,
                level=DiagnosticLevel.ERROR,
                code=self.code,
                message=f"Primary key references unknown columns: {missing}",
                hint="Fix the column names or remove them from primary_key_columns.",
            )
        ]


# ---------- STATE RULES (desired + live) ----------

_NUMERIC_ORDER = {
    "byte": 1, "short": 2, "int": 3, "long": 4,
    "float": 5, "double": 6, "decimal": 7, "string": 8,
}

def _rank(simple: str) -> int:
    """Normalize e.g. 'decimal(10,2)' -> 'decimal' and return a widening rank (lower = narrower)."""
    base = simple.split("(")[0].lower()
    return _NUMERIC_ORDER.get(base, 100)  # unknown types rank wide

class TypeNarrowingForbidden:
    """Changing an existing column to a narrower type is forbidden."""
    code = RuleCode.TYPE_NARROWING_FORBIDDEN.value
    description = "Changing a column to a narrower type is forbidden."

    def check(self, desired: DesiredTable, live: TableState | None) -> List[Diagnostic]:
        if live is None or not live.exists:
            return []

        live_by_lower: Dict[str, ColumnState] = {c.name.lower(): c for c in live.columns}
        out: List[Diagnostic] = []

        for col in desired.columns:
            live_col = live_by_lower.get(col.name.lower())
            if live_col is None:
                continue
            if _rank(col.data_type.simpleString()) < _rank(live_col.data_type.simpleString()):
                key = (
                    f"{desired.fully_qualified_table_name.catalog}."
                    f"{desired.fully_qualified_table_name.schema}."
                    f"{desired.fully_qualified_table_name.table}"
                )
                out.append(
                    Diagnostic(
                        table_key=key,
                        level=DiagnosticLevel.ERROR,
                        code=self.code,
                        message=(
                            f"Column '{col.name}' narrowing from "
                            f"{live_col.data_type.simpleString()} to {col.data_type.simpleString()}"
                        ),
                        hint="Use a widening type or backfill/migrate first.",
                    )
                )
        return out


# ---------- PLAN RULES (desired + live + planned actions) ----------

class DropPrimaryKeyRequiresExplicitNone:
    """
    Guardrail: a *pure drop* of the primary key (drop with no matching create)
    must be explicitly requested via desired.primary_key_columns == ().

    Changing PK (drop+create) is allowed.
    """
    code = RuleCode.DROP_PK_REQUIRES_EXPLICIT_NONE.value
    description = "Dropping a primary key must be explicitly requested."

    def check(
        self,
        desired: DesiredTable,
        live: TableState | None,
        planned_actions: Tuple[Action, ...],
    ) -> List[Diagnostic]:
        will_drop = any(isinstance(a, DropPrimaryKey) for a in planned_actions)
        if not will_drop:
            return []

        # Drop+create = PK change → allowed
        will_create = any(isinstance(a, CreatePrimaryKey) for a in planned_actions)
        if will_create:
            return []

        # Pure drop: require explicit empty tuple in desired
        explicitly_none = (desired.primary_key_columns is not None) and (len(desired.primary_key_columns) == 0)
        if explicitly_none:
            return []

        key = (
            f"{desired.fully_qualified_table_name.catalog}."
            f"{desired.fully_qualified_table_name.schema}."
            f"{desired.fully_qualified_table_name.table}"
        )
        return [
            Diagnostic(
                table_key=key,
                level=DiagnosticLevel.ERROR,
                code=self.code,
                message=(
                    "Plan drops a primary key but the desired spec did not explicitly request 'no primary key'."
                ),
                hint="Set desired.primary_key_columns=() to affirm the drop, or update the desired PK.",
            )
        ]


# ---------- WARNINGS RULES (global snapshot warnings) ----------

class WarningsToDiagnostics:
    """Convert snapshot warnings to diagnostics (policy: SCHEMA → error, others → warning)."""
    code = RuleCode.SNAPSHOT_WARNINGS.value
    description = "Convert snapshot warnings into diagnostics."

    def check(self, warnings: Tuple[SnapshotWarning, ...]) -> List[Diagnostic]:
        out: List[Diagnostic] = []
        for w in warnings:
            level = DiagnosticLevel.ERROR if w.aspect == Aspect.SCHEMA else DiagnosticLevel.WARNING
            table_key = ""
            try:
                if getattr(w, "table", None) and hasattr(w.table, "catalog"):
                    table_key = f"{w.table.catalog}.{w.table.schema}.{w.table.table}"
            except Exception:
                table_key = ""
            out.append(
                Diagnostic(
                    table_key=table_key,
                    level=level,
                    code=f"{self.code}_{w.aspect.name}",
                    message=w.message,
                    hint="Investigate snapshot permissions/connectivity or adjust requested aspects.",
                )
            )
        return out


# ---------- convenience factory ----------

def default_rule_set() -> tuple[
    tuple[object, ...],  # model rules
    tuple[object, ...],  # state rules
    tuple[object, ...],  # plan rules
    tuple[object, ...],  # warnings rules
]:
    """
    Return a default bundle of rules as 4 tuples that you can pass to Validator:

        model_rules, state_rules, plan_rules, warnings_rules = default_rule_set()
        validator = Validator(
            model_rules=model_rules,
            state_rules=state_rules,
            plan_rules=plan_rules,
            warnings_rules=warnings_rules,
        )
    """
    return (
        (PrimaryKeyColumnsMustBePresent(),),
        (TypeNarrowingForbidden(),),
        (DropPrimaryKeyRequiresExplicitNone(),),
        (WarningsToDiagnostics(),),
    )
