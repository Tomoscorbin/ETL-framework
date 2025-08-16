"""
Concrete validation rules.

- Centralised RuleCode (StrEnum)
- One example of each kind: model, state, plan, warnings
- A 'default_rule_set()' factory that returns (model_rules, state_rules, plan_rules, warnings_rules)
"""

from __future__ import annotations

from enum import StrEnum

from src.delta_engine.desired.models import DesiredTable
from src.delta_engine.identifiers import format_fully_qualified_table_name_from_parts
from src.delta_engine.plan.actions import Action, AddPrimaryKey, DropPrimaryKey
from src.delta_engine.state.ports import Aspect, SnapshotWarning
from src.delta_engine.state.states import ColumnState, TableState
from src.delta_engine.validation.diagnostics import Diagnostic, DiagnosticLevel

# ---------- Centralised rule codes (full words, no abbreviations) ----------


class RuleCode(StrEnum):
    """
    Enumeration of rule codes for diagnostics.

    Each value uniquely identifies a validation rule or guardrail.
    Codes are used when emitting diagnostics so they can be traced
    back to a specific policy.
    """
    PRIMARY_KEY_COLUMNS_PRESENT = "PRIMARY_KEY_COLUMNS_PRESENT"
    TYPE_NARROWING_FORBIDDEN = "TYPE_NARROWING_FORBIDDEN"
    DROP_PRIMARY_KEY_REQUIRES_EXPLICIT_NONE = "DROP_PRIMARY_KEY_REQUIRES_EXPLICIT_NONE"
    SNAPSHOT_WARNINGS = "SNAPSHOT_WARNINGS"  # we suffix by aspect at emission time


# ---------- helpers (tiny, explicit) ----------


def _table_key_from_desired(desired: DesiredTable) -> str:
    """Return the fully qualified table key for a desired table."""
    return format_fully_qualified_table_name_from_parts(
        desired.full_table_name.catalog,
        desired.full_table_name.schema,
        desired.full_table_name.table,
    )


def _table_key_from_warning_table(obj: object | None) -> str:
    """Safely extract a fully qualified table key from a snapshot warning object."""
    if obj is None:
        return ""
    try:
        # Expecting a FullyQualifiedTableName
        return format_fully_qualified_table_name_from_parts(obj.catalog, obj.schema, obj.table)
    except Exception:
        return ""


# ---------- MODEL RULES (desired only) ----------


class PrimaryKeyColumnsMustBePresent:
    """Every desired primary key column must exist in the desired column list."""

    code = RuleCode.PRIMARY_KEY_COLUMNS_PRESENT.value
    description = "Primary key columns must exist in the desired column list."

    def check(self, desired: DesiredTable) -> list[Diagnostic]:
        """Validate that all primary key columns are present in the desired schema."""
        pk_columns = desired.primary_key_columns
        if not pk_columns:
            return []

        desired_lower = {c.name.lower() for c in desired.columns}
        missing = [name for name in pk_columns if name.lower() not in desired_lower]
        if not missing:
            return []

        table_key = _table_key_from_desired(desired)
        return [
            Diagnostic(
                table_key=table_key,
                level=DiagnosticLevel.ERROR,
                code=self.code,
                message=f"Primary key references unknown columns: {missing}",
                hint="Fix the column names or remove them from primary_key_columns.",
            )
        ]


# ---------- STATE RULES (desired + live) ----------

# Simple numeric widening order; lower rank = narrower.
_NUMERIC_WIDENING_RANK: dict[str, int] = {
    "byte": 1,
    "short": 2,
    "int": 3,
    "long": 4,
    "float": 5,
    "double": 6,
    "decimal": 7,  # precision/scale handled separately
    "string": 8,
}


def _numeric_widening_rank(simple: str) -> int:
    """Normalize e.g. 'decimal(10,2)' -> 'decimal' and return a widening rank (lower = narrower)."""
    base = simple.split("(")[0].lower()
    return _NUMERIC_WIDENING_RANK.get(base, 100)  # unknown types rank widest


def _parse_decimal(simple: str) -> tuple[int, int] | None:
    """
    Parse 'decimal(p,s)' -> (p, s); returns None if not a decimal type.
    Defensive parsing; invalid shapes return None.
    """
    text = simple.strip().lower()
    if not text.startswith("decimal"):
        return None
    try:
        inside = text[text.index("(") + 1 : text.index(")")]
        precision_str, scale_str = inside.split(",", 1)
        return int(precision_str.strip()), int(scale_str.strip())
    except Exception:
        return None


class TypeNarrowingForbidden:
    """Changing an existing column to a narrower type is forbidden (numeric/decimal aware)."""

    code = RuleCode.TYPE_NARROWING_FORBIDDEN.value
    description = "Changing a column to a narrower type is forbidden."

    def check(self, desired: DesiredTable, live: TableState | None) -> list[Diagnostic]:
        """Validate that no column types are narrowed between the live and desired schema."""
        if live is None or not live.exists:
            return []

        live_by_lower: dict[str, ColumnState] = {c.name.lower(): c for c in live.columns}
        diagnostics: list[Diagnostic] = []

        for desired_col in desired.columns:
            live_col = live_by_lower.get(desired_col.name.lower())
            if live_col is None:
                continue

            desired_str = desired_col.data_type.simpleString()
            live_str = live_col.data_type.simpleString()

            # Decimal-specific narrowing: lower precision OR lower scale is narrowing.
            desired_dec = _parse_decimal(desired_str)
            live_dec = _parse_decimal(live_str)
            if desired_dec and live_dec:
                desired_precision, desired_scale = desired_dec
                live_precision, live_scale = live_dec
                is_narrowing = (desired_precision < live_precision) or (desired_scale < live_scale)
            else:
                # Rank-based general numeric narrowing
                is_narrowing = _numeric_widening_rank(desired_str) < _numeric_widening_rank(
                    live_str
                )

            if is_narrowing:
                table_key = _table_key_from_desired(desired)
                diagnostics.append(
                    Diagnostic(
                        table_key=table_key,
                        level=DiagnosticLevel.ERROR,
                        code=self.code,
                        message=(
                            f"Column '{desired_col.name}' narrowing from "
                            f"{live_str} to {desired_str}"
                        ),
                        hint="Use a widening type or backfill/migrate first.",
                    )
                )

        return diagnostics


# ---------- PLAN RULES (desired + live + planned actions) ----------


class DropPrimaryKeyRequiresExplicitNone:
    """
    Guardrail: a pure drop of the primary key (drop with no matching create)
    must be explicitly requested via desired.primary_key_columns == ().

    Changing primary key (drop+create) is allowed.
    """

    code = RuleCode.DROP_PRIMARY_KEY_REQUIRES_EXPLICIT_NONE.value
    description = "Dropping a primary key must be explicitly requested."

    def check(
        self,
        desired: DesiredTable,
        planned_actions: tuple[Action, ...],
    ) -> list[Diagnostic]:
        """Validate that primary key drops are explicitly confirmed."""
        will_drop = any(isinstance(a, DropPrimaryKey) for a in planned_actions)
        if not will_drop:
            return []

        # Drop+create = primary key change → allowed
        will_create = any(isinstance(a, AddPrimaryKey) for a in planned_actions)
        if will_create:
            return []

        # Pure drop: require explicit empty tuple in desired
        pk_cols = desired.primary_key_columns
        explicitly_none = (pk_cols is not None) and (len(pk_cols) == 0)
        if explicitly_none:
            return []

        table_key = _table_key_from_desired(desired)
        return [
            Diagnostic(
                table_key=table_key,
                level=DiagnosticLevel.ERROR,
                code=self.code,
                message=(
                    "Plan drops a primary key but the desired spec did"
                    " not explicitly request 'no primary key'."
                ),
                hint=(
                    "Set desired.primary_key_columns=() to affirm the drop,"
                    " or update the desired primary key.",
                )
            )
        ]


# ---------- WARNINGS RULES (global snapshot warnings) ----------


class WarningsToDiagnostics:
    """Convert snapshot warnings to diagnostics (policy: SCHEMA → error, others → warning)."""

    code = RuleCode.SNAPSHOT_WARNINGS.value
    description = "Convert snapshot warnings into diagnostics."

    def check(self, warnings: tuple[SnapshotWarning, ...]) -> list[Diagnostic]:
        """Convert snapshot warnings into diagnostics according to policy."""
        out: list[Diagnostic] = []
        for warning in warnings:
            level = (
                DiagnosticLevel.ERROR
                if warning.aspect == Aspect.SCHEMA
                else DiagnosticLevel.WARNING
            )
            table_key = _table_key_from_warning_table(getattr(warning, "full_table_name", None))
            out.append(
                Diagnostic(
                    table_key=table_key,
                    level=level,
                    code=f"{self.code}_{warning.aspect.name}",
                    message=warning.message,
                    hint=(
                        "Investigate snapshot permissions/connectivity"
                        " or adjust requested aspects.",
                    )
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
    Return a default bundle of rules as 4 tuples to pass to Validator:

        model_rules, state_rules, plan_rules, warnings_rules = default_rule_set()
        validator = Validator(
            model_rules=model_rules,
            state_rules=state_rules,
            plan_rules=plan_rules,
            warnings_rules=warnings_rules,
        )
    """
    model_rules = (PrimaryKeyColumnsMustBePresent(),)
    state_rules = (TypeNarrowingForbidden(),)
    plan_rules = (DropPrimaryKeyRequiresExplicitNone(),)
    warnings_rules = (WarningsToDiagnostics(),)
    return (model_rules, state_rules, plan_rules, warnings_rules)
