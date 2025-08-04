from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import ClassVar, TypeAlias

from databricks.labs.dqx import check_funcs  # type: ignore
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, DQRule  # type: ignore
from src.logger import LOGGER
from src.models.table import DeltaTable

RuleBuilder: TypeAlias = Callable[[DeltaTable], Iterable[DQRule]]


@dataclass(frozen=True)
class DQDeltaTable(DeltaTable):
    """
    DeltaTable that auto-derives DQx rules from column metadata.
    Primary key columns are automatically checked for duplicates.
    Callers can still pass `rules=` explicitly.
    """

    # registry of plug-in builder functions
    _builders: ClassVar[list[RuleBuilder]] = []

    @classmethod
    def register_builder(cls, fn: RuleBuilder) -> RuleBuilder:
        """Decorator: `@DQDeltaTable.register_builder`."""
        cls._builders.append(fn)
        return fn

    def __post_init__(self) -> None:
        # we’re frozen, so we use object.__setattr__
        auto_rules = []
        for build in self._builders:
            try:
                auto_rules.extend(build(self))
            except Exception as exc:
                LOGGER.error(f"Rule builder {build.__name__} failed: {exc}")

        # de-dupe: caller-supplied rules win
        combined = self.rules + [r for r in auto_rules if r not in self.rules]
        object.__setattr__(self, "rules", combined)


@DQDeltaTable.register_builder
def _is_unique(table: DeltaTable):
    if table.primary_key_column_names:
        yield DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=table.primary_key_column_names,
        )


@DQDeltaTable.register_builder
def _is_in_list(table: DeltaTable):
    for col in table.columns:
        quality_rule = col.quality_rule
        if quality_rule and quality_rule.allowed_values:
            yield DQRowRule(
                criticality="error",
                check_func=check_funcs.is_in_list,
                column=col.name,
                check_func_args=[col.allowed_values],
            )


@DQDeltaTable.register_builder
def _is_in_range(table: DeltaTable):
    """
    • min only  → `is_not_less_than`
    • max only  → `is_not_greater_than`
    • both      → `is_between`  (one rule instead of two)
    """
    for col in table.columns:
        if not col.quality_rule:
            continue

        low, high = col.quality_rule.min_value, col.quality_rule.max_value

        if low is not None and high is not None:
            yield DQRowRule(
                criticality="error",
                check_func=check_funcs.is_in_range,
                column=col.name,
                check_func_args=[low, high],
            )

        if low is not None:
            yield DQRowRule(
                criticality="error",
                check_func=check_funcs.is_not_less_than,
                column=col.name,
                check_func_args=[low],
            )

        if high is not None:
            yield DQRowRule(
                criticality="error",
                check_func=check_funcs.is_not_greater_than,
                column=col.name,
                check_func_args=[high],
            )
