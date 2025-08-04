from databricks.labs.dqx import check_funcs  # type: ignore
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule  # type: ignore
from src.models.table import DQDeltaTable


@DQDeltaTable.register_builder
def _is_unique(table: DQDeltaTable):
    if table.primary_key_column_names:
        yield DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=table.primary_key_column_names,
        )


@DQDeltaTable.register_builder
def _is_in_list(table: DQDeltaTable):
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
def _is_in_range(table: DQDeltaTable):
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
                check_func=check_funcs.is_between,
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
