from __future__ import annotations

from typing import Iterable, Tuple

from src.delta_engine.common_types import ThreePartTableName
from src.delta_engine.constraints.specs import PrimaryKeySpec, ForeignKeySpec
from src.delta_engine.actions import (
    ConstraintPlan,
    CreatePrimaryKey, 
    CreateForeignKey,
    DropPrimaryKey,
    DropForeignKey,
)

# ----------------- sort key builders (small + explicit) -----------------

def _three_part_sort_key(name: ThreePartTableName) -> Tuple[str, str, str]:
    """Sort by (catalog, schema, table)."""
    return name

def _pk_spec_sort_key(spec: PrimaryKeySpec) -> tuple:
    """PK specs sort by table then constraint name; columns are a last-resort tiebreaker."""
    return (_three_part_sort_key(spec.three_part_table_name), spec.name, tuple(spec.columns))

def _fk_spec_sort_key(spec: ForeignKeySpec) -> tuple:
    """
    FK specs sort by source table, then target table, then name.
    Columns are tiebreakers to keep order stable if names collide.
    """
    return (
        _three_part_sort_key(spec.source_three_part_table_name),
        _three_part_sort_key(spec.target_three_part_table_name),
        spec.name,
        tuple(spec.source_columns),
        tuple(spec.target_columns),
    )

def _drop_pk_action_sort_key(action: DropPrimaryKey) -> tuple:
    return (_three_part_sort_key(action.three_part_table_name), action.name)

def _drop_fk_action_sort_key(action: DropForeignKey) -> tuple:
    return (_three_part_sort_key(action.source_three_part_table_name), action.name)

def _create_pk_action_sort_key(action: CreatePrimaryKey) -> tuple:
    return (_three_part_sort_key(action.three_part_table_name), action.name, tuple(action.columns))

def _create_fk_action_sort_key(action: CreateForeignKey) -> tuple:
    return (
        _three_part_sort_key(action.source_three_part_table_name),
        _three_part_sort_key(action.target_three_part_table_name),
        action.name,
        tuple(action.source_columns),
        tuple(action.target_columns),
    )

# ----------------- public ordering for specs -----------------

def order_primary_key_specs(primary_keys: Iterable[PrimaryKeySpec]) -> tuple[PrimaryKeySpec, ...]:
    return tuple(sorted(primary_keys, key=_pk_spec_sort_key))

def order_foreign_key_specs(foreign_keys: Iterable[ForeignKeySpec]) -> tuple[ForeignKeySpec, ...]:
    return tuple(sorted(foreign_keys, key=_fk_spec_sort_key))

# ----------------- public ordering for actions -----------------

def order_drop_primary_keys(actions: Iterable[DropPrimaryKey]) -> tuple[DropPrimaryKey, ...]:
    return tuple(sorted(actions, key=_drop_pk_action_sort_key))

def order_drop_foreign_keys(actions: Iterable[DropForeignKey]) -> tuple[DropForeignKey, ...]:
    return tuple(sorted(actions, key=_drop_fk_action_sort_key))

def order_create_primary_keys(actions: Iterable[CreatePrimaryKey]) -> tuple[CreatePrimaryKey, ...]:
    return tuple(sorted(actions, key=_create_pk_action_sort_key))

def order_create_foreign_keys(actions: Iterable[CreateForeignKey]) -> tuple[CreateForeignKey, ...]:
    return tuple(sorted(actions, key=_create_fk_action_sort_key))

# ----------------- convenience: order a whole plan -----------------

def order_constraint_plan(plan: ConstraintPlan) -> ConstraintPlan:
    """
    Return a new plan with a deterministic, safe order:
      1) drop foreign keys
      2) drop primary keys
      3) create primary keys
      4) create foreign keys
    """
    return ConstraintPlan(
        drop_foreign_keys=order_drop_foreign_keys(plan.drop_foreign_keys),
        drop_primary_keys=order_drop_primary_keys(plan.drop_primary_keys),
        create_primary_keys=order_create_primary_keys(plan.create_primary_keys),
        create_foreign_keys=order_create_foreign_keys(plan.create_foreign_keys),
    )
