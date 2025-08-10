from typing import Sequence, Dict, Set
from src.delta_engine.models import Table
from src.delta_engine.state.states import CatalogState
from src.delta_engine.common_types import ThreePartTableName
from src.delta_engine.constraints.resolver import resolve_from_models
from src.delta_engine.actions import (
    CreatePrimaryKey,
    CreateForeignKey,
    DropPrimaryKey,
    DropForeignKey,
)
from src.delta_engine.constraints.ordering import order_constraint_plan
from src.delta_engine.actions import ConstraintPlan


class ConstraintPlanner:
    def build_constraint_plan(self, state: CatalogState, models: Sequence[Table]) -> ConstraintPlan:
        resolved = resolve_from_models(models)

        pk_by_table = _pk_name_by_table(state)
        fk_names_by_source = _fk_names_by_source(state)

        drop_fks: list[DropForeignKey] = []
        drop_pks: list[DropPrimaryKey] = []
        create_pks: list[CreatePrimaryKey] = []
        create_fks: list[CreateForeignKey] = []

        # PKs: if name differs, drop dependents from state → drop old PK → create new PK
        for pk in resolved.primary_keys:
            existing_name = pk_by_table.get(pk.three_part_table_name)
            if existing_name == pk.name:
                continue

            if existing_name is not None:
                # Use state-based dependents (no reader needed)
                ts = _table_state_for(state, pk.three_part_table_name)
                for (src_three, fk_name) in ts.constraints.referencing_foreign_keys:
                    drop_fks.append(DropForeignKey(source_three_part_table_name=src_three, name=fk_name))
                drop_pks.append(DropPrimaryKey(three_part_table_name=pk.three_part_table_name, name=existing_name))

            create_pks.append(CreatePrimaryKey(
                three_part_table_name=pk.three_part_table_name,
                name=pk.name,
                columns=pk.columns,
            ))

        # FKs: create if name missing on source
        for fk in resolved.foreign_keys:
            names_on_src = fk_names_by_source.get(fk.source_three_part_table_name, set())
            if fk.name in names_on_src:
                continue
            create_fks.append(CreateForeignKey(
                source_three_part_table_name=fk.source_three_part_table_name,
                name=fk.name,
                source_columns=fk.source_columns,
                target_three_part_table_name=fk.target_three_part_table_name,
                target_columns=fk.target_columns,
            ))

        plan = ConstraintPlan(
            drop_foreign_keys=tuple(drop_fks),
            drop_primary_keys=tuple(drop_pks),
            create_primary_keys=tuple(create_pks),
            create_foreign_keys=tuple(create_fks),
        )
        return order_constraint_plan(plan)

def _table_state_for(state: CatalogState, t: ThreePartTableName):
    full = f"{t[0]}.{t[1]}.{t[2]}"
    ts = state.tables.get(full)
    if ts is None:
        # Should not happen if you snapshotted the same set you resolved
        raise KeyError(f"Missing TableState for {full}")
    return ts

def _pk_name_by_table(state: CatalogState) -> Dict[ThreePartTableName, str]:
    out: Dict[ThreePartTableName, str] = {}
    for ts in state.tables.values():
        if ts.constraints.primary_key_name:
            out[(ts.catalog_name, ts.schema_name, ts.table_name)] = ts.constraints.primary_key_name
    return out

def _fk_names_by_source(state: CatalogState) -> Dict[ThreePartTableName, Set[str]]:
    out: Dict[ThreePartTableName, Set[str]] = {}
    for ts in state.tables.values():
        if ts.constraints.foreign_key_names:
            out[(ts.catalog_name, ts.schema_name, ts.table_name)] = set(ts.constraints.foreign_key_names)
    return out
