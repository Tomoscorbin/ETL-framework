# compile/foreign_key_planner.py
from __future__ import annotations
from typing import Tuple, Dict, List
from src.delta_engine.actions import ForeignKeyAdd, ForeignKeyDrop
from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState, TableState
from src.delta_engine.compile.validator import PreflightValidator
from src.delta_engine.constraints.naming import build_foreign_key_name, construct_full_table_name

class ForeignKeyPlanner:
    def __init__(self, validator: PreflightValidator) -> None:
        self.validator = validator

    def plan_for_table(
        self,
        desired_table: Table,
        actual_state: TableState | None,
        catalog_state: CatalogState,
        planned_add_columns: List[str],          # names that will be added by align/create
        planned_fk_drops: List[str] = [],        # names we already decided to drop (rare)
    ) -> Tuple[List[ForeignKeyAdd], List[ForeignKeyDrop]]:
        actual_by = {fk.constraint_name: fk for fk in (actual_state.foreign_keys if actual_state else [])}
        desired_specs = self._normalize_desired_fks(desired_table)

        # validate first (cross-table existence & type checks, etc.)
        self.validator.validate_foreign_keys(
            catalog_state=catalog_state,
            catalog=desired_table.catalog_name,
            schema=desired_table.schema_name,
            src_table=desired_table.table_name,
            desired_fks=desired_table.foreign_keys,
            add_column_names=planned_add_columns,
            planned_fk_drops=planned_fk_drops,
        )

        adds: List[ForeignKeyAdd] = []
        drops: List[ForeignKeyDrop] = []

        if actual_state:
            for name in actual_by:
                if name not in desired_specs:
                    drops.append(ForeignKeyDrop(
                        constraint_name=name,
                        catalog_name=actual_state.catalog_name,
                        schema_name=actual_state.schema_name,
                        table_name=actual_state.table_name,
                    ))

        for name, spec in desired_specs.items():
            a = actual_by.get(name)
            changed = a and (
                a.source_columns != spec["source_columns"]
                or a.reference_table_name != spec["reference_table_name"]
                or a.reference_columns != spec["reference_columns"]
            )
            if a is None or changed:
                if changed:
                    drops.append(ForeignKeyDrop(
                        constraint_name=name,
                        catalog_name=actual_state.catalog_name,
                        schema_name=actual_state.schema_name,
                        table_name=actual_state.table_name,
                    ))
                adds.append(ForeignKeyAdd(
                    constraint_name=name,
                    catalog_name=desired_table.catalog_name,
                    schema_name=desired_table.schema_name,
                    source_table_name=desired_table.table_name,
                    source_columns=spec["source_columns"],
                    reference_table_name=spec["reference_table_name"],
                    reference_columns=spec["reference_columns"],
                ))

        return adds, drops

    def _normalize_desired_fks(self, desired_table: Table) -> Dict[str, Dict]:
        out: Dict[str, Dict] = {}
        for fk in desired_table.foreign_keys:
            if len(fk.source_columns) != len(fk.reference_columns):
                raise RuntimeError(f"FK must map 1:1 columns on {desired_table.full_name}: {fk}")
            name = build_foreign_key_name(
                catalog=desired_table.catalog_name,
                schema=desired_table.schema_name,
                source_table=desired_table.table_name,
                source_columns=fk.source_columns,
                reference_table=fk.reference_table_name,
            )
            out[name] = {
                "source_columns": list(fk.source_columns),
                "reference_table_name": fk.reference_table_name,
                "reference_columns": list(fk.reference_columns),
            }
        return out
