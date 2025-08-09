# compile/planner.py
from __future__ import annotations
from typing import Sequence, List
from src.logger import LOGGER
from src.delta_engine.actions import Plan, CreateTable, AlignTable, ForeignKeyAdd, ForeignKeyDrop
from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState
from src.delta_engine.compile.validator import PreflightValidator
from .diffs import TableDiff
from .diff_calculator import DiffCalculator
from .table_planner import TablePlanner
from .foreign_key_planner import ForeignKeyPlanner


class Planner:
    """
    Two-pass planning:
      1) Per-table: diff -> validate -> assemble (Create|Align)
      2) Cross-table: foreign keys (uses catalog + planned per-table column adds)
    """
    def __init__(self, validator: PreflightValidator | None = None) -> None:
        self.validator = validator or PreflightValidator()
        self.diff_calc = DiffCalculator()
        self.assembler = TablePlanner()
        self.fk_planner = ForeignKeyPlanner(self.validator)

    def plan(self, desired_tables: Sequence[Table], actual_catalog_state: CatalogState) -> Plan:
        create_actions: List[CreateTable] = []
        align_actions:  List[AlignTable]  = []
        fk_add_actions: List[ForeignKeyAdd] = []
        fk_drop_actions: List[ForeignKeyDrop] = []

        # Keep track of which column names will exist after we apply table-level actions.
        planned_add_column_names: dict[str, List[str]] = {}

        # ----- Pass 1: per-table diffs -> actions -----
        for desired in desired_tables:
            actual = actual_catalog_state.get(desired.catalog_name, desired.schema_name, desired.table_name)
            full_name = f"{desired.catalog_name}.{desired.schema_name}.{desired.table_name}"

            diff: TableDiff = self.diff_calc.diff(desired, actual)

            # Per-table preflight validation (PK, add/drop/nullability checks)
            desired_pk_cols = [c.name for c in desired.columns if c.is_primary_key]
            actual_pk_cols  = list(actual.primary_key_columns) if (actual and actual.exists) else []
            add_names  = [d.name for d in diff.columns_to_add]
            drop_names = [d.name for d in diff.columns_to_drop]
            will_drop_pk = diff.primary_key.drop

            self.validator.validate_align(
                full_name=full_name,
                desired_pk_cols=desired_pk_cols,
                actual_pk_cols=actual_pk_cols,
                add_column_names=add_names,
                drop_column_names=drop_names,
                will_drop_pk=will_drop_pk,
            )

            action = self.assembler.assemble(desired, diff)

            if diff.is_create:
                LOGGER.info("Plan: CREATE %s", full_name)
                create_actions.append(action)  # type: ignore[arg-type]
                # For CREATEs, "planned add columns" = *all* desired column names (not diff.columns_to_add)
                planned_add_column_names[full_name] = [c.name for c in desired.columns]
            else:
                LOGGER.info("Plan: ALIGN %s", full_name)
                align_actions.append(action)   # type: ignore[arg-type]
                planned_add_column_names[full_name] = add_names

        # ----- Pass 2: foreign keys (cross-table) -----
        for desired in desired_tables:
            actual = actual_catalog_state.get(desired.catalog_name, desired.schema_name, desired.table_name)
            full_name = f"{desired.catalog_name}.{desired.schema_name}.{desired.table_name}"
            add_names = planned_add_column_names.get(full_name, [])

            fk_adds, fk_drops = self.fk_planner.plan_for_table(
                desired_table=desired,
                actual_state=actual,
                catalog_state=actual_catalog_state,
                planned_add_columns=add_names,
            )
            fk_add_actions.extend(fk_adds)
            fk_drop_actions.extend(fk_drops)

        return Plan(
            create_tables=create_actions,
            align_tables=align_actions,
            drop_foreign_keys=fk_drop_actions,
            add_foreign_keys=fk_add_actions,
        )
