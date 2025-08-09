# compile/planner.py
from __future__ import annotations
from typing import Sequence, List, Dict, Tuple
from src.logger import LOGGER
from src.delta_engine.actions import (
    Plan, CreateTable, AlignTable, ForeignKeyAdd, ForeignKeyDrop
)
from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState, TableState
from src.delta_engine.compile.validator import PreflightValidator
from .diffs import TableDiff
from .diff_calculator import DiffCalculator
from .table_planner import TablePlanner
from .foreign_key_planner import ForeignKeyPlanner


class Planner:
    """
    Pipeline:
      1) compute_diffs
      2) build_create_actions, build_align_actions
      3) validate_creates, validate_aligns
      4) build_fk_actions (uses catalog + planned column adds)
      5) validate_fks
      6) return Plan
    """

    def __init__(self, validator: PreflightValidator | None = None) -> None:
        self.validator = validator or PreflightValidator()
        self.diff_calculator = DiffCalculator()
        self.assembler = TablePlanner()
        self.fk_planner = ForeignKeyPlanner(self.validator)

    # ---------- public entry ----------

    def plan(self, desired_tables: Sequence[Table], actual_catalog_state: CatalogState) -> Plan:
        diffs_by_table = self.compute_diffs(desired_tables, actual_catalog_state)

        create_actions, add_column_names_map = self.build_create_actions(diffs_by_table)
        align_actions, add_column_names_map2 = self.build_align_actions(diffs_by_table, actual_catalog_state)

        # merge the “columns that will exist” maps (align overrides create if any overlap)
        planned_add_column_names: Dict[str, List[str]] = {**add_column_names_map, **add_column_names_map2}

        self.validate_creates(create_actions)
        self.validate_aligns(align_actions, desired_tables, actual_catalog_state)

        fk_adds, fk_drops = self.build_fk_actions(
            desired_tables=desired_tables,
            actual_catalog_state=actual_catalog_state,
            planned_add_column_names=planned_add_column_names,
        )
        self.validate_fks(fk_adds, fk_drops)

        return Plan(
            create_tables=create_actions,
            align_tables=align_actions,
            drop_foreign_keys=fk_drops,
            add_foreign_keys=fk_adds,
        )

    # ---------- stage 1: diffs ----------

    def compute_diffs(
        self,
        desired_tables: Sequence[Table],
        actual_catalog_state: CatalogState,
    ) -> Dict[str, Tuple[Table, TableState | None, TableDiff]]:
        out: Dict[str, Tuple[Table, TableState | None, TableDiff]] = {}
        for desired in desired_tables:
            actual = actual_catalog_state.get(desired.catalog_name, desired.schema_name, desired.table_name)
            diff = self.diff_calculator.diff(desired, actual)
            key = self._full_name(desired)
            out[key] = (desired, actual, diff)
        return out

    # ---------- stage 2: action producers (by type) ----------

    def build_create_actions(
        self,
        diffs_by_table: Dict[str, Tuple[Table, TableState | None, TableDiff]],
    ) -> Tuple[List[CreateTable], Dict[str, List[str]]]:
        actions: List[CreateTable] = []
        planned_add_column_names: Dict[str, List[str]] = {}

        for full, (desired, _actual, diff) in diffs_by_table.items():
            if not diff.is_create:
                continue
            action = self.assembler.assemble(desired, diff)  # CreateTable
            actions.append(action)
            # For CREATEs, every desired column will exist post-plan
            planned_add_column_names[full] = [c.name for c in desired.columns]
            LOGGER.info("Plan: CREATE %s", full)

        return actions, planned_add_column_names

    def build_align_actions(
        self,
        diffs_by_table: Dict[str, Tuple[Table, TableState | None, TableDiff]],
        actual_catalog_state: CatalogState,
    ) -> Tuple[List[AlignTable], Dict[str, List[str]]]:
        actions: List[AlignTable] = []
        planned_add_column_names: Dict[str, List[str]] = {}

        for full, (desired, actual, diff) in diffs_by_table.items():
            if diff.is_create:
                continue

            # Per-table validation inputs
            desired_pk = [c.name for c in desired.columns if c.is_primary_key]
            actual_pk  = list(actual.primary_key_columns) if (actual and actual.exists) else []
            add_names  = [d.name for d in diff.columns_to_add]
            drop_names = [d.name for d in diff.columns_to_drop]
            will_drop_pk = diff.primary_key.drop

            # Lightweight per-table preflight (schema-ish)
            self.validator.validate_align(
                full_name=full,
                desired_pk_cols=desired_pk,
                actual_pk_cols=actual_pk,
                add_column_names=add_names,
                drop_column_names=drop_names,
                will_drop_pk=will_drop_pk,
            )

            action = self.assembler.assemble(desired, diff)  # AlignTable
            actions.append(action)
            planned_add_column_names[full] = add_names
            LOGGER.info("Plan: ALIGN %s", full)

        return actions, planned_add_column_names

    def build_fk_actions(
        self,
        desired_tables: Sequence[Table],
        actual_catalog_state: CatalogState,
        planned_add_column_names: Dict[str, List[str]],
    ) -> Tuple[List[ForeignKeyAdd], List[ForeignKeyDrop]]:
        fk_adds: List[ForeignKeyAdd] = []
        fk_drops: List[ForeignKeyDrop] = []

        for desired in desired_tables:
            full = self._full_name(desired)
            actual = actual_catalog_state.get(desired.catalog_name, desired.schema_name, desired.table_name)
            add_names = planned_add_column_names.get(full, [])
            adds, drops = self.fk_planner.plan_for_table(
                desired_table=desired,
                actual_state=actual,
                catalog_state=actual_catalog_state,
                planned_add_columns=add_names,
            )
            fk_adds.extend(adds)
            fk_drops.extend(drops)
        return fk_adds, fk_drops

    # ---------- stage 3: validators (batch-level) ----------

    def validate_creates(self, creates: List[CreateTable]) -> None:
        # Keep it simple for now; you can add batch rules later (e.g., duplicate names).
        # Example: ensure no duplicates in the batch
        seen = set()
        for c in creates:
            full = f"{c.catalog_name}.{c.schema_name}.{c.table_name}"
            if full in seen:
                raise RuntimeError(f"Duplicate CREATE for {full}")
            seen.add(full)

    def validate_aligns(
        self,
        aligns: List[AlignTable],
        desired_tables: Sequence[Table],
        actual_catalog_state: CatalogState,
    ) -> None:
        # Placeholder for batch policies (e.g., disallow dropping columns across multiple tables in one run)
        # Intentionally light; per-table checks already executed in build_align_actions.
        return

    def validate_fks(
        self,
        fk_adds: List[ForeignKeyAdd],
        fk_drops: List[ForeignKeyDrop],
    ) -> None:
        # Batch-level sanity checks (e.g., same constraint dropped twice)
        drop_keys = {(d.catalog_name, d.schema_name, d.table_name, d.constraint_name) for d in fk_drops}
        for a in fk_adds:
            add_key = (a.catalog_name, a.schema_name, a.source_table_name, a.constraint_name)
            # If we are dropping and re-adding same name on same table, that's fine (rename/change scenario).
            # But if there's a conflicting add for the same name across tables, you can flag here.
            _ = add_key  # no-op for now

    # ---------- helpers ----------

    def _full_name(self, t: Table) -> str:
        return f"{t.catalog_name}.{t.schema_name}.{t.table_name}"
