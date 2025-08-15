"""
Plan Builder

Purpose
-------
Convert a flat list of planned actions into an execution-ready, deterministic
sequence ordered by:
  1) catalog → 2) schema → 3) table
and, within each table:
  - CREATE TABLE
  - ALIGN TABLE (coalesced sub-actions)
  - any unexpected action types (appended last for visibility)

Notes:
-----
- This module does not coalesce or transform actions; the differ already
  collapses granular changes into a single `AlignTable` per table.
- Ordering is stable and predictable for reproducible runs.
- Actions are keyed by `full_table_name` (FullyQualifiedTableName).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.delta_engine.plan.actions import (
    Action,
    AlignTable,
    CreateTable,
)


@dataclass(frozen=True)
class Plan:
    """An ordered, execution-ready sequence of actions."""

    actions: tuple[Action, ...]


class PlanBuilder:
    """
    Order actions by catalog → schema → table, then per table:
      1) CREATE TABLE
      2) ALIGN TABLE

    Any unexpected action types are appended after `AlignTable`.
    Input order within each bucket is preserved.
    """

    def build(self, actions: Iterable[Action]) -> Plan:
        # 1) Group by catalog → schema → table
        grouped: dict[str, dict[str, dict[str, list[Action]]]] = {}
        for action in actions:
            full_table_name = action.full_table_name
            catalog = full_table_name.catalog
            schema = full_table_name.schema
            table = full_table_name.table
            grouped.setdefault(catalog, {}).setdefault(schema, {}).setdefault(table, []).append(
                action
            )

        # 2) Walk groups in deterministic order and bucket per table
        ordered: list[Action] = []
        for catalog in sorted(grouped.keys()):
            by_schema = grouped[catalog]
            for schema in sorted(by_schema.keys()):
                by_table = by_schema[schema]
                for table in sorted(by_table.keys()):
                    per_table = by_table[table]
                    ordered.extend(self._order_for_one_table(per_table))

        return Plan(actions=tuple(ordered))

    def _order_for_one_table(self, actions: list[Action]) -> list[Action]:
        creates: list[Action] = []
        aligns: list[Action] = []
        leftovers: list[Action] = []

        for action in actions:
            if isinstance(action, CreateTable):
                creates.append(action)
            elif isinstance(action, AlignTable):
                aligns.append(action)
            else:
                leftovers.append(action)

        return [*creates, *aligns, *leftovers]
