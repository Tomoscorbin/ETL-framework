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
    Order actions per catalog → schema → table, then bucket by action type
    for each table in the simple sequence below:

        CREATE TABLE
        ADD COLUMNS
        ALTER NULLABILITY
        DROP PRIMARY KEY
        CREATE PRIMARY KEY
        SET TBLPROPERTIES
        SET TABLE COMMENT
        SET COLUMN COMMENTS

    No coalescing; preserves input order within each bucket.
    """

    def build(self, actions: Iterable[Action]) -> Plan:
        # 1) Group by catalog → schema → table
        by_catalog: dict[str, dict[str, dict[str, list[Action]]]] = {}
        for action in actions:
            table = action.table  # FullyQualifiedTableName
            by_catalog.setdefault(table.catalog, {}).setdefault(table.schema, {}).setdefault(
                table.table, []
            ).append(action)

        # 2) Walk groups in deterministic order and bucket per table
        ordered: list[Action] = []
        for catalog in sorted(by_catalog.keys()):
            by_schema = by_catalog[catalog]
            for schema in sorted(by_schema.keys()):
                by_table = by_schema[schema]
                for table in sorted(by_table.keys()):
                    per_table = by_table[table]
                    ordered.extend(self._order_for_one_table(per_table))

        return Plan(actions=tuple(ordered))

    def _order_for_one_table(self, actions: list[Action]) -> list[Action]:
        creates = [a for a in actions if isinstance(a, CreateTable)]
        aligns = [a for a in actions if isinstance(a, AlignTable)]
        # anything else means a refactor leak; keep for development/testing purposes
        leftovers = [a for a in actions if a not in creates and a not in aligns]
        return [*creates, *aligns, *leftovers]
