from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from src.delta_engine.plan.actions import (
    Action,
    CreateTable,
    AddColumns,
    AlterColumnNullability,
    DropPrimaryKey,
    CreatePrimaryKey,
    SetTableProperties,
    SetTableComment,
    SetColumnComments,
)


@dataclass(frozen=True)
class Plan:
    """An ordered, execution-ready sequence of actions."""
    actions: Tuple[Action, ...]


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
        by_catalog: Dict[str, Dict[str, Dict[str, List[Action]]]] = {}
        for action in actions:
            table = action.table  # FullyQualifiedTableName
            by_catalog.setdefault(table.catalog, {}) \
                      .setdefault(table.schema, {}) \
                      .setdefault(table.table, []) \
                      .append(action)

        # 2) Walk groups in deterministic order and bucket per table
        ordered: List[Action] = []
        for catalog in sorted(by_catalog.keys()):
            by_schema = by_catalog[catalog]
            for schema in sorted(by_schema.keys()):
                by_table = by_schema[schema]
                for table in sorted(by_table.keys()):
                    per_table = by_table[table]
                    ordered.extend(self._order_for_one_table(per_table))

        return Plan(actions=tuple(ordered))


    def _order_for_one_table(self, actions: List[Action]) -> List[Action]:
        """
        Bucketing for a single table.
        """
        create = [a for a in actions if isinstance(a, CreateTable)]
        cols   = [a for a in actions if isinstance(a, AddColumns)]
        nulls  = [a for a in actions if isinstance(a, AlterColumnNullability)]
        drops  = [a for a in actions if isinstance(a, DropPrimaryKey)]
        pks    = [a for a in actions if isinstance(a, CreatePrimaryKey)]
        props  = [a for a in actions if isinstance(a, SetTableProperties)]
        tcomm  = [a for a in actions if isinstance(a, SetTableComment)]
        ccomm  = [a for a in actions if isinstance(a, SetColumnComments)]

        ordered: List[Action] = []
        ordered += create
        ordered += cols
        ordered += nulls
        ordered += drops
        ordered += pks
        ordered += props
        ordered += tcomm
        ordered += ccomm
        return ordered
