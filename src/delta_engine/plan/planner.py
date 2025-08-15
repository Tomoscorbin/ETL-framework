"""
Planner

Orchestrates:
  snapshot (if needed) → diff → order

- Uses a CatalogStateReader to take a snapshot when live_state is not provided.
- Delegates diffing to `Differ` with switches derived from requested aspects.
- Orders actions via `PlanBuilder`.
- Returns the plan plus any snapshot warnings (validation/policy handled upstream).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Tuple

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.plan.differ import Differ, DiffOptions
from src.delta_engine.plan.plan_builder import Plan, PlanBuilder
from src.delta_engine.state.ports import Aspect, CatalogStateReader, SnapshotPolicy, SnapshotRequest, SnapshotWarning
from src.delta_engine.state.states import CatalogState


@dataclass(frozen=True)
class PlanOutcome:
    plan: Plan
    warnings: tuple[SnapshotWarning, ...]


class Planner:
    """
    Orchestrates: snapshot (if needed) -> diff -> order.
    Returns the plan and any snapshot warnings (caller decides what to do with them).
    """

    def __init__(
        self,
        catalog_reader: CatalogStateReader,
        *,
        differ: Differ | None = None,
        plan_builder: PlanBuilder | None = None,
    ) -> None:
        self.catalog_reader = catalog_reader
        self.differ = differ or Differ()
        self.plan_builder = plan_builder or PlanBuilder()

    def plan(
        self,
        desired: DesiredCatalog,
        *,
        aspects: Iterable[Aspect],
        policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE,
        live_state: CatalogState | None = None,
    ) -> PlanOutcome:
        aspects_frozen = frozenset(aspects)
        options = _options_from_aspects(aspects_frozen)

        if live_state is None:
            tables = tuple(d.fully_qualified_table_name for d in desired.tables)
            request = SnapshotRequest(
                tables=tables,
                aspects=aspects_frozen,
                policy=policy,
            )
            snapshot = self.catalog_reader.snapshot(request)
            live = snapshot.state
            warnings = snapshot.warnings
        else:
            live = live_state
            warnings = tuple()

        actions = self.differ.diff(desired, live, options)
        plan = self.plan_builder.build(actions)
        return PlanOutcome(plan=plan, warnings=warnings)


def _options_from_aspects(aspects: frozenset[Aspect]) -> DiffOptions:
    """Map requested snapshot slices to which aspects the differ should manage."""
    return DiffOptions(
        manage_schema=(Aspect.SCHEMA in aspects),
        manage_table_comment=(Aspect.COMMENTS in aspects),
        manage_properties=(Aspect.PROPERTIES in aspects),
        manage_primary_key=(Aspect.PRIMARY_KEY in aspects),
    )
