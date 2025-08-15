from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.delta_engine.desired.models import DesiredCatalog
from src.delta_engine.diff.differ import DiffOptions, diff_catalog
from src.delta_engine.plan.plan_builder import Plan, PlanBuilder
from src.delta_engine.state.ports import Aspect, CatalogStateReader, SnapshotPolicy, SnapshotRequest


@dataclass(frozen=True)
class PlanOutcome:
    plan: Plan
    warnings: tuple  # tuple[SnapshotWarning, ...]


class Planner:
    """
    Orchestrates: snapshot (if needed) -> diff -> order.
    Returns the plan and any snapshot warnings (validator decides what to do with them).
    """

    def __init__(
        self, catalog_reader: CatalogStateReader, *, plan_builder: PlanBuilder | None = None
    ) -> None:
        self.catalog_reader = catalog_reader
        self.plan_builder = plan_builder or PlanBuilder()

    def plan(
        self,
        desired: DesiredCatalog,
        *,
        aspects: Iterable[Aspect],
        policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE,
        live_state=None,  # Optional[CatalogState]; when provided, no snapshot is performed
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
            warnings = ()

        actions = diff_catalog(desired, live, options)
        plan = self.plan_builder.build(actions)
        return PlanOutcome(plan=plan, warnings=warnings)


def _options_from_aspects(aspects: frozenset[Aspect]) -> DiffOptions:
    """Map snapshot slices to what the differ is allowed to manage."""
    return DiffOptions(
        manage_schema=(Aspect.SCHEMA in aspects),
        manage_table_comment=(Aspect.COMMENTS in aspects),
        manage_properties=(Aspect.PROPERTIES in aspects),
        manage_primary_key=(Aspect.PRIMARY_KEY in aspects),
    )
