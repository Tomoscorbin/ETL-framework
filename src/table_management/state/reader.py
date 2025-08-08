from __future__ import annotations
from typing import Protocol, Sequence
from src.table_management.models import Table
from src.table_management.state.snapshot import CatalogState


class CatalogReader(Protocol):
    """Reads current Unity Catalog state for given tables."""

    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        """Return a point-in-time state for the given tables."""
        ...
