"""
Protocol for reading current Unity Catalog table state.

Defines the `UnityCatalogReader` interface, which reads live catalog metadata
for a set of tables and returns it as a `CatalogState`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState


class UnityCatalogReader(Protocol):
    """Reads current Unity Catalog state for given tables."""

    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        """Return a point-in-time state for the given tables."""
        ...
